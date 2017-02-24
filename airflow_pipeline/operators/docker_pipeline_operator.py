
"""
.. module:: operators.docker_pipeline_operator
    :synopsis: A DockerOperator that moves XCOM data used by the pipeline

.. moduleauthor:: Ludovic Claude <ludovic.claude@chuv.ch>
"""

try:
    from airflow.operators import DockerOperator
except ImportError:
    from airflow.operators.docker_operator import DockerOperator
from airflow.utils import apply_defaults
from airflow_pipeline.pipelines import TransferPipelineXComs

import logging


class DockerPipelineOperator(DockerOperator, TransferPipelineXComs):
    """
    A DockerOperator that moves XCOM data used by the pipeline.

    A temporary directory is created on the host and mounted into a container to allow storing files
    that together exceed the default disk size of 10GB in a container. The path to the mounted
    directory can be accessed via the environment variable ``AIRFLOW_TMP_DIR``.

    :param image: Docker image from which to create the container.
    :type image: str
    :param api_version: Remote API version.
    :type api_version: str
    :param command: Command to be run in the container.
    :type command: str or list
    :param cpus: Number of CPUs to assign to the container.
        This value gets multiplied with 1024. See
        https://docs.docker.com/engine/reference/run/#cpu-share-constraint
    :type cpus: float
    :param docker_url: URL of the host running the docker daemon.
    :type docker_url: str
    :param environment: Environment variables to set in the container.
    :type environment: dict
    :param force_pull: Pull the docker image on every run.
    :type force_pull: bool
    :param mem_limit: Maximum amount of memory the container can use. Either a float value, which
        represents the limit in bytes, or a string like ``128m`` or ``1g``.
    :type mem_limit: float or str
    :param network_mode: Network mode for the container.
    :type network_mode: str
    :param tls_ca_cert: Path to a PEM-encoded certificate authority to secure the docker connection.
    :type tls_ca_cert: str
    :param tls_client_cert: Path to the PEM-encoded certificate used to authenticate docker client.
    :type tls_client_cert: str
    :param tls_client_key: Path to the PEM-encoded key used to authenticate docker client.
    :type tls_client_key: str
    :param tls_hostname: Hostname to match against the docker server certificate or False to
        disable the check.
    :type tls_hostname: str or bool
    :param tls_ssl_version: Version of SSL to use when communicating with docker daemon.
    :type tls_ssl_version: str
    :param tmp_dir: Mount point inside the container to a temporary directory created on the host by
        the operator. The path is also made available via the environment variable
        ``AIRFLOW_TMP_DIR`` inside the container.
    :type tmp_dir: str
    :param user: Default user inside the docker container.
    :type user: int or str
    :param volumes: List of volumes to mount into the container, e.g.
        ``['/host/path:/container/path', '/host/path2:/container/path2:ro']``.
    :param xcom_push: Does the stdout will be pushed to the next step using XCom.
           The default is True.
    :type xcom_push: bool
    :param xcom_all: Push all the stdout or just the last line. The default is True (all lines).
    :type xcom_all: bool
    :param parent_task: name of the parent task to use to locate XCom parameters
    :type parent_task: str
    """
    template_fields = ('templates_dict','incoming_parameters',)
    template_ext = tuple()
    ui_color = '#e9ffdb'  # nyanza

    @apply_defaults
    def __init__(
            self,
            image,
            api_version=None,
            command=None,
            cpus=1.0,
            docker_url='unix://var/run/docker.sock',
            environment=None,
            force_pull=False,
            mem_limit=None,
            network_mode=None,
            tls_ca_cert=None,
            tls_client_cert=None,
            tls_client_key=None,
            tls_hostname=None,
            tls_ssl_version=None,
            tmp_dir='/tmp/airflow',
            user=None,
            volumes=None,
            xcom_push=True,
            xcom_all=True,
            parent_task=None,
            *args, **kwargs):

        DockerOperator.__init__(self,
                                image=image,
                                api_version=api_version,
                                command=command,
                                cpus=cpus,
                                docker_url=docker_url,
                                environment=environment,
                                force_pull=force_pull,
                                mem_limit=mem_limit,
                                network_mode=network_mode,
                                tls_ca_cert=tls_ca_cert,
                                tls_client_cert=tls_client_cert,
                                tls_client_key=tls_client_key,
                                tls_hostname=tls_hostname,
                                tls_ssl_version=tls_ssl_version,
                                tmp_dir=tmp_dir,
                                user=user,
                                volumes=volumes,
                                xcom_push=xcom_push,
                                xcom_all=xcom_all,
                                *args, **kwargs)
        TransferPipelineXComs.__init__(self, parent_task)
        self.logs = None

    def pre_execute(self, context):
        self.read_pipeline_xcoms(context, expected=[
                                 'folder', 'session_id', 'participant_id', 'scan_date',
                                 'dataset'])
        self.pipeline_xcoms['task_id'] = self.task_id
        self.op_kwargs.update(self.pipeline_xcoms)

    def execute(self, context):

        self.logs = super(SpmPipelineOperator, self).execute(context)

        self.op_kwargs = self.op_kwargs or {}
        self.pipeline_xcoms = self.pipeline_xcoms or {}
        if self.provide_context:
            context.update(self.op_kwargs)
            context.update(self.pipeline_xcoms)
            self.op_kwargs = context

        return_value = self.docker_callable(*self.op_args, **self.op_kwargs)
        logging.info("Done. Returned value was: " + str(return_value))

        if isinstance(return_value, dict):
            self.pipeline_xcoms.update(return_value)

        self.write_pipeline_xcoms(context)

        return return_value

    def trigger_dag(self, context, dag_id):
        if dag_id:
            run_id = 'trig__' + datetime.now().isoformat()
            payload = {
                'output': self.logs,
                'error': ''
            }
            payload.update(self.pipeline_xcoms)

            session = settings.Session()
            dr = DagRun(
                dag_id=dag_id,
                run_id=run_id,
                conf=payload,
                external_trigger=True)
            session.add(dr)
            session.commit()
            session.close()
