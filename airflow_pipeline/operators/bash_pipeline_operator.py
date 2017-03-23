
"""
.. module:: operators.bash_pipeline_operator

    :synopsis: A BashOperator that registers provenance information the pipeline

.. moduleauthor:: Ludovic Claude <ludovic.claude@chuv.ch>
"""


from airflow.operators.bash_operator import BashOperator
from airflow.utils import apply_defaults
from airflow.exceptions import AirflowException
from airflow_pipeline.pipelines import TransferPipelineXComs

import logging
import os

from shutil import rmtree


def default_output_folder(folder, **kwargs):
    return folder


class BashPipelineOperator(BashOperator, TransferPipelineXComs):

    """
    A BashOperator that registers provenance information in the pipeline.

    The path to the input directory can be accessed via the environment variable ``AIRFLOW_INPUT_DIR``.
    The path to the output directory can be accessed via the environment variable ``AIRFLOW_OUTPUT_DIR``,
    where output dir is computed from function output_folder_callable.

    :param bash_command: The command, set of commands or reference to a
        bash script (must be '.sh') to be executed.
    :type bash_command: string
    :param xcom_push: If xcom_push is True, the last line written to stdout
        will also be pushed to an XCom when the bash command completes.
    :type xcom_push: bool
    :param env: If env is not None, it must be a mapping that defines the
        environment variables for the new process; these are used instead
        of inheriting the current process environment, which is the default
        behavior. (templated)
    :type env: dict
    :type output_encoding: output encoding of bash command
    :param output_folder_callable: A reference to an object that is callable.
        It should return the location of the output folder on the host containing the results of the computation.
    :type output_folder_callable: python callable
    :param auto_cleanup_output_folder: When True, the output folder is automatically cleaned before
        execution and on error.
    :type auto_cleanup_output_folder: bool
    :param parent_task: name of the parent task to use to locate XCom parameters
    :type parent_task: str
    :param on_failure_trigger_dag_id: The dag_id to trigger if this stage of the pipeline has failed,
        i.e. when validate_result_callable raises AirflowSkipException.
    :type on_failure_trigger_dag_id: str
    :param dataset_config: Collection of flags and setting related to the dataset:
        - boost_provenance_scan: When True, we consider that all the files from same folder share the same meta-data.
        The processing is 2x faster. Enabled by default.
        - session_id_by_patient: Rarely, a data set might use study IDs which are unique by patient (not for the whole
        study).
        E.g.: LREN data. In such a case, you have to enable this flag. This will use PatientID + StudyID as a session
        ID.
    :type dataset_config: dict
    """

    template_fields = ('incoming_parameters', 'bash_command', 'env')
    template_ext = tuple()
    ui_color = '#e9ffdb'  # nyanza

    @apply_defaults
    def __init__(
            self,
            bash_command,
            xcom_push=True,
            env=None,
            output_encoding='utf-8',
            parent_task=None,
            output_folder_callable=default_output_folder,
            auto_cleanup_output_folder=False,
            on_failure_trigger_dag_id=None,
            dataset_config=None,
            *args, **kwargs):

        BashOperator.__init__(self,
                              bash_command=bash_command,
                              xcom_push=xcom_push,
                              env=env or {},
                              output_encoding=output_encoding,
                              *args, **kwargs)
        TransferPipelineXComs.__init__(self, parent_task, dataset_config)
        self.output_folder_callable = output_folder_callable
        self.auto_cleanup_output_folder = auto_cleanup_output_folder
        self.on_failure_trigger_dag_id = on_failure_trigger_dag_id

    def pre_execute(self, context):
        self.read_pipeline_xcoms(context, expected=['folder', 'dataset'])

    def execute(self, context):

        self.pipeline_xcoms = self.pipeline_xcoms or {}
        output_dir = self.output_folder_callable(
            **self.pipeline_xcoms)

        if self.auto_cleanup_output_folder:
            # Ensure that there is no data in the output folder
            try:
                if os.path.exists(output_dir):
                    os.removedirs(output_dir)
            except Exception:
                logging.error("Cannot cleanup output directory %s before executing Bash command",
                              output_dir)

        self.env['AIRFLOW_INPUT_DIR'] = self.pipeline_xcoms['folder']
        self.env['AIRFLOW_OUTPUT_DIR'] = output_dir

        try:
            logs = super(BashPipelineOperator, self).execute(context)
        except AirflowException:
            logs = ""
            logging.error("Bash command failed")
            logging.error("-----------")
            logging.error("Output:")
            for line in iter(self.sp.stdout.readline, b''):
                logging.error(line)
                logs = logs + line + "\n"
            logging.error("-----------")
            if self.auto_cleanup_output_folder:
                # Clean output folder before attempting to retry the
                # computation
                rmtree(output_dir, ignore_errors=True)
            self.trigger_dag(context, self.on_failure_trigger_dag_id, logs)
            raise

        self.pipeline_xcoms['folder'] = output_dir
        self.pipeline_xcoms['output'] = logs
        self.pipeline_xcoms['error'] = ''

        software_versions = {'others': '{"bash_command"="%s"}' % self.bash_command}
        self.track_provenance(output_dir, software_versions)

        self.write_pipeline_xcoms(context)

        return logs
