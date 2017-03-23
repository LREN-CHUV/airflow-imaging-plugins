
"""
.. module:: operators.spm_pipeline_operator

    :synopsis: Executes a pipeline on SPM, where a 'pipeline' is a function implemented in SPM.
               Provenance information is tracked by this operator.

.. moduleauthor:: Ludovic Claude <ludovic.claude@chuv.ch>
"""


from airflow.operators.python_operator import PythonOperator
from airflow.utils import apply_defaults
from airflow.exceptions import AirflowSkipException
from airflow_spm.errors import SPMError
from airflow_pipeline.pipelines import TransferPipelineXComs
from data_tracking.files_recording import create_provenance, visit

import logging
import os
import json

from shutil import rmtree
from io import StringIO
from subprocess import CalledProcessError
from subprocess import check_output


def default_validate_result(return_value, task_id):
    if return_value == 0.0:
        raise AirflowSkipException
    if return_value <= 0:
        raise SPMError('%s failed' % task_id)
    return True


def default_output_folder(folder):
    return folder


class SpmPipelineOperator(PythonOperator, TransferPipelineXComs):

    """
    Executes a pipeline on SPM, where a 'pipeline' is a function implemented in SPM.

    :param spm_function: Name of the SPM function to call.
        The SPM function should return 1.0 on success, 0.0 on failure
    :type spm_function: str
    :param spm_arguments_callable: A reference to an object that is callable.
        It should return a list of arguments to call on the SPM function.
    :type spm_arguments_callable: python callable
    :param op_kwargs: a dictionary of keyword arguments that will get unpacked
        in your function
    :type op_kwargs: dict
    :param op_args: a list of positional arguments that will get unpacked when
        calling your callable
    :type op_args: list
    :param provide_context: if set to true, Airflow will pass a set of
        keyword arguments that can be used in your function. This set of
        kwargs correspond exactly to what you can use in your jinja
        templates. For this to work, you need to define `**kwargs` in your
        function header.
    :type provide_context: bool
    :param templates_dict: a dictionary where the values are templates that
        will get templated by the Airflow engine sometime between
        ``__init__`` and ``execute`` takes place and are made available
        in your callable's context after the template has been applied
    :type templates_dict: dict of str
    :param templates_exts: a list of file extensions to resolve while
        processing templated fields, for examples ``['.sql', '.hql']``
    :param matlab_paths: list of paths to add to Matlab search path. Put there the paths to the
        Matlab scripts containing the function to execute and their dependencies
    :type matlab_paths: list
    :param parent_task: name of the parent task to use to locate XCom parameters
    :type parent_task: str
    :param validate_result_callable: A reference to a function that takes as arguments (return_value, task_id)
        and returns true if the result is valid.
    :type validate_result_callable: python callable
    :param output_folder_callable: A reference to an object that is callable.
        It should return the location of the output folder containing the results of the computation.
    :type output_folder_callable: python callable
    :param on_skip_trigger_dag_id: The dag_id to trigger if this stage of the pipeline is skipped,
        i.e. when validate_result_callable raises AirflowSkipException.
    :type on_skip_trigger_dag_id: str
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

    ui_color = '#c2560a'
    template_fields = ('incoming_parameters',)
    template_ext = tuple()

    @apply_defaults
    def __init__(
            self,
            spm_function,
            spm_arguments_callable,
            op_args=None,
            op_kwargs=None,
            provide_context=True,
            templates_dict=None,
            templates_exts=None,
            parent_task=None,
            matlab_paths=None,
            validate_result_callable=default_validate_result,
            output_folder_callable=default_output_folder,
            on_skip_trigger_dag_id=None,
            on_failure_trigger_dag_id=None,
            dataset_config=None,
            *args, **kwargs):
        PythonOperator.__init__(self,
                                python_callable=spm_arguments_callable,
                                op_args=op_args,
                                op_kwargs=op_kwargs,
                                provide_context=provide_context,
                                templates_dict=templates_dict,
                                templates_exts=templates_exts,
                                *args, **kwargs)
        TransferPipelineXComs.__init__(self, parent_task, dataset_config)

        self.spm_function = spm_function
        self.matlab_paths = matlab_paths
        self.validate_result_callable = validate_result_callable
        self.output_folder_callable = output_folder_callable
        self.on_skip_trigger_dag_id = on_skip_trigger_dag_id
        self.on_failure_trigger_dag_id = on_failure_trigger_dag_id
        self.engine = None
        self.out = None
        self.err = None
        self.provenance_previous_step_id = None

    def pre_execute(self, context):
        super(SpmPipelineOperator, self).pre_execute(context)
        self.read_pipeline_xcoms(context, expected=[
                                 'folder', 'session_id', 'participant_id', 'scan_date',
                                 'dataset', 'matlab_version', 'spm_version', 'spm_revision',
                                 'provenance_details'])
        self.op_kwargs.update(self.pipeline_xcoms)
        self.out = StringIO()
        self.err = StringIO()

    def execute(self, context):
        if self.engine:
            params = super(SpmPipelineOperator, self).execute(context)
            output_folder = self.output_folder_callable(
                *self.op_args, **self.op_kwargs)
            result_value = None

            # Ensure that there is no data in the output folder as some SPM scripts
            # can break if they find unexpected data...
            try:
                if os.path.exists(output_folder):
                    os.removedirs(output_folder)
            except Exception:
                logging.error("Cannot cleanup output directory %s before executing SPM function %s",
                              output_folder, self.spm_function)

            logging.info("Calling SPM function %s(%s)", self.spm_function, ','.join(
                map(lambda s: "'%s'" % s if isinstance(s, str) else str(s), params)))

            try:
                # Execute SPM function
                result_value = getattr(self.engine, self.spm_function)(
                    stdout=self.out, stderr=self.err, *params)

                self.engine.exit()
                self.engine = None
            except Exception:
                logging.error("SPM failed and returned %s", result_value)
                logging.error("-----------")
                logging.error("SPM output:")
                logging.error(self.out.getvalue())
                logging.error("SPM errors:")
                logging.error(self.err.getvalue())
                logging.error("-----------")
                # Clean output folder before attempting to retry the
                # computation
                rmtree(output_folder, ignore_errors=True)
                self.trigger_dag(context, self.on_failure_trigger_dag_id, self.out.getvalue(), self.err.getvalue())
                raise

            self.pipeline_xcoms['folder'] = output_folder
            self.pipeline_xcoms['output'] = self.out.getvalue()
            self.pipeline_xcoms['error'] = self.err.getvalue()

            logging.info("SPM returned %s", result_value)
            logging.info("-----------")
            logging.info("SPM output:")
            logging.info(self.out.getvalue())
            logging.info("SPM errors:")
            logging.info(self.err.getvalue())
            logging.info("-----------")

            try:
                valid = self.validate_result_callable(
                    result_value, context['ti'].task_id)
            except AirflowSkipException:
                self.trigger_dag(context, self.on_skip_trigger_dag_id, self.out.getvalue(), self.err.getvalue())
                raise
            except Exception:
                # Clean output folder before attempting to retry the
                # computation
                rmtree(output_folder, ignore_errors=True)
                self.trigger_dag(context, self.on_failure_trigger_dag_id, self.out.getvalue(), self.err.getvalue())
                raise

            if valid:
                provenance_details = json.loads(self.pipeline_xcoms['provenance_details'])
                provenance_details['spm_scripts'] = []
                version = None
                for path in self.matlab_paths:
                    try:
                        version = check_output('cd %s ; git describe --tags' % path, shell=True).strip()
                        provenance_details['matlab_scripts'].append({
                            'path': path,
                            'version': version
                        })
                    except CalledProcessError:
                        logging.warning('Cannot find the Git version on folder %s', path)

                provenance_id = create_provenance(self.pipeline_xcoms['dataset'], software_versions={
                    'matlab_version': self.pipeline_xcoms['matlab_version'],
                    'spm_version': self.pipeline_xcoms['spm_version'],
                    'spm_revision': self.pipeline_xcoms['spm_revision'],
                    'fn_called': self.spm_function,
                    'fn_version': version,
                    'others': json.dumps(provenance_details)})

                provenance_step_id = visit(self.task_id, output_folder, provenance_id,
                                           previous_step_id=self.previous_step_id(),
                                           config=self.dataset_config)
                self.pipeline_xcoms['provenance_previous_step_id'] = provenance_step_id

            self.write_pipeline_xcoms(context)

            return result_value
        else:
            msg = 'Matlab has not started on this node'
            logging.error(msg)
            raise SPMError(msg)

    def on_kill(self):
        if self.engine:
            self.engine.exit()
            self.engine = None

    def post_execute(self, context):
        if self.engine:
            self.engine.exit()
            self.engine = None
