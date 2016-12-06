"""
.. module:: operators.spm_pipeline_operator
    :synopsis: Executes a pipeline on SPM, where a 'pipeline' is a function implemented in SPM

.. moduleauthor:: Ludovic Claude <ludovic.claude@chuv.ch>
"""

from airflow import configuration
from airflow.operators import PythonOperator
from airflow.utils import apply_defaults
from airflow_spm.errors import SPMError

import logging

from io import StringIO


def default_validate_result(return_value, task_id):
    if success < 1.0:
        raise RuntimeError('%s failed' % task_id)


def default_output_folder(input_data_folder):
    return input_data_folder


class SpmPipelineOperator(PythonOperator):

    """
    Executes a pipeline on SPM, where a 'pipeline' is a function implemented in SPM.

    :param spm_function: Name of the SPM function to call.
    :type spm_function: string
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
    :type parent_task: string
    :param validate_result_callable: A reference to a function that takes as arguments (return_value, task_id)
        and returns true if the result is valid.
    :type validate_result_callable: python callable
    :param output_folder_callable: A reference to an object that is callable.
        It should return the location of the output folder containing the results of the computation.
    :type output_folder_callable: python callable
    """
    @apply_defaults
    def __init__(
            self,
            spm_function,
            spm_arguments_callable,
            op_args=None,
            op_kwargs=None,
            provide_context=False,
            templates_dict=None,
            templates_exts=None,
            parent_task=None,
            matlab_paths=None,
            validate_result_callable=default_validate_result,
            output_folder_callable=default_output_folder,
            *args, **kwargs):
        super(SpmPipelineOperator, self).__init__(python_callable=spm_arguments_callable,
                                                  op_args=op_args,
                                                  op_kwargs=op_kwargs,
                                                  provide_context=provide_context,
                                                  templates_dict=templates_dict,
                                                  templates_exts=templates_exts,
                                                  *args, **kwargs)
        self.spm_function = spm_function
        self.parent_task = parent_task
        self.matlab_paths = matlab_paths
        self.validate_result_callable = validate_result_callable
        self.output_folder_callable = output_folder_callable

    def pre_execute(self, context):
        spm_dir = str(configuration.get('spm', 'SPM_DIR'))
        if matlab.engine:
            self.engine = matlab.engine.start_matlab()
        if self.engine:
            if self.matlab_paths:
                for path in self.matlab_paths:
                    self.engine.addpath(path)
            self.engine.addpath(spm_dir)
        else:
            msg = 'Matlab has not started on this node'
            logging.error(msg)
            raise SPMError(msg)
        ti = context['ti']
        self.input_data_folder = ti.xcom_pull(
            key='folder', task_ids=self.parent_task)
        self.session_id = ti.xcom_pull(
            key='session_id', task_ids=self.parent_task)
        self.participant_id = ti.xcom_pull(
            key='participant_id', task_ids=self.parent_task)
        self.scan_date = ti.xcom_pull(
            key='scan_date', task_ids=self.parent_task)
        self.out = StringIO()
        self.err = StringIO()
        self.op_kwargs['input_data_folder'] = self.input_data_folder
        self.op_kwargs['session_id'] = self.session_id
        self.op_kwargs['participant_id'] = self.participant_id
        self.op_kwargs['scan_date'] = self.scan_date

    def execute(self, context):
        if self.engine:
            params = super(SpmOperator, self).execute(context)

            logging.info("Calling engine.%s(%s)" %
                         (self.spm_function, ','.join(map(str, params))))

            result_value = getattr(self.engine, self.spm_function)(
                stdout=self.out, stderr=self.err, *params)

            self.engine.exit()
            self.engine = None
            logging.info("SPM returned %s", result_value)

            self.validate_result_callable(result_value, context['ti'].task_id)
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

        logging.info("-----------")
        logging.info("SPM output:")
        logging.info(self.out.getvalue())
        logging.info("SPM errors:")
        logging.info(self.err.getvalue())
        logging.info("-----------")

        ti = context['ti']
        ti.xcom_push(key='folder', value=self.output_folder_callable(
            *self.op_args, **self.op_kwargs))
        ti.xcom_push(key='session_id', value=self.session_id)
        ti.xcom_push(key='participant_id', value=self.participant_id)
        ti.xcom_push(key='scan_date', value=self.scan_date)
