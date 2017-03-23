
"""
.. module:: operators.python_pipeline_operator

    :synopsis: A PythonOperator that moves XCOM data used by the pipeline

.. moduleauthor:: Ludovic Claude <ludovic.claude@chuv.ch>
"""


from airflow.operators.python_operator import PythonOperator
from airflow.utils import apply_defaults
from airflow_pipeline.pipelines import TransferPipelineXComs

import logging
import json


class PythonPipelineOperator(PythonOperator, TransferPipelineXComs):

    """
    A PythonOperator that moves XCOM data used by the pipeline.

    :param python_callable: A reference to an object that is callable.
        If the call returns a Python dictionary with the key 'folder', the
        value of that key will be used to build provenance and scan the output folder for files.
    :type python_callable: python callable
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
    :param parent_task: name of the parent task to use to locate XCom parameters
    :type parent_task: str
    :param on_failure_trigger_dag_id: The dag_id to trigger if this stage of the pipeline has failed,
        i.e. when validate_result_callable raises AirflowSkipException.
    :type on_failure_trigger_dag_id: str
    :param software_versions: List of software and their versions used to build provenance information.
    :type software_versions: dictionary
    """

    template_fields = ('templates_dict', 'incoming_parameters', )
    template_ext = tuple()
    ui_color = '#94A147'

    @apply_defaults
    def __init__(
            self,
            python_callable,
            op_args=None,
            op_kwargs=None,
            provide_context=True,
            templates_dict=None,
            templates_exts=None,
            parent_task=None,
            on_failure_trigger_dag_id=None,
            software_versions=None,
            *args, **kwargs):

        PythonOperator.__init__(self,
                                python_callable=python_callable,
                                op_args=op_args,
                                op_kwargs=op_kwargs,
                                provide_context=provide_context,
                                templates_dict=templates_dict,
                                templates_exts=templates_exts,
                                *args, **kwargs)
        TransferPipelineXComs.__init__(self, parent_task, None)
        self.on_failure_trigger_dag_id = on_failure_trigger_dag_id
        self.software_versions = software_versions

    def pre_execute(self, context):
        self.read_pipeline_xcoms(context, expected=['dataset'])

    def execute(self, context):
        self.op_kwargs = self.op_kwargs or {}
        self.pipeline_xcoms = self.pipeline_xcoms or {}
        if self.provide_context:
            context.update(self.op_kwargs)
            context.update(self.pipeline_xcoms)
            self.op_kwargs = context

        try:
            return_value = self.python_callable(*self.op_args, **self.op_kwargs)
        except Exception as e:
            self.trigger_dag(context, self.on_failure_trigger_dag_id, str(e))
            raise
        logging.info("Done. Returned value was: " + str(return_value))

        if isinstance(return_value, dict):
            self.pipeline_xcoms.update(return_value)
            if 'folder' in return_value:
                self.track_provenance(return_value['folder'], json.dumps(
                    self.software_versions) if self.software_versions else '{}')

        self.write_pipeline_xcoms(context)

        return return_value
