"""
.. module:: operators.python_pipeline_operator
    :synopsis: A PythonOperator that moves XCOM data used by the pipeline

.. moduleauthor:: Ludovic Claude <ludovic.claude@chuv.ch>
"""

from airflow.operators import PythonOperator
from airflow.utils import apply_defaults

import logging


class PythonPipelineOperator(PythonOperator):
    """
    A PythonOperator that moves XCOM data used by the pipeline.

    :param python_callable: A reference to an object that is callable
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
    """
    template_fields = ('templates_dict',)
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
            *args, **kwargs):
        super().__init__(python_callable=python_callable,
                         op_args=op_args,
                         op_kwargs=op_kwargs,
                         provide_context=provide_context,
                         templates_dict=templates_dict,
                         templates_exts=templates_exts,
                         *args, **kwargs)
        self.parent_task = parent_task

    def pre_execute(self, context):
        self.folder = self.xcom_pull(
            context, task_ids=self.parent_task, key='folder')
        self.session_id = self.xcom_pull(
            context, task_ids=self.parent_task, key='session_id')
        if not self.session_id:
            dr = context['dag_run']
            self.session_id = dr.conf['session_id']

        self.participant_id = self.xcom_pull(
            context, task_ids=self.parent_task, key='participant_id')
        self.scan_date = self.xcom_pull(
            context, task_ids=self.parent_task, key='scan_date')

    def execute(self, context):
        if self.provide_context:
            context.update(self.op_kwargs)
            context['templates_dict'] = self.templates_dict
            context['folder'] = self.folder
            context['session_id'] = self.session_id
            context['participant_id'] = self.participant_id
            context['scan_date'] = self.scan_date
            self.op_kwargs = context

        return_value = self.python_callable(*self.op_args, **self.op_kwargs)
        logging.info("Done. Returned value was: " + str(return_value))

        if isinstance(return_value, dict):
            for k in ['folder', 'participant_id', 'scan_date']:
                if k in return_value:
                    self.__setattr__(k, return_value[k])

        return return_value

    def post_execute(self, context):
        self.xcom_push(context, key='folder', value=self.folder)
        self.xcom_push(context, key='session_id', value=self.session_id)
        self.xcom_pull(context, key='participant_id',
                       value=self.participant_id)
        self.xcom_pull(context, key='scan_date', value=self.scan_date)
