
"""
.. module:: operators.spm_operator

    :synopsis: Operator that executes SPM via Matlab Python engine: SpmOperator

.. moduleauthor:: Ludovic Claude <ludovic.claude@chuv.ch>
"""


from airflow import configuration
from airflow.operators.python_operator import PythonOperator
from airflow.utils import apply_defaults
from airflow_spm.errors import SPMError

import logging

try:
    import matlab.engine
except (IOError, RuntimeError, ImportError):
    logging.error('Matlab not available on this node')


class SpmOperator(PythonOperator):

    """
    Executes SPM.

    :param python_callable: A reference to an object that is callable.
        The 'engine' argument to the python function will be set as the Matlab engine to call.
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
    :param matlab_paths: list of paths to add to Matlab search path. Put there the paths to the
        Matlab scripts containing the function to execute and their dependencies
    :type matlab_paths: list
    """

    ui_color = '#f2570a'

    @apply_defaults
    def __init__(
            self,
            python_callable,
            op_args=None,
            op_kwargs=None,
            provide_context=False,
            templates_dict=None,
            templates_exts=None,
            matlab_paths=None,
            *args, **kwargs):
        super(SpmOperator, self).__init__(python_callable=python_callable,
                                          op_args=op_args,
                                          op_kwargs=op_kwargs,
                                          provide_context=provide_context,
                                          templates_dict=templates_dict,
                                          templates_exts=templates_exts,
                                          *args, **kwargs)
        self.matlab_paths = matlab_paths

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

    def execute(self, context):
        if self.engine:
            self.op_kwargs['engine'] = self.engine
            result = super(SpmOperator, self).execute(context)
            self.engine.exit()
            self.engine = None
            return result
        else:
            msg = 'Matlab has not started on this node'
            logging.error(msg)
            raise SPMError(msg)

    def handle_failure(self, error, test_mode=False, context=None):
        logging.error("-----------")
        logging.error("SPM output:")
        logging.error(self.out.getvalue())
        logging.error("SPM errors:")
        logging.error(self.err.getvalue())
        logging.error("-----------")
        super(SpmOperator, self).handle_failure(error, test_mode=False, context=None)

    def on_kill(self):
        if self.engine:
            self.engine.exit()
            self.engine = None

    def post_execute(self, context):
        if self.engine:
            self.engine.exit()
            self.engine = None
