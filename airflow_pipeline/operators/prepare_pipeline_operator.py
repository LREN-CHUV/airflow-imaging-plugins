"""
.. module:: operators.prepare_pipeline_operator
    :synopsis: Operator that prepares the pipeline

.. moduleauthor:: Ludovic Claude <ludovic.claude@chuv.ch>
"""

from textwrap import dedent
from airflow.operators import BaseOperator
from airflow.utils import apply_defaults

import logging


class PreparePipelineOperator(BaseOperator):
    """
    Prepare the pipeline by injecting additional information as XCOM messages.

    :param initial_root_folder: root folder for the initial folder containing the scans to
        process organised by folder and where the name of the folder is the session_id
    :type initial_root_folder: string
    """

    template_fields = ('incoming_parameters',)
    template_ext = ()
    ui_color = '#94A1B7'

    @apply_defaults
    def __init__(
            self,
            initial_root_folder,
            *args, **kwargs):
        super(PreparePipelineOperator, self).__init__(*args, **kwargs)
        self.initial_root_folder = initial_root_folder
        self.incoming_parameters = dedent("""
          # Task {{ task.task_id }}
          # Task configuration

          Initial root folder: __$initial_root_folder__

          ## Pipeline __{{ dag.dag_id }}__ parameters

          dataset = {{ dag_run.dr.conf['dataset'] }}
          session_id = {{ dag_run.conf['session_id'] }}
        """.replace("$initial_root_folder",initial_root_folder))

    def execute(self, context):
        dr = context['dag_run']
        session_id = dr.conf['session_id']
        dataset = dr.conf['dataset']
        folder = self.initial_root_folder + '/' + session_id

        logging.info('folder %s, session_id %s' % (folder, session_id))

        self.xcom_push(context, key='folder', value=folder)
        self.xcom_push(context, key='session_id', value=session_id)
        self.xcom_push(context, key='dataset', value=dataset)
