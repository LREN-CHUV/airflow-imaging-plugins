"""
.. module:: operators.prepare_pipeline_operator
    :synopsis: Operator that prepares the pipeline

.. moduleauthor:: Ludovic Claude <ludovic.claude@chuv.ch>
"""

from textwrap import dedent
from airflow.operators import BaseOperator
from airflow.utils import apply_defaults

import logging
import os
import json


class PreparePipelineOperator(BaseOperator):
    """
    Prepare the pipeline by injecting additional information as XCOM messages.
    """

    template_fields = ('incoming_parameters',)
    template_ext = ()
    ui_color = '#94A1B7'
    spm_fact_file = '/etc/ansible/facts.d/spm.fact'

    @apply_defaults
    def __init__(
            self,
            initial_root_folder,
            *args, **kwargs):
        super(PreparePipelineOperator, self).__init__(*args, **kwargs)
        self.incoming_parameters = dedent("""
          # Task {{ task.task_id }}
          ## Task configuration

          None

          ## Pipeline {{ dag.dag_id }} parameters

          folder: {{ dag_run.conf['folder'] }}
          dataset = {{ dag_run.conf['dataset'] }}
          session_id = {{ dag_run.conf['session_id'] }}
        """.replace("$initial_root_folder",initial_root_folder))

    def execute(self, context):
        dr = context['dag_run']
        session_id = dr.conf['session_id']
        dataset = dr.conf['dataset']
        folder = dr.conf["folder"]

        logging.info('folder %s, session_id %s', folder, session_id)

        if os.path.exists(self.spm_fact_file):
            with open(self.spm_fact_file, 'r') as f:
                spm_facts = json.load(f)
                self.xcom_push(context, key='matlab_version', value=spm_facts['general']['matlab_version'])
                self.xcom_push(context, key='spm_version', value=spm_facts['general']['spm_version'])
                self.xcom_push(context, key='spm_revision', value=spm_facts['general']['spm_revision'])
                self.xcom_push(context, key='provenance_details', value=json.dumps(spm_facts))

        self.xcom_push(context, key='folder', value=folder)
        self.xcom_push(context, key='session_id', value=session_id)
        self.xcom_push(context, key='dataset', value=dataset)
