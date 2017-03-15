
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

    :param include_spm_facts: When True, reads SPM facts from Ansible and add this information as XCOM messages.
    :type include_spm_facts: bool
    """

    template_fields = ('incoming_parameters',)
    template_ext = ()
    ui_color = '#94A1B7'
    spm_fact_file = '/etc/ansible/facts.d/spm.fact'

    @apply_defaults
    def __init__(self, include_spm_facts=True, *args, **kwargs):
        super(PreparePipelineOperator, self).__init__(*args, **kwargs)
        self.incoming_parameters = dedent("""
          # Task {{ task.task_id }}
          ## Task configuration

          None

          ## Pipeline {{ dag.dag_id }} parameters

          folder: {{ dag_run.conf['folder'] }}
          dataset = {{ dag_run.conf['dataset'] }}
          {% if 'session_id' in dag_run.conf %}
          session_id = {{ dag_run.conf['session_id'] }}
          {% endif %}
        """)
        self.include_spm_facts = include_spm_facts

    def execute(self, context):
        dr = context['dag_run']
        dataset = dr.conf['dataset']
        folder = dr.conf["folder"]
        if 'session_id' in dr.conf:
            session_id = dr.conf['session_id']
            logging.info('dataset %s, folder %s, session_id %s', dataset, folder, session_id)
        else:
            session_id = None
            logging.info('dataset %s, folder %s', dataset, folder)
        if 'relative_context_path' in dr.conf:
            relative_context_path = dr.conf['relative_context_path']
        else:
            relative_context_path = None

        if self.include_spm_facts:
            if os.path.exists(self.spm_fact_file):
                with open(self.spm_fact_file, 'r') as f:
                    spm_facts = json.load(f)
                    self.xcom_push(context, key='matlab_version', value=spm_facts['general']['matlab_version'])
                    self.xcom_push(context, key='spm_version', value=spm_facts['general']['spm_version'])
                    self.xcom_push(context, key='spm_revision', value=spm_facts['general']['spm_revision'])
                    self.xcom_push(context, key='provenance_details', value=json.dumps(spm_facts))

        self.xcom_push(context, key='folder', value=folder)
        self.xcom_push(context, key='dataset', value=dataset)
        self.xcom_push(context, key='provenance_previous_step_id', value='-1')
        if session_id:
            self.xcom_push(context, key='session_id', value=session_id)
        if relative_context_path:
            self.xcom_push(context, key='relative_context_path', value=relative_context_path)
