"""
.. module:: operators.prepare_pipeline_operator
    :synopsis: Operator that prepares the pipeline

.. moduleauthor:: Ludovic Claude <ludovic.claude@chuv.ch>
"""

from textwrap import dedent
from airflow.operators import BaseOperator
from airflow.utils import apply_defaults
from mri_meta_extract.files_recording import create_provenance

import logging
import os
import json


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
    spm_fact_file: '/etc/ansible/facts.d/spm.fact'

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

          Initial root folder: $initial_root_folder

          ## Pipeline {{ dag.dag_id }} parameters

          dataset = {{ dag_run.conf['dataset'] }}
          session_id = {{ dag_run.conf['session_id'] }}
        """.replace("$initial_root_folder",initial_root_folder))

    def execute(self, context):
        dr = context['dag_run']
        session_id = dr.conf['session_id']
        dataset = dr.conf['dataset']
        folder = self.initial_root_folder + '/' + session_id

        logging.info('folder %s, session_id %s', folder, session_id)

        if os.path.exists(spm_fact_file):
            with open(spm_fact_file, 'r') as f:
                spm_facts = json.load(f)
                self.xcom_push(context, key='matlab_version', value=spm_facts['general']['matlab_version'])
                self.xcom_push(context, key='spm_version', value=spm_facts['general']['spm_version'])
                self.xcom_push(context, key='spm_revision', value=spm_facts['general']['spm_revision'])
                self.xcom_push(context, key='provenance_details', value=json.dumps(spm_facts))

        self.xcom_push(context, key='folder', value=folder)
        self.xcom_push(context, key='session_id', value=session_id)
        self.xcom_push(context, key='dataset', value=dataset)
