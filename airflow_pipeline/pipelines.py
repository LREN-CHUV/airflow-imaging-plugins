import logging

from textwrap import dedent

PIPELINE_XCOMS = ['folder', 'session_id', 'participant_id',
                  'scan_date', 'spm_output', 'spm_error', 'dataset']


def pipeline_trigger(parent_task):
    """
      Use this function with TriggerDagRunOperator to always trigger a DAG and
      pass pipeline information to the next DAG
    """

    def trigger(context, dag_run_obj):
        """
          Use this function with TriggerDagRunOperator to always trigger a DAG and
          pass pipeline information to the next DAG
        """
        ti = context['task_instance']
        dr = context['dag_run']
        dag_run_obj.payload = {}
        for key in PIPELINE_XCOMS:
            dag_run_obj.payload[key] = ti.xcom_pull(
                task_ids=parent_task, key=key)
            if (not dag_run_obj.payload[key]) and key in dr.conf:
                dag_run_obj.payload[key] = dr.conf[key]
        return dag_run_obj

    return trigger


class TransferPipelineXComs(object):

    def __init__(self, parent_task):
        self.parent_task = parent_task
        self.pipeline_xcoms = {}
        self.incoming_parameters = dedent("""
          # Task __{{ task.task_id }}__
          ## Incoming parameters

          dataset = __{{ task_instance.xcom_pull(task_ids='$parent_task', key='dataset') }}__
          folder = __{{ task_instance.xcom_pull(task_ids='$parent_task', key='folder') }}__
          session_id = __{{ task_instance.xcom_pull(task_ids='$parent_task', key='session_id') }}__
          scan_date = __{{ task_instance.xcom_pull(task_ids='$parent_task', key='scan_date') }}__

          {% set spm_output = task_instance.xcom_pull(task_ids='$parent_task', key='spm_output') %}
          {% set spm_error = task_instance.xcom_pull(task_ids='$parent_task', key='spm_error') %}
          {% if spm_output or spm_error %}

          ## SPM output from previous task ($parent_task)
          ### Output
          {{ spm_output }}
          ### Errors
          {{ spm_error }}
          {% endif %}
        """.replace("$parent_task",parent_task))

    def read_pipeline_xcoms(self, context, expected=None):
        expected = expected or []
        for xcom in PIPELINE_XCOMS:
            value = self.xcom_pull(
                context, task_ids=self.parent_task, key=xcom)
            if value:
                self.pipeline_xcoms[xcom] = value
            elif xcom in expected:
                logging.warning("xcom argument '%s' is empty" % xcom)

    def write_pipeline_xcoms(self, context):
        for key, value in self.pipeline_xcoms.items():
            self.xcom_push(context, key=key, value=value)
