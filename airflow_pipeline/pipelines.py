
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
        for key in ['folder', 'session_id', 'participant_id', 'scan_date', 'spm_output', 'spm_error']:
            dag_run_obj.payload[key] = ti.xcom_pull(task_ids=parent_task, key=key) | dr.conf[key] | None
        return dag_run_obj

    return trigger
