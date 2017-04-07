
"""
.. module:: operators.scan_folder_pipeline_operator

    :synopsis: ScanFlatFolderPipelineOperator triggers a DAG run for a specified ``dag_id`` for each folder discovered
               in a parent folder, where the parent folder location is provided by the pipeline XCOMs.

.. moduleauthor:: Ludovic Claude <ludovic.claude@chuv.ch>
"""

from airflow.utils import apply_defaults
from airflow_pipeline.pipelines import TransferPipelineXComs

from .scan_folder_operator import ScanFlatFolderOperator

from .common import default_extract_context, default_trigger_dagrun


class ScanFlatFolderPipelineOperator(ScanFlatFolderOperator, TransferPipelineXComs):

    """
    Triggers a DAG run for a specified ``dag_id`` for each folder discovered in a parent folder.
    The parent folder is given by upstream pipeline XCOMs, and this operator should be last one
    called in the currewnt DAG as pipeline XCOMs are not transmitted downstream.

    :param trigger_dag_id: the dag_id to trigger
    :type trigger_dag_id: str
    :param trigger_dag_run_callable: a reference to a python function that will be
        called while passing it the ``context`` object and a placeholder
        object ``obj`` for your callable to fill and return if you want
        a DagRun created. This ``obj`` object contains a ``run_id`` and
        ``payload`` attribute that you can modify in your function.
        The ``run_id`` should be a unique identifier for that DAG run, and
        the payload has to be a picklable object that will be made available
        to your tasks while executing that DAG run. Your function header
        should look like ``def foo(context, dag_run_obj):``
    :type trigger_dag_run_callable: python callable
    :param extract_context_callable: a reference to a python function that will be
        called while passing it a ```root_folder``` and a ```folder``` string. The function
        should return a dictionary containing the context attributes that it could extract from the paths.
    :type extract_context_callable: python callable
    :param accept_folder_callable: a reference to a python function that will be
        called while passing it a ```path``` string. The function
        should return True if it accepts the folder and the operator will trigger a
        new DAG run of that folder, False otherwise.
    :type accept_folder_callable: python callable
    :param depth: Depth of folders to traverse. Default: 1
    :type depth: int
    :param parent_task: name of the parent task to use to locate XCom parameters
    :type parent_task: str
    :param dataset_config: Collection of flags and setting related to the dataset:
        - boost_provenance_scan: When True, we consider that all the files from same folder share the same meta-data.
        The processing is 2x faster. Enabled by default.
        - session_id_by_patient: Rarely, a data set might use study IDs which are unique by patient (not for the whole
        study).
        E.g.: LREN data. In such a case, you have to enable this flag. This will use PatientID + StudyID as a session
        ID.
    :type dataset_config: list
    """

    template_fields = tuple()
    template_ext = tuple()
    ui_color = '#cceeeb'

    @apply_defaults
    def __init__(
            self,
            trigger_dag_id,
            trigger_dag_run_callable=default_trigger_dagrun,
            extract_context_callable=default_extract_context,
            accept_folder_callable=None,
            depth=1,
            parent_task=None,
            dataset_config=None,
            organised_folder=True,
            *args, **kwargs):
        super(ScanFlatFolderPipelineOperator, self).__init__(dataset=None,  # will be filled by pipeline XCOMs
                                                             folder=None,
                                                             trigger_dag_id=trigger_dag_id,
                                                             trigger_dag_run_callable=trigger_dag_run_callable,
                                                             extract_context_callable=extract_context_callable,
                                                             accept_folder_callable=accept_folder_callable,
                                                             depth=depth,
                                                             *args, **kwargs)
        TransferPipelineXComs.__init__(self, parent_task, dataset_config, organised_folder)

    def root_folder(self, context):
        return self.pipeline_xcoms['folder']

    def pre_execute(self, context):
        super(ScanFlatFolderPipelineOperator, self).pre_execute(context)
        self.read_pipeline_xcoms(context, expected=['folder'])

    # def execute(self, context)
    #   No need to write pipeline XCOMs, this operator should be the last in the DAG
