
"""
.. module:: operators.scan_folder_operator

    :synopsis: ScanFolderOperator to look for the list of folders containing scans starting from a parent folder
               and for each folder to trigger a pipeline DAG run.
               ScanDailyFolderOperator to look for the list of folders containing scans starting from
               a subfolder matching the execution date year then the full execution date day and
               for each folder to trigger a pipeline DAG run.

.. moduleauthor:: Ludovic Claude <ludovic.claude@chuv.ch>
"""


import logging
import os

from airflow.utils import apply_defaults
from airflow.exceptions import AirflowSkipException
from airflow.utils.db import provide_session

from .common import FolderOperator, default_look_for_ready_marker_file


def default_is_valid_session_id(session_id):
    sid = session_id.strip().lower()
    return not ('delete' in sid) and not ('phantom' in sid)


def default_trigger_dagrun(context, dag_run_obj):
    if True:
        session_id = context['params']['session_id']
        start_date = context['start_date']
        # The payload will be available in target dag context as
        # kwargs['dag_run'].conf
        dag_run_obj.payload = context['params']
        # Use a granularity of days for run_id to avoid restarting the same computation on
        # a MRI scan several times on the same day. Recomputation is expensive and should be minimised,
        # to force a re-computation an administrator will need to delete existing DagRuns or configure the
        # DAG to use another trigger_dagrun function that uses another naming convention for run_id
        dag_run_obj.run_id = session_id + '-' + \
            start_date.strftime('%Y%m%d')
        return dag_run_obj


class ScanFolderOperator(FolderOperator):

    """
    Triggers a DAG run for a specified ``dag_id`` for each scan folder discovered in a parent folder.

    :param folder: root folder
    :type folder: str
    :param trigger_dag_id: the dag_id to trigger
    :type trigger_dag_id: str
    :param python_callable: a reference to a python function that will be
        called while passing it the ``context`` object and a placeholder
        object ``obj`` for your callable to fill and return if you want
        a DagRun created. This ``obj`` object contains a ``run_id`` and
        ``payload`` attribute that you can modify in your function.
        The ``run_id`` should be a unique identifier for that DAG run, and
        the payload has to be a picklable object that will be made available
        to your tasks while executing that DAG run. Your function header
        should look like ``def foo(context, dag_run_obj):``
    :type python_callable: python callable
    :param dataset: name of the dataset
    :type dataset: str
    :param is_valid_session_id: a reference to a python function that will be
        called while passing it the ``session_id`` string and return True if
        the session is valid, False otherwise
    :type is_valid_session_id: python callable
    """

    template_fields = tuple()
    template_ext = tuple()
    ui_color = '#cceeeb'

    @apply_defaults
    def __init__(
            self,
            folder,
            trigger_dag_id,
            python_callable=default_trigger_dagrun,
            dataset=None,
            is_valid_session_id=default_is_valid_session_id,
            *args, **kwargs):
        super(ScanFolderOperator, self).__init__(trigger_dag_id,
                                                 dataset=dataset,
                                                 python_callable=python_callable, *args, **kwargs)
        self.folder = folder
        self.is_valid_session_id = is_valid_session_id

    def execute(self, context):
        self.scan_dirs(self.folder, context)

    @provide_session
    def scan_dirs(self, folder, context, session=None):
        if not os.path.exists(folder):
            raise AirflowSkipException

        for fname in os.listdir(folder):
            if not (fname in ['.git', '.svn', '.tmp']):
                path = os.path.join(folder, fname)
                if os.path.isdir(path):
                    if self.is_valid_session_id(fname):

                        logging.info(
                            'Prepare trigger for preprocessing : %s', str(fname))

                        self.trigger_dag_run(context, {'folder': path, 'session_id': fname}, session)
                        self.offset = self.offset + 1


class ScanDailyFolderOperator(FolderOperator):

    """
    Triggers a DAG run for a specified ``dag_id`` for each scan folder discovered in a daily folder.

    The day is related to the ``execution_date`` given in the context of the DAG.

    :param folder: root folder
    :type folder: str
    :param trigger_dag_id: the dag_id to trigger
    :type trigger_dag_id: str
    :param python_callable: a reference to a python function that will be
        called while passing it the ``context`` object and a placeholder
        object ``obj`` for your callable to fill and return if you want
        a DagRun created. This ``obj`` object contains a ``run_id`` and
        ``payload`` attribute that you can modify in your function.
        The ``run_id`` should be a unique identifier for that DAG run, and
        the payload has to be a picklable object that will be made available
        to your tasks while executing that DAG run. Your function header
        should look like ``def foo(context, dag_run_obj):``
    :type python_callable: python callable
    :param dataset: name of the dataset
    :type dataset: str
    :param look_for_ready_marker_file: a reference to a python function that will be
        called while passing it a date and return True if we need to look for the
        ``ready`` marker file to ensure that file creation and copy operations are complete,
        False otherwise
    :type look_for_ready_marker_file: python callable
    :param is_valid_session_id: a reference to a python function that will be
        called while passing it the ``session_id`` string and return True if
        the session is valid, False otherwise
    :type is_valid_session_id: python callable
    """

    template_fields = tuple()
    template_ext = tuple()
    ui_color = '#bbefeb'

    @apply_defaults
    def __init__(
            self,
            folder,
            trigger_dag_id,
            python_callable=default_trigger_dagrun,
            dataset=None,
            look_for_ready_marker_file=default_look_for_ready_marker_file,
            ready_marker_file='.ready',
            is_valid_session_id=default_is_valid_session_id,
            *args, **kwargs):
        super(ScanDailyFolderOperator, self).__init__(folder=folder, trigger_dag_id=trigger_dag_id,
                                                      python_callable=python_callable,
                                                      is_valid_session_id=is_valid_session_id,
                                                      dataset=dataset, *args, **kwargs)
        self.folder = folder
        self.is_valid_session_id = is_valid_session_id
        self.look_for_ready_marker_file = look_for_ready_marker_file
        self.ready_marker_file = ready_marker_file

    def execute(self, context):
        self.scan_dirs(self.folder, context)

    @provide_session
    def scan_dirs(self, folder, context, session=None):
        daily_folder_date = context['execution_date']

        if not os.path.exists(folder):
            raise AirflowSkipException

        daily_folder = os.path.join(folder, daily_folder_date.strftime(
            '%Y'), daily_folder_date.strftime('%Y%m%d'))

        # Ugly hack for LREN
        if not os.path.isdir(daily_folder):
            daily_folder = os.path.join(
                folder, '2014', daily_folder_date.strftime('%Y%m%d'))

        if not os.path.isdir(daily_folder):
            raise AirflowSkipException

        for fname in os.listdir(daily_folder):
            path = os.path.join(daily_folder, fname)
            if os.path.isdir(path):

                ready_marker_file = os.path.join(path, self.ready_marker_file)
                if self.is_valid_session_id(fname) \
                   and not self.look_for_ready_marker_file(daily_folder_date) \
                   or os.access(ready_marker_file, os.R_OK):

                    logging.info(
                        'Prepare trigger for preprocessing : %s', str(fname))

                    self.trigger_dag_run(context, {'folder': path, 'session_id': fname}, session)
                    self.offset = self.offset - 1
