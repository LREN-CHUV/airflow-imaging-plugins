
"""
.. module:: operators.scan_folder_operator

    :synopsis: ScanDailyFolderOperator triggers a DAG run for a specified ``dag_id`` for the daily
               folder matching path root_folder/yyyy/yyyyMMdd where the date used is the execution date.
               ScanFlatFolderOperator triggers a DAG run for a specified ``dag_id`` for each folder discovered
               in a parent folder.

.. moduleauthor:: Ludovic Claude <ludovic.claude@chuv.ch>
"""


import logging

import os

from airflow.utils import apply_defaults
from airflow.exceptions import AirflowSkipException
from airflow.utils.db import provide_session

from .common import FolderOperator, default_extract_context, default_look_for_ready_marker_file
from .common import default_trigger_dagrun, default_build_daily_folder_path_callable


def _is_valid_folder_depth(folder, depth):
    if not os.path.exists(folder):
        return False
    for d in range(0, depth):
        try:
            folder = os.path.join(folder, os.listdir(folder)[0])
        except (OSError, IndexError):
            logging.warning("Cannot find folder with depth %s from folder %s", str(depth), folder)
            return False
    return True


class ScanFlatFolderOperator(FolderOperator):

    """
    Triggers a DAG run for a specified ``dag_id`` for each folder discovered in a parent folder.

    :param dataset: name of the dataset
    :type dataset: str
    :param folder: root folder
    :type folder: str
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
    """

    template_fields = tuple()
    template_ext = tuple()
    ui_color = '#cceeeb'

    @apply_defaults
    def __init__(
            self,
            dataset,
            folder,
            trigger_dag_id,
            trigger_dag_run_callable=default_trigger_dagrun,
            extract_context_callable=default_extract_context,
            accept_folder_callable=None,
            depth=1,
            *args, **kwargs):
        super(ScanFlatFolderOperator, self).__init__(dataset=dataset,
                                                     trigger_dag_id=trigger_dag_id,
                                                     trigger_dag_run_callable=trigger_dag_run_callable,
                                                     extract_context_callable=extract_context_callable,
                                                     *args, **kwargs)
        self.folder = folder
        self.accept_folder_callable = accept_folder_callable
        self.depth = depth

    def root_folder(self, context):
        return self.folder

    def execute(self, context):
        self.scan_dirs(self.root_folder(context), context)

    @provide_session
    def scan_dirs(self, folder, context, session=None, depth=0):

        if not _is_valid_folder_depth(folder, depth):
            raise AirflowSkipException

        if depth == self.depth:
            logging.info(
                'Prepare trigger for %s : %s', self.trigger_dag_id, str(folder))

            self.trigger_dag_run(context, root_folder=self.root_folder(context), folder=folder, session=session)
            self.offset += 1
        else:
            for fname in os.listdir(folder):
                if not (fname in ['.git', '.svn', '.tmp']):
                    path = os.path.join(folder, fname)
                    if os.path.isdir(path):
                        try:
                            if self.accept_folder_callable(path=path):
                                raise TypeError
                        except TypeError:
                            self.scan_dirs(path, context, session=session, depth=depth + 1)


class ScanDailyFolderOperator(ScanFlatFolderOperator):

    """
    Triggers a DAG run for a specified ``dag_id`` for the daily folder.

    The daily folder should match path root_folder/yyyy/yyyyMMdd where the date
    used is the execution date.

    :param dataset: name of the dataset
    :type dataset: str
    :param folder: root folder
    :type folder: str
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
    :param build_daily_folder_path_callable: a reference to a python function that will be
        called while passing it a ```folder``` and a ```date``` and return the path to
        the daily folder to traverse.
    :type build_daily_folder_path_callable: python callable
    :param look_for_ready_marker_file: a reference to a python function that will be
        called while passing it a date and return True if we need to look for the
        ``ready`` marker file to ensure that file creation and copy operations are complete,
        False otherwise
    :type look_for_ready_marker_file: python callable
    :param ready_marker_file: name of the marker file to look for if necessary
        (if look_for_ready_marker_file said so). Default to '.ready'
    :type ready_marker_file: str
    :param depth: Depth of folders to traverse inside the daily folder. Default: 0
    :type depth: int
    """

    template_fields = tuple()
    template_ext = tuple()
    ui_color = '#cceeeb'

    @apply_defaults
    def __init__(
            self,
            dataset,
            folder,
            trigger_dag_id,
            trigger_dag_run_callable=default_trigger_dagrun,
            extract_context_callable=default_extract_context,
            accept_folder_callable=None,
            build_daily_folder_path_callable=default_build_daily_folder_path_callable,
            look_for_ready_marker_file=default_look_for_ready_marker_file,
            ready_marker_file='.ready',
            depth=0,
            *args, **kwargs):
        super(ScanDailyFolderOperator, self).__init__(dataset=dataset,
                                                      folder=folder,
                                                      trigger_dag_id=trigger_dag_id,
                                                      trigger_dag_run_callable=trigger_dag_run_callable,
                                                      extract_context_callable=extract_context_callable,
                                                      accept_folder_callable=accept_folder_callable,
                                                      depth=depth,
                                                      *args, **kwargs)
        self.build_daily_folder_path_callable = build_daily_folder_path_callable
        self.look_for_ready_marker_file = look_for_ready_marker_file
        self.ready_marker_file = ready_marker_file

    def execute(self, context):
        self.scan_daily_dirs(self.folder, context)

    @provide_session
    def scan_daily_dirs(self, folder, context, session=None):
        daily_folder_date = context['execution_date']

        if not os.path.exists(folder):
            raise AirflowSkipException

        daily_folder = self.build_daily_folder_path_callable(folder, daily_folder_date)

        if not os.path.isdir(daily_folder):
            raise AirflowSkipException

        ready_marker_file = os.path.join(daily_folder, self.ready_marker_file)
        if not self.look_for_ready_marker_file(daily_folder_date) or os.access(ready_marker_file, os.R_OK):

            self.scan_dirs(folder, context, session)
