
"""
.. module:: operators.folder_operator

    :synopsis: DailyFolderOperator triggers a DAG run for a specified ``dag_id`` for the daily
               folder matching path root_folder/yyyy/yyyyMMdd where the date used is the execution date.
               FlatFolderOperator triggers a DAG run for a specified ``dag_id`` for each folder discovered
               in a parent folder.

.. moduleauthor:: Ludovic Claude <ludovic.claude@chuv.ch>
"""


import logging

import os

from airflow.utils import apply_defaults
from airflow.exceptions import AirflowSkipException
from airflow.utils.db import provide_session

from .common import FolderOperator, default_look_for_ready_marker_file


def default_trigger_dagrun(context, dag_run_obj):
    if True:
        folder = context['params']['folder']
        start_date = context['start_date']
        # The payload will be available in target dag context as
        # kwargs['dag_run'].conf
        dag_run_obj.payload = context['params']
        # Use a granularity of days for run_id to avoid restarting the same computation on
        # a MRI scan several times on the same day. Recomputation is expensive and should be minimised,
        # to force a re-computation an administrator will need to delete existing DagRuns or configure the
        # DAG to use another trigger_dagrun function that uses another naming convention for run_id
        dag_run_obj.run_id = folder + '-' + \
            start_date.strftime('%Y%m%d')
        return dag_run_obj


class FlatFolderOperator(FolderOperator):

    """
    Triggers a DAG run for a specified ``dag_id`` for each folder discovered in a parent folder.

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
    :param depth: Depth of folders to traverse. Default: 1
    :type depth: int
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
            depth=1,
            *args, **kwargs):
        super(FlatFolderOperator, self).__init__(python_callable=python_callable,
                                                 trigger_dag_id=trigger_dag_id,
                                                 dataset=dataset,
                                                 *args, **kwargs)
        self.folder = folder
        self.depth = depth

    def execute(self, context):
        self.scan_dirs(self.folder, context)

    @provide_session
    def scan_dirs(self, folder, context, session=None, depth=0, rel_folder='.'):

        if not os.path.exists(folder):
            raise AirflowSkipException

        if depth == self.depth:
            logging.info(
                'Prepare trigger for preprocessing : %s', str(folder))

            self.trigger_dag_run(context, {'folder': folder, 'relative_context_path': rel_folder}, session)
        else:
            if rel_folder == '.':
                rel_folder = ''
            for fname in os.listdir(folder):
                if not (fname in ['.git', '.svn', '.tmp']):
                    path = os.path.join(folder, fname)
                    rel_path = "%s/%s" % (rel_folder, fname)
                    if os.path.isdir(path):
                        self.scan_dirs(path, context, session=session, depth=depth + 1, rel_folder=rel_path)


class DailyFolderOperator(FolderOperator):

    """
    Triggers a DAG run for a specified ``dag_id`` for the daily folder.

    The daily folder should match path root_folder/yyyy/yyyyMMdd where the date
    used is the execution date.

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
            look_for_ready_marker_file=default_look_for_ready_marker_file,
            ready_marker_file='.ready',
            *args, **kwargs):
        super(DailyFolderOperator, self).__init__(python_callable=python_callable,
                                                  trigger_dag_id=trigger_dag_id,
                                                  dataset=dataset,
                                                  *args, **kwargs)
        self.folder = folder
        self.look_for_ready_marker_file = look_for_ready_marker_file
        self.ready_marker_file = ready_marker_file

    def execute(self, context):
        self.scan_dirs(self.folder, context)

    @provide_session
    def scan_dirs(self, folder, context, session=None):
        daily_folder_date = context['execution_date']

        if not os.path.exists(folder):
            raise AirflowSkipException

        rel_daily_folder = daily_folder_date.strftime('%Y'), daily_folder_date.strftime('%Y%m%d')
        daily_folder = os.path.join(folder, daily_folder_date.strftime(
            '%Y'), daily_folder_date.strftime('%Y%m%d'))

        if not os.path.isdir(daily_folder):
            raise AirflowSkipException

        ready_marker_file = os.path.join(daily_folder, self.ready_marker_file)
        if not self.look_for_ready_marker_file(daily_folder_date) or os.access(ready_marker_file, os.R_OK):

            logging.info(
                'Prepare trigger for preprocessing : %s', str(daily_folder))

            self.trigger_dag_run(context, {'folder': daily_folder, 'relative_context_path': rel_daily_folder}, session)
            self.offset = self.offset - 1
