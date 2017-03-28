"""Common code for the scan folder operators"""

import logging
import random
import copy
import os

from datetime import datetime, timedelta

from airflow.models import BaseOperator, DagRun
from airflow.utils import apply_defaults
from airflow.utils.state import State
from airflow.operators.dagrun_operator import DagRunOrder
from airflow.utils.db import provide_session
from sqlalchemy.exc import IntegrityError


def default_look_for_ready_marker_file(daily_folder_date):
    """Look for the ready marker file if the folder date is today"""
    return daily_folder_date.date() == datetime.today().date()


def default_extract_context(root_folder, folder, pipeline_xcoms=dict()):
    """Extract the folder and relative_context_path"""
    context = dict()
    context.update(pipeline_xcoms)
    context['folder'] = folder
    context['root_folder'] = root_folder
    context['relative_context_path'] = os.path.relpath(folder, start=root_folder)
    return context


def extract_context_from_session_path(root_folder, folder, pipeline_xcoms=dict()):
    """
    Extract the folder, relative_context_path and session_id from a folder.

    Assumes that the last part of the name represents a session ID
    """
    context = dict()
    context.update(pipeline_xcoms)
    context['folder'] = folder
    context['root_folder'] = root_folder
    context['relative_context_path'] = os.path.relpath(folder, start=root_folder)
    context['session_id'] = os.path.basename(folder)
    return context


def default_trigger_dagrun(context, dag_run_obj):
    """Trigger a DAG run for any folder"""

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


def session_folder_trigger_dagrun(context, dag_run_obj):
    """Trigger a DAG run for a folder containing a scan session"""

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


def default_build_daily_folder_path_callable(folder, date):
    return os.path.join(folder, date.strftime('%Y'), date.strftime('%Y%m%d'))


def default_accept_folder(path):
    return True


def round_up_time(dt=None, date_delta=timedelta(minutes=1)):
    """
    Round a datetime object to a multiple of a timedelta.

    dt : datetime.datetime object, default now.
    date_delta : timedelta object, we round to a multiple of this, default 1 minute.
    Author: Thierry Husson 2012 - Use it as you want but don't blame me.
            Stijn Nevens 2014 - Changed to use only datetime objects as variables
    """

    round_to = date_delta.total_seconds()

    if dt is None:
        dt = datetime.now()
    seconds = (dt - dt.min).seconds
    # rounding up, // is a floor division, not a comment on following line:
    rounding = (seconds + round_to) // round_to * round_to
    return dt + timedelta(0, rounding - seconds, -dt.microsecond)


class FolderOperator(BaseOperator):

    """
    Abstract base class for other FolderOperators.

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
        called while passing it a ```root_folder```, a ```folder``` string and optionally a
        ```pipeline_xcoms``` dictionary. The function should return a dictionary containing
        the context attributes that it could extract from the paths.
    :type extract_context_callable: python callable
    :param dataset: name of the dataset
    :type dataset: str
    """

    template_fields = tuple()
    template_ext = tuple()

    @apply_defaults
    def __init__(
            self,
            dataset,
            trigger_dag_id,
            trigger_dag_run_callable=None,
            extract_context_callable=None,
            *args, **kwargs):
        super(FolderOperator, self).__init__(*args, **kwargs)
        self.dataset = dataset
        self.trigger_dag_id = trigger_dag_id
        self.trigger_dag_run_callable = trigger_dag_run_callable
        self.extract_context_callable = extract_context_callable
        self.offset = 1
        self.pipeline_xcoms = None

    @provide_session
    def trigger_dag_run(self, context, root_folder, folder, session=None):
        context = copy.copy(context)
        context_params = context['params']
        context_params['dataset'] = self.dataset
        if self.extract_context_callable:
            context_params.update(self.extract_context_callable(
                root_folder=root_folder, folder=folder, pipeline_xcoms=self.pipeline_xcoms))

        while True:
            dr_time = round_up_time(datetime.now() - timedelta(minutes=self.offset))
            run_id = "trig__{0}".format(dr_time.isoformat())
            dr = session.query(DagRun).filter(
                DagRun.dag_id == self.trigger_dag_id, DagRun.run_id == run_id).first()
            if dr:
                # Try to avoid too many collisions when backfilling a long
                # backlog
                self.offset = self.offset + random.randint(1, 100)
            else:
                break

        context['start_date'] = dr_time

        dro = DagRunOrder(run_id=run_id)
        dro = self.trigger_dag_run_callable(context, dro)
        if dro:
            dr = DagRun(
                dag_id=self.trigger_dag_id,
                run_id=dro.run_id,
                execution_date=dr_time,
                start_date=datetime.now(),
                state=State.RUNNING,
                conf=dro.payload,
                external_trigger=True)
            session.add(dr)
            try:
                session.commit()
                logging.info("Created DagRun {}".format(dr))
            except IntegrityError:
                # Bad luck, some concurrent thread has already created an execution
                # at this time
                session.rollback()
                session.close()
                session = None
                # Retry, while attempting to avoid too many collisions when
                # backfilling a long backlog
                self.offset = self.offset + random.randint(1, 10000)
                self.trigger_dag_run(context, root_folder, folder)
        else:
            logging.info("Criteria not met, moving on")

    def execute(self, context):
        pass
