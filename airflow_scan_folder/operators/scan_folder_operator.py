
import logging

import os
import copy

from time import sleep
from datetime import datetime, timedelta

from airflow import settings
from airflow.models import BaseOperator, DagRun
from airflow.utils import apply_defaults
from airflow.utils.state import State
from airflow.operators.dagrun_operator import DagRunOrder
from airflow.exceptions import AirflowSkipException
from sqlalchemy.exc import IntegrityError


def default_is_valid_session_id(session_id):
    sid = session_id.strip().lower()
    return not ('delete' in sid) and not ('phantom' in sid)


def default_look_for_ready_file_marker(daily_folder_date):
    return daily_folder_date.date() == datetime.today().date()


def default_trigger_dagrun(context, dag_run_obj):
    if True:
        session_id = context['params']['session_id']
        start_date = context['start_date']
        # The payload will be available in target dag context as
        # kwargs['dag_run'].conf
        dag_run_obj.payload = context['params']
        dag_run_obj.run_id = session_id + '-' + \
            start_date.strftime('%Y%m%d-%H%M')
        return dag_run_obj


def roundUpTime(dt=None, dateDelta=timedelta(minutes=1)):
    """Round a datetime object to a multiple of a timedelta
    dt : datetime.datetime object, default now.
    dateDelta : timedelta object, we round to a multiple of this, default 1 minute.
    Author: Thierry Husson 2012 - Use it as you want but don't blame me.
            Stijn Nevens 2014 - Changed to use only datetime objects as variables
    """
    roundTo = dateDelta.total_seconds()

    if dt == None:
        dt = datetime.now()
    seconds = (dt - dt.min).seconds
    # rounding up, // is a floor division, not a comment on following line:
    rounding = (seconds + roundTo) // roundTo * roundTo
    return dt + timedelta(0, rounding - seconds, -dt.microsecond)


class ScanFolderOperator(BaseOperator):
    """
    Triggers a DAG run for a specified ``dag_id`` for each scan folder discovered
    in a daily folder

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
            is_valid_session_id=default_is_valid_session_id,
            look_for_ready_file_marker=default_look_for_ready_file_marker,
            ready_file_marker='.ready',
            *args, **kwargs):
        super(ScanFolderOperator, self).__init__(*args, **kwargs)
        self.folder = folder
        self.python_callable = python_callable
        self.trigger_dag_id = trigger_dag_id
        self.is_valid_session_id = is_valid_session_id
        self.look_for_ready_file_marker = look_for_ready_file_marker
        self.ready_file_marker = ready_file_marker

    def execute(self, context):
        self.scan_dirs(self.folder, context)

    def scan_dirs(self, folder, context):
        daily_folder_date = context['execution_date']
        offset = 1

        if not os.path.exists(folder):
            raise AirflowSkipException

        daily_folder = os.path.join(folder, daily_folder_date.strftime(
            '%Y'), daily_folder_date.strftime('%Y%m%d'))

        if not os.path.isdir(daily_folder):
            daily_folder = os.path.join(
                folder, '2014', daily_folder_date.strftime('%Y%m%d'))

        if not os.path.isdir(daily_folder):
            raise AirflowSkipException

        for fname in os.listdir(daily_folder):
            path = os.path.join(daily_folder, fname)
            if os.path.isdir(path):

                ready_file_marker = os.path.join(path, self.ready_file_marker)
                if self.is_valid_session_id(fname) and not self.look_for_ready_file_marker(daily_folder_date) or os.access(ready_file_marker, os.R_OK):

                    logging.info(
                        'Prepare trigger for preprocessing : %s', str(fname))

                    self.trigger_dag_run(context, path, fname, offset)
                    offset = offset + 1

    def trigger_dag_run(self, context, path, session_dir_name, offset):
        context = copy.copy(context)
        context_params = context['params']
        # Folder containing the DICOM files to process
        context_params['folder'] = path
        # Session ID identifies the session for a scan. The
        # last part of the folder path should match session_id
        context_params['session_id'] = session_dir_name

        session = settings.Session()
        while True:
            dr_time = roundUpTime(dateDelta=timedelta(minutes=offset))
            run_id = "trig__{0}".format(dr_time.isoformat())
            dr = session.query(DagRun).filter(
                DagRun.dag_id == self.trigger_dag_id, DagRun.run_id == run_id).first()
            if dr:
                offset = offset + 1
            else:
                break

        context['start_date'] = dr_time

        try:
            dro = DagRunOrder(run_id=run_id)
            dro = self.python_callable(context, dro)
            if dro:
                dr = DagRun(
                    dag_id=self.trigger_dag_id,
                    run_id=dro.run_id,
                    execution_date=dr_time,
                    state=State.RUNNING,
                    conf=dro.payload,
                    external_trigger=True)
                session.add(dr)
                try:
                    session.commit()
                except IntegrityError:
                    # Bad luck, some concurrent thread has already created an execution
                    # at this time
                    session.rollback()
                    session.close()
                    session = None
                    self.trigger_dag_run(context, path, session_dir_name, offset + 1)
                logging.info("Created DagRun {}".format(dr))
            else:
                logging.info("Criteria not met, moving on")
        finally:
            if session:
                session.close()
