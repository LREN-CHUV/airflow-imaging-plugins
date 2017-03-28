"""Common code for the scan folder operators"""

import logging
import random
import copy

from datetime import datetime, timedelta

from airflow.models import BaseOperator, DagRun
from airflow.utils import apply_defaults
from airflow.utils.state import State
from airflow.operators.dagrun_operator import DagRunOrder
from airflow.utils.db import provide_session
from sqlalchemy.exc import IntegrityError


def default_look_for_ready_marker_file(daily_folder_date):
    return daily_folder_date.date() == datetime.today().date()


def round_up_time(dt=None, date_delta=timedelta(minutes=1)):
    """
    Round a datetime object to a multiple of a timedelta.

    dt : datetime.datetime object, default now.
    dateDelta : timedelta object, we round to a multiple of this, default 1 minute.
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
    """

    template_fields = tuple()
    template_ext = tuple()

    @apply_defaults
    def __init__(
            self,
            trigger_dag_id,
            dataset=None,
            python_callable=None,
            *args, **kwargs):
        super(FolderOperator, self).__init__(*args, **kwargs)
        self.trigger_dag_id = trigger_dag_id
        self.dataset = dataset
        self.python_callable = python_callable
        self.offset = 1

    @provide_session
    def trigger_dag_run(self, context, custom_context_params, session=None):
        context = copy.copy(context)
        context_params = context['params']
        context_params['dataset'] = self.dataset
        context_params.update(custom_context_params)

        while True:
            dr_time = round_up_time(datetime.now() - timedelta(minutes=self.offset))
            run_id = "trig__{0}".format(dr_time.isoformat())
            dr = session.query(DagRun).filter(
                DagRun.dag_id == self.trigger_dag_id, DagRun.run_id == run_id).first()
            if dr:
                # Try to avoid too many collisions when backfilling a long
                # backlog
                self.offset = self.offset - random.randint(1, 100)
            else:
                break

        context['start_date'] = dr_time

        dro = DagRunOrder(run_id=run_id)
        dro = self.python_callable(context, dro)
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
                self.trigger_dag_run(context, custom_context_params)
        else:
            logging.info("Criteria not met, moving on")
