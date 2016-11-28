from airflow.hooks.mysql_hook import MySqlHook
from airflow.operators.python_operator import PythonOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils import apply_defaults
from airflow import configuration

import os, sys
import logging

logging.info("Loading Dicom plugin...")

class DicomOperator(object):

    @apply_defaults
    def __init__(self, *args, **kwargs):

        super(PythonOperator, self).__init__(*args, **kwargs)


class DicomPlugin(AirflowPlugin):

    name = "dicom"
    operator = [DicomOperator]
