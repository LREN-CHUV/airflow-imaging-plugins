|License| |Codacy Badge| |Code Health|

Airflow Imaging plugins
=======================

Set of plugins helping to work with imaging data in Airflow.

Imaging data is organised by folders, where each fist-level folder
represents a scanning session.

A 'pipeline' represents the steps to process a folder containing one
scanning session. In Airflow, we use the XCOM mechanism to transmit data
from one step of the pipeline to the next step. This is why each
processing pipelines need to start with
airflow\_pipeline.operators.PreparePipelineOperator as it injects into
XCOM the necessary information that is required for the following steps:

-  airflow\_spm.operators.SpmPipelineOperator
-  airflow\_pipeline.operators.PythonPipelineOperator

List of plugins
---------------

-  airflow\_spm.operators.SpmOperator: Executes SPM or just Matlab
-  airflow\_spm.operators.SpmPipelineOperator: Executes a pipeline on
   SPM, where a 'pipeline' is a function implemented in SPM
-  airflow\_freespace.operators.FreeSpaceSensor: Waits for enough free
   disk space on the disk.
-  airflow\_scan\_folder.operators.ScanFolderOperator: Triggers a DAG
   run for a specified ``dag_id`` for each scan folder discovered in a
   folder.
-  airflow\_scan\_folder.operators.ScanDailyFolderOperator: Triggers a
   DAG run for a specified ``dag_id`` for each scan folder discovered in
   a daily folder.
-  airflow\_scan\_folder.operators.FlatFolderOperator: Triggers a DAG
   run for a specified ``dag_id`` for each folder discovered in a parent
   folder.
-  airflow\_scan\_folder.operators.DailyFolderOperator: Triggers a DAG
   run for a specified ``dag_id`` for the daily folder matching path
   root\_folder/yyyy/yyyyMMdd where the date used is the execution date.
-  airflow\_pipeline.operators.PreparePipelineOperator: An operator that
   prepares the pipeline
-  airflow\_pipeline.operators.PythonPipelineOperator: A PythonOperator
   that moves XCOM data used by the pipeline
-  airflow\_pipeline.operators.BashPipelineOperator: A BashOperator that
   registers provenance information in the pipeline
-  airflow\_pipeline.operators.DockerPipelineOperator: A DockerOperator
   that registers provenance information in the pipeline

Python version: 3

Installation
------------

::

      pip3 install from git: pip3 install git+https://github.com/LREN-CHUV/airflow-imaging-plugins.git@master#egg=airflow_imaging_plugins

Setup and configuration
-----------------------

Airflow setup for MRI scans pipeline:
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

-  In Airflow config file, add the [spm] section with the following
   entries:
-  SPM\_DIR: root path to the installation of SPM

.. |License| image:: https://img.shields.io/badge/license-Apache--2.0-blue.svg
   :target: https://github.com/LREN-CHUV/airflow-imaging-plugins/blob/master/LICENSE
.. |Codacy Badge| image:: https://api.codacy.com/project/badge/Grade/7a9c796392e4420495ee1fabd0fce9ae
   :target: https://www.codacy.com/app/hbp-mip/airflow-imaging-plugins?utm_source=github.com&utm_medium=referral&utm_content=LREN-CHUV/airflow-imaging-plugins&utm_campaign=Badge_Grade
.. |Code Health| image:: https://landscape.io/github/LREN-CHUV/airflow-imaging-plugins/master/landscape.svg?style=flat
   :target: https://landscape.io/github/LREN-CHUV/airflow-imaging-plugins/master
