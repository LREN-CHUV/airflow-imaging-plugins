|CHUV| |License| |Codacy Badge| |PyPI| |CircleCI|

Airflow Imaging plugins
=======================

Airflow plugins providing support for preprocessing of neuroimaging
data.

The following packages are provided:

-  airflow\_freespace: Sensors that check the amount of free space on
   the disk and waits until enough free space is available
-  airflow\_pipeline: Operators and helpers to build generic processing
   pipelines
-  airflow\_scan\_folder: Operators used to scan folders for new work
-  airflow\_spm: Operators adapting Matlab and `SPM
   12 <http://www.fil.ion.ucl.ac.uk/spm>`__ to work inside Airflow

Usage
-----

Imaging data is organised by folders, where each fist-level folder
represents a scanning session.

A 'pipeline' represents the steps to process a folder containing one
scanning session. In Airflow, we use the XCOM mechanism to transmit data
from one step of the pipeline to the next step. This is why each
processing pipelines need to start with
airflow\_pipeline.operators.PreparePipelineOperator as it injects into
XCOM the necessary information that is required for the other
\*PipelineOperator:

All Python callback functions provided to those operators can use as
arguments the following variables coming from XCOM:

-  folder
-  session\_id
-  participant\_id (optional)
-  scan\_date (optional)
-  output
-  error
-  dataset
-  matlab\_version
-  spm\_version
-  spm\_revision
-  provenance\_details
-  provenance\_previous\_step\_id
-  relative\_context\_path

See airflow\_pipeline.pipelines.PIPELINE\_XCOMS for an up-to-date list

List of plugins
---------------

-  airflow\_spm.operators.SpmOperator: Executes SPM or just Matlab
-  airflow\_spm.operators.SpmPipelineOperator: Executes a pipeline on
   SPM, where a 'pipeline' is a function implemented in SPM
-  airflow\_freespace.operators.FreeSpaceSensor: Waits for enough free
   disk space on the disk.
-  airflow\_scan\_folder.operators.ScanFlatFolderOperator: Triggers a
   DAG run for a specified ``dag_id`` for each scan folder discovered in
   a folder.
-  airflow\_scan\_folder.operators.ScanDailyFolderOperator: Triggers a
   DAG run for a specified ``dag_id`` for each scan folder discovered in
   a daily folder.
-  airflow\_scan\_folder.operators.ScanFlatFolderPipelineOperator:
   Triggers a DAG run for a specified ``dag_id`` for each folder
   discovered in a parent folder, where the parent folder location is
   provided by the pipeline XCOMs.
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

build
-----

Run ``./build.sh``.

Push on PyPi
------------

Run ``./publish.sh``.

(This builds the project prior to pushing it).

.. |CHUV| image:: https://img.shields.io/badge/CHUV-LREN-AF4C64.svg
   :target: https://www.unil.ch/lren/en/home.html
.. |License| image:: https://img.shields.io/badge/license-Apache--2.0-blue.svg
   :target: https://github.com/LREN-CHUV/airflow-imaging-plugins/blob/master/LICENSE
.. |Codacy Badge| image:: https://api.codacy.com/project/badge/Grade/7a9c796392e4420495ee1fabd0fce9ae
   :target: https://www.codacy.com/app/hbp-mip/airflow-imaging-plugins?utm_source=github.com&utm_medium=referral&utm_content=LREN-CHUV/airflow-imaging-plugins&utm_campaign=Badge_Grade
.. |PyPI| image:: https://img.shields.io/pypi/v/airflow-imaging-plugins.svg
   :target: https://pypi.python.org/pypi/airflow-imaging-plugins/
.. |CircleCI| image:: https://circleci.com/gh/HBPMedical/airflow-imaging-plugins.svg?style=svg
   :target: https://circleci.com/gh/HBPMedical/airflow-imaging-plugins
