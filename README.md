[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](https://github.com/LREN-CHUV/airflow-imaging-plugins/blob/master/LICENSE) [![Codacy Badge](https://api.codacy.com/project/badge/Grade/7a9c796392e4420495ee1fabd0fce9ae)](https://www.codacy.com/app/hbp-mip/airflow-imaging-plugins?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=LREN-CHUV/airflow-imaging-plugins&amp;utm_campaign=Badge_Grade) [![Code Health](https://landscape.io/github/LREN-CHUV/airflow-imaging-plugins/master/landscape.svg?style=flat)](https://landscape.io/github/LREN-CHUV/airflow-imaging-plugins/master) [![PyPI](https://img.shields.io/pypi/v/airflow-imaging-plugins.svg)](https://pypi.python.org/pypi/airflow-imaging-plugins/)

# Airflow Imaging plugins

Airflow plugins providing support for preprocessing of neuroimaging data.

The following packages are provided:

* airflow_freespace: Sensors that check the amount of free space on the disk and waits until enough free space is available
* airflow_pipeline: Operators and helpers to build generic processing pipelines
* airflow_scan_folder: Operators used to scan folders for new work
* airflow_spm: Operators adapting Matlab and [SPM 12](http://www.fil.ion.ucl.ac.uk/spm) to work inside Airflow

## Usage

Imaging data is organised by folders, where each fist-level folder represents a scanning session.

A 'pipeline' represents the steps to process a folder containing one scanning session.
In Airflow, we use the XCOM mechanism to transmit data from one step of the pipeline to the next step.
This is why each processing pipelines need to start with airflow_pipeline.operators.PreparePipelineOperator as it injects into XCOM the necessary information that is required for the other \*PipelineOperator:

All Python callback functions provided to those operators can use as arguments the following variables coming from XCOM:

* folder
* session_id
* participant_id
* scan_date
* output
* error
* dataset
* matlab_version
* spm_version
* spm_revision
* provenance_details
* provenance_previous_step_id
* relative_context_path

See airflow_pipeline.pipelines.PIPELINE_XCOMS for an up-to-date list

## List of plugins

* airflow_spm.operators.SpmOperator: Executes SPM or just Matlab
* airflow_spm.operators.SpmPipelineOperator: Executes a pipeline on SPM, where a 'pipeline' is a function implemented in SPM
* airflow_freespace.operators.FreeSpaceSensor: Waits for enough free disk space on the disk.
* airflow_scan_folder.operators.ScanFlatFolderOperator: Triggers a DAG run for a specified ``dag_id`` for each scan folder discovered in a folder.
* airflow_scan_folder.operators.ScanDailyFolderOperator: Triggers a DAG run for a specified ``dag_id`` for each scan folder discovered in a daily folder.
* airflow_scan_folder.operators.ScanFlatFolderPipelineOperator: Triggers a DAG run for a specified ``dag_id`` for each folder discovered in a parent folder, where the parent folder location is provided by the pipeline XCOMs.
* airflow_pipeline.operators.PreparePipelineOperator: An operator that prepares the pipeline
* airflow_pipeline.operators.PythonPipelineOperator: A PythonOperator that moves XCOM data used by the pipeline
* airflow_pipeline.operators.BashPipelineOperator: A BashOperator that registers provenance information in the pipeline
* airflow_pipeline.operators.DockerPipelineOperator: A DockerOperator that registers provenance information in the pipeline

Python version: 3

## Installation

```
  pip3 install from git: pip3 install git+https://github.com/LREN-CHUV/airflow-imaging-plugins.git@master#egg=airflow_imaging_plugins
```

## Setup and configuration

### Airflow setup for MRI scans pipeline:

* In Airflow config file, add the [spm] section with the following entries:
   * SPM_DIR: root path to the installation of SPM
