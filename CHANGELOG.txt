
# Changelog

## 2.4.0 - 2017-06-07

* DockerPipelineOperator: add cleanup_output_folder parameter.

## 2.3.0 - 2017-05-17

* Update to Apache Airflow 1.8.1
* Improve values in pipeline XComs

## 2.2.0 - 2017-03-30

* SpmPipelineOperator inherits from SpmOperator

## 2.1.0 - 2017-03-28

* Remove dataset argument from ScanFlatFolderPipelineOperator

## 2.0.0 - 2017/03/28

* Refactor airflow_scan_operator package.
  * All operators prefixed with 'Scan'
  * Add ScanFlatFolderPipelineOperator
  * Add extract_context_callable to extract information from the paths,
    removes the need for special operators to scan organised / non organised folders

## 1.3.0

* Add data_config parameter to PythonPipelineOperator

## 1.2.0

* Add provenance tracking to Python pipeline operator

## 1.1.0

* Optional output volume for Docker Pipeline operator

## 1.0.0 - First public release
