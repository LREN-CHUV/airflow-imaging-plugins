# -*- coding: UTF-8 -*-
from setuptools import setup, find_packages

_version = '2.1.0'
_packages = find_packages(exclude=["docs", "*.tests", "*.tests.*", "tests.*", "tests"])

_short_description = ("pylint-common is a Pylint plugin to improve Pylint "
                      "error analysis of the standard Python library")

_classifiers = (
    'Development Status :: 4 - Beta',
    'Environment :: Plugins',
    'Intended Audience :: Developers',
    'Intended Audience :: Science/Research',
    'Operating System :: Unix',
    'License :: OSI Approved :: Apache Software License',
    'Topic :: Scientific/Engineering :: Bio-Informatics',
    'Programming Language :: Python :: 3 :: Only',
    'Programming Language :: Python :: 3.3',
    'Programming Language :: Python :: 3.4',
    'Programming Language :: Python :: 3.5',
    'Programming Language :: Python :: 3.6',
)

_install_requires = [
    'airflow>=1.7.0',
    'data_tracking>=1.5.4'
]

setup(
    name="airflow_imaging_plugins",
    description="Airflow plugins to support Neuroimaging tasks.",
    author='LREN CHUV',
    author_email='ludovic.claude@chuv.ch',
    url='https://github.com/LREN-CHUV/airflow-imaging-plugins',
    license='Apache License 2.0',
    zip_safe=False,
    version=_version,
    packages=_packages,
    classifiers=_classifiers,
    keywords='airflow mri provenance',
    install_requires=_install_requires
)
