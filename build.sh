#!/usr/bin/env bash

set -e

# Generate README.rst from README.md (useful to publish on PyPi)
if [ ! $CIRCLECI ]; then
  pandoc --from=markdown --to=rst --output=README.rst README.md
fi

# Remove old builds
rm -rf dist/*
rm -rf airflow_imaging_plugins.egg-info

# Build from setup.py
python3 setup.py bdist_wheel
