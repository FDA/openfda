#!/bin/bash

#
# Python
#
rm -rf build openfda.egg-info _python-env

export LANG=C
PYTHON_ENV='./_python-env'
python3 -m pip install --user virtualenv
python3 -m pip install awscli

# Setup virtualenv if it doesn't exist.
test -e $PYTHON_ENV || python3 -m venv $PYTHON_ENV

# Install project sources and dependencies into the environment
$PYTHON_ENV/bin/pip uninstall -y openfda || true
$PYTHON_ENV/bin/pip install cython
$PYTHON_ENV/bin/pip install -U -r  requirements.txt

#
# Node
#
pushd api/faers
echo 'installing node modules for API'
npm install
popd
pushd openfda/spl
echo 'installing node modules for build'
npm install
popd
