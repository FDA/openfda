#!/bin/bash

#
# Python
#

WITH_PYPY=${WITH_PYPY:=0}

# On OSX, we must use greadlink (aka gnu readlink) for -m.
# This is installed via homebrew coreutils.
READLINK=$(which greadlink || which readlink)

rm -rf build/
PYTHON_ENV=$($READLINK -m ./_python-env)

# Fetch virtualenv
if [[ $WITH_PYPY -eq 1 ]]; then
  PYTHON=$(which pypy)
  if ! which pypy; then
    echo "PyPy was not found.  You can download from:"
    echo "http://pypy.org/download.html"
    echo 'https://bitbucket.org/pypy/pypy/downloads/pypy-2.3-linux64.tar.bz2  (linux)'
    echo 'https://bitbucket.org/pypy/pypy/downloads/pypy3-2.1-beta1-osx64.tar.bz2 (mac)'
    exit
  fi
else
  PYTHON=$(which python)
fi

pip install --user virtualenv

# Setup virtualenv if it doesn't exist.
test -e $PYTHON_ENV || virtualenv -p $PYTHON $PYTHON_ENV

# Install project sources and dependencies into the environment
$PYTHON_ENV/bin/pip uninstall -y openfda || true
$PYTHON_ENV/bin/python setup.py develop

#
# Node
#
pushd api/faers
npm install
popd
