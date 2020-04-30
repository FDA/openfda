#!/bin/bash

export LUIGI_CONFIG_PATH=./config/luigi.cfg
export PYTHON=./_python-env/bin/python
export LOGDIR=./logs

mkdir -p $LOGDIR

$PYTHON openfda/faers/pipeline.py LoadJSON --quarter=all > $LOGDIR/faers.log 2>&1
