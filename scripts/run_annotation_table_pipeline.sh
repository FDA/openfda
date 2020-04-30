#!/bin/bash

set -x

export LUIGI_CONFIG_PATH=./config/luigi.cfg
export PYTHON=./_python-env/bin/python
export LOGDIR=./logs

mkdir -p $LOGDIR

$PYTHON openfda/annotation_table/pipeline.py CombineHarmonization > $LOGDIR/annotation.log 2>&1
