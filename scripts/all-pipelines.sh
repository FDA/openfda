#!/bin/bash

set -x

export LUIGI_CONFIG_PATH=./config/luigi.cfg
export PYTHON=./_python-env/bin/python
export LOGDIR=./logs

mkdir -p $LOGDIR

$PYTHON openfda/annotation_table/pipeline.py CombineHarmonization > $LOGDIR/annotation.log 2>&1
$PYTHON openfda/faers/pipeline.py LoadJSON --quarter=all > $LOGDIR/faers.log 2>&1
$PYTHON openfda/res/pipeline.py RunWeeklyProcess > $LOGDIR/res.log 2>&1
$PYTHON openfda/spl/pipeline.py ProcessBatch > $LOGDIR/spl.log 2>&1
$PYTHON openfda/classification/pipeline.py LoadJSON > $LOGDIR/classification.log 2>&1
$PYTHON openfda/device_pma/pipeline.py LoadJSON > $LOGDIR/device_pma.log 2>&1
$PYTHON openfda/device_clearance/pipeline.py LoadJSON > $LOGDIR/device_clearance.log 2>&1
$PYTHON openfda/registration/pipeline.py LoadJSON > $LOGDIR/registration.log 2>&1
$PYTHON openfda/device_recall/pipeline.py LoadJSON > $LOGDIR/device_recall.log 2>&1
$PYTHON openfda/maude/pipeline.py LoadJSON > $LOGDIR/maude.log 2>&1
$PYTHON openfda/device_udi/pipeline.py LoadJSON > $LOGDIR/device_udi.log 2>&1
$PYTHON openfda/caers/pipeline.py LoadJSON > $LOGDIR/caers.log 2>&1
