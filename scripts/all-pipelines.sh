#!/bin/bash

set -x

export LUIGI_CONFIG_PATH=./config/luigi.cfg
export PYTHON=./_python-env/bin/python
export LOGDIR=./logs

mkdir -p $LOGDIR

$PYTHON openfda/annotation_table/pipeline.py CombineHarmonization > $LOGDIR/annotation.log 2>&1
$PYTHON openfda/nsde/pipeline.py LoadJSON > $LOGDIR/nsde.log 2>&1
$PYTHON openfda/unii/pipeline.py LoadJSON > $LOGDIR/unii.log 2>&1
$PYTHON openfda/drugsfda/pipeline.py LoadJSON > $LOGDIR/drugsfda.log 2>&1
$PYTHON openfda/ndc/pipeline.py LoadJSON > $LOGDIR/ndc.log 2>&1
$PYTHON openfda/faers/pipeline.py LoadJSON --quarter=all > $LOGDIR/faers.log 2>&1
$PYTHON openfda/res/pipeline.py RunWeeklyProcess > $LOGDIR/res.log 2>&1
$PYTHON openfda/spl/pipeline.py LoadJSON > $LOGDIR/spl.log 2>&1
$PYTHON openfda/adae/pipeline.py LoadJSON > $LOGDIR/adae.log 2>&1
$PYTHON openfda/classification/pipeline.py LoadJSON > $LOGDIR/classification.log 2>&1
$PYTHON openfda/device_pma/pipeline.py LoadJSON > $LOGDIR/device_pma.log 2>&1
$PYTHON openfda/device_clearance/pipeline.py LoadJSON > $LOGDIR/device_clearance.log 2>&1
$PYTHON openfda/registration/pipeline.py LoadJSON > $LOGDIR/registration.log 2>&1
$PYTHON openfda/device_recall/pipeline.py LoadJSON > $LOGDIR/device_recall.log 2>&1
$PYTHON openfda/maude/pipeline.py LoadJSON > $LOGDIR/maude.log 2>&1
$PYTHON openfda/device_udi/pipeline.py LoadJSON > $LOGDIR/device_udi.log 2>&1
$PYTHON openfda/covid19serology/pipeline.py LoadJSON > $LOGDIR/serology.log 2>&1
$PYTHON openfda/caers/pipeline.py LoadJSON > $LOGDIR/caers.log 2>&1
$PYTHON openfda/substance_data/pipeline.py LoadJSON > $LOGDIR/substance.log 2>&1
$PYTHON openfda/tobacco_problem/pipeline.py LoadJSON > $LOGDIR/tobacco_problem.log 2>&1
$PYTHON openfda/downloadstats/pipeline.py LoadJSON > $LOGDIR/downloadstats.log 2>&1
