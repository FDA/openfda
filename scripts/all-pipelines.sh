#!/bin/bash

set -x

export LUIGI_CONFIG_PATH=./config/luigi.cfg
export PYTHON=./_python-env/bin/python

$PYTHON openfda/faers/pipeline.py LoadJSON --quarter=all
$PYTHON openfda/classification/pipeline.py LoadJSON
$PYTHON openfda/res/pipeline.py RunWeeklyProcess
$PYTHON openfda/annotation_table/pipeline.py CombineHarmonization
$PYTHON openfda/maude/pipeline.py LoadJSON
$PYTHON openfda/device_recall/pipeline.py LoadJSON
$PYTHON openfda/device_pma/pipeline.py LoadJSON
$PYTHON openfda/device_clearance/pipeline.py LoadJSON
$PYTHON openfda/registration/pipeline.py LoadJSON
$PYTHON openfda/spl/pipeline.py ProcessBatch
