#!/bin/bash

set -x
source ./_python-env/bin/activate

export LUIGI_CONFIG_PATH=./config/luigi.cfg
export PYTHON=./_python-env/bin/python
export LOGDIR=./logs
export PYTHONUNBUFFERED=true

mkdir -p $LOGDIR

es_host="${ES_HOST:-localhost:9200}"
BASEDIR=$(dirname "$0")
$BASEDIR/wait-for.sh $es_host -t 120

$PYTHON openfda/nsde/pipeline.py LoadJSON --FDAConfig-es-host=$es_host
$PYTHON openfda/caers/pipeline.py LoadJSON --FDAConfig-es-host=$es_host
$PYTHON openfda/substance_data/pipeline.py LoadJSON --FDAConfig-es-host=$es_host
$PYTHON openfda/device_clearance/pipeline.py LoadJSON --FDAConfig-es-host=$es_host
$PYTHON openfda/maude/pipeline.py LoadJSON --FDAConfig-es-host=$es_host

#$PYTHON openfda/classification/pipeline.py LoadJSON --FDAConfig-es-host=$es_host
#$PYTHON openfda/device_pma/pipeline.py LoadJSON --FDAConfig-es-host=$es_host
#$PYTHON openfda/registration/pipeline.py LoadJSON --FDAConfig-es-host=$es_host
#$PYTHON openfda/annotation_table/pipeline.py CombineHarmonization --FDAConfig-es-host=$es_host > $LOGDIR/annotation.log 2>&1
#$PYTHON openfda/ndc/pipeline.py LoadJSON --FDAConfig-es-host=$es_host > $LOGDIR/ndc.log 2>&1
#$PYTHON openfda/faers/pipeline.py LoadJSON --FDAConfig-es-host=$es_host --quarter=all > $LOGDIR/faers.log 2>&1
#$PYTHON openfda/res/pipeline.py RunWeeklyProcess --FDAConfig-es-host=$es_host > $LOGDIR/res.log 2>&1
#$PYTHON openfda/spl/pipeline.py LoadJSON --FDAConfig-es-host=$es_host > $LOGDIR/spl.log 2>&1
#$PYTHON openfda/adae/pipeline.py LoadJSON --FDAConfig-es-host=$es_host > $LOGDIR/adae.log 2>&1
#$PYTHON openfda/device_recall/pipeline.py LoadJSON --FDAConfig-es-host=$es_host > $LOGDIR/device_recall.log 2>&1
#$PYTHON openfda/maude/pipeline.py LoadJSON --FDAConfig-es-host=$es_host > $LOGDIR/maude.log 2>&1
#$PYTHON openfda/device_udi/pipeline.py LoadJSON --FDAConfig-es-host=$es_host > $LOGDIR/device_udi.log 2>&1
#$PYTHON openfda/covid19serology/pipeline.py LoadJSON --FDAConfig-es-host=$es_host > $LOGDIR/serology.log 2>&1
