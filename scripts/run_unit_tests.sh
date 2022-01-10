#!/bin/bash

set -x
set -e
source ./_python-env/bin/activate
es_host="${ES_HOST:-localhost:9200}"
BASEDIR=$(dirname "$0")
$BASEDIR/wait-for.sh $es_host -t 180

nose2 -c nose2_unit.cfg --junit-xml --with-coverage
