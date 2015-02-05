#!/bin/bash #
# Sets up the Elasticsearch instance

set -x
set -e

# Drop index
curl -XDELETE 'http://localhost:9200/drugevent' || true
curl -XDELETE 'http://localhost:9200/drugevent.base' || true
curl -XDELETE 'http://localhost:9200/drugevent.spill' || true
curl -XPOST 'http://localhost:9200/_cache/clear'

# Create index level settings
curl -XPOST 'http://localhost:9200/drugevent.base' --data-binary @"schemas/indexing.json"

# Add mapping
curl -s -XPOST localhost:9200/drugevent.base/safetyreport/_mapping --data-binary @"schemas/faers_mapping.json"

# Setup default alias
curl -XPOST 'http://localhost:9200/_aliases' -d '
{
  "actions" : [
     { "add" : { "index" : ["drugevent.base" ], "alias" : "drugevent.serving" } },
     { "add" : { "index" : ["drugevent.base" ], "alias" : "drugevent.staging" } }
  ]
}'

