#!/bin/bash
# Sets up the Elasticsearch instance

set -x
set -e

# Drop index
curl -XDELETE 'http://localhost:9200/recall' || true
curl -XDELETE 'http://localhost:9200/recall.base' || true
curl -XDELETE 'http://localhost:9200/recall.spill' || true
curl -XPOST 'http://localhost:9200/_cache/clear'

# Create index level settings
curl -XPOST 'http://localhost:9200/recall.base' --data-binary @"schemas/indexing.json"

# Add mapping
curl -s -XPOST localhost:9200/recall.base/enforcementreport/_mapping --data-binary @"schemas/res_mapping.json"

# Setup default alias
curl -XPOST 'http://localhost:9200/_aliases' -d '
{
  "actions" : [
     { "add" : { "index" : ["recall.base" ], "alias" : "recall.serving" } },
     { "add" : { "index" : ["recall.base" ], "alias" : "recall.staging" } }
  ]
}'

