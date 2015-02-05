#!/bin/bash #
# Sets up the Elasticsearch instance

set -x
set -e

# Drop index
curl -XDELETE 'http://localhost:9200/druglabel' || true
curl -XDELETE 'http://localhost:9200/druglabel.base' || true
curl -XDELETE 'http://localhost:9200/druglabel.spill' || true
curl -XPOST 'http://localhost:9200/_cache/clear'

# Create index level settings
curl -XPOST 'http://localhost:9200/druglabel.base' --data-binary @"schemas/indexing.json"

# Add mapping
curl -s -XPOST localhost:9200/druglabel.base/spl/_mapping --data-binary @"schemas/spl_mapping.json"

# Setup default alias
curl -XPOST 'http://localhost:9200/_aliases' -d '
{
  "actions" : [
     { "add" : { "index" : ["druglabel.base" ], "alias" : "druglabel.serving" } },
     { "add" : { "index" : ["druglabel.base" ], "alias" : "druglabel.staging" } }
  ]
}'
