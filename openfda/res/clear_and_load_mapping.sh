#!/bin/bash
#
# Sets up the Elasticsearch instance

# Drop index
curl -XDELETE 'http://localhost:9200/recall'
curl -XPOST 'http://localhost:9200/_cache/clear'

# Create index level settings
curl -XPOST 'http://localhost:9200/recall' \
--data-binary @"schemas/indexing.json"

# Add mapping
curl -s -XPOST localhost:9200/recall/enforcementreport/_mapping \
--data-binary @"schemas/res_mapping.json"

