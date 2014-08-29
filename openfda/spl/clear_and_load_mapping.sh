#!/bin/bash
#
# Sets up the Elasticsearch instance

# Drop index
curl -XDELETE 'http://localhost:9200/druglabel'
curl -XPOST 'http://localhost:9200/_cache/clear'

# Create index level settings
curl -XPOST 'http://localhost:9200/druglabel' \
--data-binary @"schemas/indexing.json"

# Add mapping
curl -s -XPOST localhost:9200/druglabel/spl/_mapping \
--data-binary @"schemas/spl_mapping.json"
