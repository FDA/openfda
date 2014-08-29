#!/bin/bash
#
# Sets up the Elasticsearch instance

# Drop index
curl -XDELETE 'http://localhost:9200/deviceevent'
curl -XPOST 'http://localhost:9200/_cache/clear'

# Create index level settings
curl -XPOST 'http://localhost:9200/deviceevent' \
--data-binary @"schemas/indexing.json"

# Add mapping
curl -s -XPOST localhost:9200/deviceevent/maude/_mapping \
--data-binary @"schemas/maude_mapping.json"

