#!/bin/bash

curl -XPUT 'http://localhost:9200/deviceudi/_mapping/deviceudi' --data '@./../schemas/deviceudi_mapping.json'
curl -XPUT 'http://localhost:9200/deviceclearance/_mapping/device510k' --data '@./../schemas/clearance_mapping.json'
curl -XPUT 'http://localhost:9200/devicerecall/_mapping/recall' --data '@./../schemas/device_recall_mapping.json'
curl -XPUT 'http://localhost:9200/foodevent/_mapping/rareport' --data '@./../schemas/foodevent_mapping.json'
curl -XPUT 'http://localhost:9200/deviceevent/_mapping/maude' --data '@./../schemas/maude_mapping.json'
curl -XPUT 'http://localhost:9200/othernsde/_mapping/othernsde' --data '@./../schemas/othernsde_mapping.json'
curl -XPUT 'http://localhost:9200/ndc/_mapping/ndc' --data '@./../schemas/ndc_mapping.json'
curl -XPUT 'http://localhost:9200/devicepma/_mapping/pma' --data '@./../schemas/pma_mapping.json'
curl -XPUT 'http://localhost:9200/devicereglist/_mapping/registration' --data '@./../schemas/registration_mapping.json'
curl -XPUT 'http://localhost:9200/recall/_mapping/enforcementreport' --data '@./../schemas/res_mapping.json'
curl -XPUT 'http://localhost:9200/druglabel/_mapping/spl' --data '@./../schemas/spl_mapping.json'
curl -XPUT 'http://localhost:9200/drugevent/_mapping/safetyreport' --data '@./../schemas/faers_mapping.json'
curl -XPUT 'http://localhost:9200/deviceclass/_mapping/classification' --data '@./../schemas/classification_mapping.json'
curl -XPUT 'http://localhost:9200/animalandveterinarydrugevent/_mapping/animalandveterinarydrugevent' --data '@./../animalandveterinarydrugevent_mapping.json'
curl -XPUT 'http://localhost:9200/openfdadata/_mapping/downloads' --data '@./../schemas/downloads_mapping.json'

