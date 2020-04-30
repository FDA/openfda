#!/bin/bash
set -x

export PYTHON=./_python-env/bin/python
export DATA_DIR=./data/fields

mkdir -p $DATA_DIR

$PYTHON scripts/generate_fields_yaml.py generate \
--mapping ./schemas/faers_mapping.json \
--schema ./schemas/drugevent_schema.json \
--filename $DATA_DIR/drug.event.yaml

$PYTHON scripts/generate_fields_yaml.py generate \
--mapping ./schemas/spl_mapping.json \
--schema ./schemas/druglabel_schema.json \
--filename $DATA_DIR/drug.label.yaml

$PYTHON scripts/generate_fields_yaml.py generate \
--mapping ./schemas/res_mapping.json \
--schema ./schemas/recall_schema.json \
--filename $DATA_DIR/drug.enforcement.yaml

$PYTHON scripts/generate_fields_yaml.py generate \
--mapping ./schemas/res_mapping.json \
--schema ./schemas/recall_schema.json \
--filename $DATA_DIR/food.enforcement.yaml

$PYTHON scripts/generate_fields_yaml.py generate \
--mapping ./schemas/foodevent_mapping.json \
--schema ./schemas/foodevent_schema.json \
--filename $DATA_DIR/food.event.yaml

$PYTHON scripts/generate_fields_yaml.py generate \
--mapping ./schemas/res_mapping.json \
--schema ./schemas/recall_schema.json \
--filename $DATA_DIR/device.enforcement.yaml

$PYTHON scripts/generate_fields_yaml.py generate \
--mapping ./schemas/maude_mapping.json \
--schema ./schemas/deviceevent_schema.json \
--filename $DATA_DIR/device.event.yaml

$PYTHON scripts/generate_fields_yaml.py generate \
--mapping ./schemas/classification_mapping.json \
--schema ./schemas/deviceclass_schema.json \
--filename $DATA_DIR/device.classification.yaml

$PYTHON scripts/generate_fields_yaml.py generate \
--mapping ./schemas/clearance_mapping.json \
--schema ./schemas/deviceclearance_schema.json \
--filename $DATA_DIR/device.510k.yaml

$PYTHON scripts/generate_fields_yaml.py generate \
--mapping ./schemas/pma_mapping.json \
--schema ./schemas/devicepma_schema.json \
--filename $DATA_DIR/device.pma.yaml

$PYTHON scripts/generate_fields_yaml.py generate \
--mapping ./schemas/registration_mapping.json \
--schema ./schemas/devicereglist_schema.json \
--filename $DATA_DIR/device.registrationlisting.yaml

$PYTHON scripts/generate_fields_yaml.py generate \
--mapping ./schemas/device_recall_mapping.json \
--schema ./schemas/devicerecall_schema.json \
--filename $DATA_DIR/device.recall.yaml

$PYTHON scripts/generate_fields_yaml.py generate \
--mapping ./schemas/deviceudi_mapping.json \
--schema ./schemas/deviceudi_schema.json \
--filename $DATA_DIR/device.udi.yaml

$PYTHON scripts/generate_fields_yaml.py generate \
--mapping ./schemas/animaldrugevent_mapping.json \
--schema ./schemas/animaldrugevent_schema.json \
--filename $DATA_DIR/animal.drug.event.yaml

cd $DATA_DIR

zip _fields.zip device.*.yaml drug.*.yaml food.*.yaml
