# Elasticsearch Schemas and JSON Schemas
There are two types of schemas stored in this directory:
  - **Elasticsearch mapping files**: These are used to create the mapping in the Elasticsearch instance. Each pipeline will create the mapping if it does not already exist. It does this by reading in the appropriate `_mapping.json` file.
  - **JSON Schema files**: These serve as a reference to the API output. These files get copied to the download location so that a developer consuming the downloads can understand the structure and type of each API result. These follow the naming convention of `_schema.json`.
  
**Please note**: both of these files are used as input to `python scripts/generate_fields_yaml.py`, which is a utility that generates a skeleton yaml file (requires further manual editing) that is used to power the reference pages on the open.fda.gov website.


