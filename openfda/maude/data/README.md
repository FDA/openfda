# MAUDE Data
## enums.csv
These data come from: http://www.fda.gov/MedicalDevices/DeviceRegulationandGuidance/PostmarketRequirements/ReportingAdverseEvents/ucm127891.htm
The file is constructed manually according to the values peppered throughout the
bottom half of the page. The format is:
`key_name, code, code_desc`

## wide_sample.json
This file is created manually and is used in generating the mapping schema
for the deviceevent/maude index. This file is required because the date fields
that are null are removed by the join_maude.py process (since Elasticsearch
cannot handle null dates), but the date fields must be there to generate the
mapping file.
