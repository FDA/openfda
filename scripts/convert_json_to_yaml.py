import json
import os
import yaml

print(
    os.path.dirname(os.path.dirname(os.getcwd()) + "\schemas\phishpharm_mapping.json")
)
with open(
    os.path.dirname(os.path.realpath(__file__)) + "\..\schemas\phishpharm_mapping.json"
) as f:
    print(yaml.safe_dump(json.load(f)))
