import json


def byteify(input):
    if isinstance(input, dict):
        return {byteify(key): byteify(value) for key, value in iter(input.items())}
    elif isinstance(input, list):
        return [byteify(element) for element in input]
    elif isinstance(input, str):
        return input.encode("utf-8")
    else:
        return input


schema_json = {"type": "object", "properties": {}}


def generate_fields(obj):
    if "properties" in obj:
        field_obj = {"type": "object"}
        prop_obj = {}
        for item, val in obj["properties"].items():
            if item.endswith("_exact"):
                pass
            else:
                prop_obj[item] = generate_fields(val)
        field_obj["properties"] = prop_obj
    elif "fields" in obj:
        exact = obj["fields"]["exact"]
        field_obj = {"type": exact["type"]}
        if "format" in exact:
            if "date" in exact["format"]:
                field_obj["format"] = "date"
                field_obj["type"] = "string"
            else:
                field_obj["format"] = exact["format"]
    else:
        field_obj = {"type": obj["type"]}
        if "format" in obj:
            if "date" in obj["format"]:
                field_obj["format"] = "date"
                field_obj["type"] = "string"
            else:
                field_obj["format"] = obj["format"]
    return field_obj


with open("../schemas/substancedata_mapping.json") as json_file:
    map_file = byteify(json.load(json_file))

    for field, value in map_file["substancedata"]["properties"].items():
        if field.endswith("_exact"):
            pass
        else:
            schema_json["properties"][field] = generate_fields(value)


with open("../schemas/substancedata_schema.json", "w") as f:
    json.dump(schema_json, f, indent=2, sort_keys=True)
