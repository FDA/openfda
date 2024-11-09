#!/usr/bin/python

import simplejson as json
from openfda import parallel


def read_json_file(json_file):
    """
    Reads an ElasticSearch style multi-json file:
      Each line contains a single JSON record.
      Yields one object for each line
    """
    for line in json_file:
        yield json.loads(line)


# TODO(hansnelsen): add to common library, is used in all annotation processing
def read_harmonized_file(harmonized_file):
    """
    Create a dictionary that is keyed by spl_id.
    """
    harmonization_dict = {}
    for row in read_json_file(harmonized_file):
        spl_id = row["id"]
        if spl_id in harmonization_dict:
            harmonization_dict[spl_id].append(row)
        else:
            harmonization_dict[spl_id] = [row]
    return harmonization_dict


# TODO(hansnelsen): Add to a common library, since it is used by faers and res
# annotate.py. _add_field() is identical in all files.
def _add_field(openfda, field, value):
    if type(value) != type([]):
        value = [value]
    for v in value:
        if not v:
            continue
        if field not in openfda:
            openfda[field] = {}
        openfda[field][v] = True
    return


# TODO(hansnelsen): Looks very similiar to the code in faers/annotate.py, we
# should consider refactoring this into a general piece of code for generating
# a de-duped list for each key in the openfda dict. Note: this openfda record
# has a few more keys than its faers counterpart
# TODO(hansnelsen): rename AddHarmonizedRowToOpenfda to
#                   add_harmonized_row_to_openfda so that it follows naming
#                   style of other function
def AddHarmonizedRowToOpenfda(openfda, row):

    _add_field(openfda, "manufacturer_name", row["manufacturer_name"])

    for rxnorm in row["rxnorm"]:
        _add_field(openfda, "rxcui", rxnorm["rxcui"])

    _add_field(openfda, "spl_set_id", row["spl_set_id"])
    _add_field(openfda, "is_original_packager", row["is_original_packager"])

    _add_field(openfda, "upc", row["upc"])

    if row["unii_indexing"] != []:
        unii_list = []
        for unii_row in row["unii_indexing"]:
            for key, value in unii_row.items():
                if key == "unii":
                    unii_list.append(value)
                if key == "va":
                    for this_item in value:
                        for va_key, va_value in this_item.items():
                            if va_key == "name":
                                if va_value.find("[MoA]") != -1:
                                    _add_field(openfda, "pharm_class_moa", va_value)
                                if (
                                    va_value.find("[Chemical/Ingredient]") != -1
                                    or va_value.find("[CS]") != -1
                                ):
                                    _add_field(openfda, "pharm_class_cs", va_value)
                                if va_value.find("[PE]") != -1:
                                    _add_field(openfda, "pharm_class_pe", va_value)
                                if va_value.find("[EPC]") != -1:
                                    _add_field(openfda, "pharm_class_epc", va_value)
                            if va_key == "number":
                                _add_field(openfda, "nui", va_value)
        _add_field(openfda, "unii", unii_list)


def annotate_drug(drug, harmonized_dict):
    openfda = {}
    spl_id = drug.get("spl_id")
    if spl_id != None:
        if spl_id in harmonized_dict:
            AddHarmonizedRowToOpenfda(openfda, harmonized_dict[spl_id][0])

    openfda_lists = {}
    for field, value in openfda.items():
        openfda_lists[field] = [s for s in value.keys()]
    drug["openfda"] = openfda_lists


class AnnotateMapper(parallel.Mapper):
    def __init__(self, harmonized_file):
        self.harmonized_file = harmonized_file

    def map_shard(self, map_input, map_output):
        self.harmonized_dict = read_harmonized_file(open(self.harmonized_file))
        parallel.Mapper.map_shard(self, map_input, map_output)

    def map(self, key, drug, out):
        annotate_drug(drug, self.harmonized_dict)
        out.add(key, drug)
