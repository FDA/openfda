#!/usr/bin/python

import collections

import simplejson as json

from openfda import parallel


def read_json_file(json_file):
    for line in json_file:
        yield json.loads(line)


def read_harmonized_file(harmonized_file):
    """
    Create a dictionary that is keyed by NDA/ANDA application numbers.  Used to join
    against the application_number value.
    """
    return_dict = collections.defaultdict(list)
    for row in read_json_file(harmonized_file):
        return_dict[row["application_number"].strip().upper()].append(row)
    return return_dict


def _add_field(openfda, field, value):
    if not isinstance(value, list):
        value = [value]

    for v in value:
        if not v:
            continue
        if field not in openfda:
            openfda[field] = {}
        openfda[field][v] = True
    return


def AddHarmonizedRowToOpenfda(openfda, row):
    if not row["is_original_packager"]:
        return

    if row["product_ndc"] in row["spl_product_ndc"]:
        _add_field(openfda, "application_number", row["application_number"])
        # Using the most precise possible name for brand_name
        if row["brand_name_suffix"]:
            brand_name = row["brand_name"] + " " + row["brand_name_suffix"]
        else:
            brand_name = row["brand_name"]

        _add_field(openfda, "brand_name", brand_name.rstrip().upper())
        _add_field(openfda, "generic_name", row["generic_name"].upper())
        _add_field(openfda, "manufacturer_name", row["manufacturer_name"])
        _add_field(openfda, "product_ndc", row["product_ndc"])
        _add_field(openfda, "product_ndc", row["spl_product_ndc"])
        _add_field(openfda, "product_type", row["product_type"])

        for route in row["route"].split(";"):
            route = route.strip()
            _add_field(openfda, "route", route)

        for substance in row["substance_name"].split(";"):
            substance = substance.strip()
            _add_field(openfda, "substance_name", substance)

        for rxnorm in row["rxnorm"]:
            _add_field(openfda, "rxcui", rxnorm["rxcui"])

        _add_field(openfda, "spl_id", row["id"])
        _add_field(openfda, "spl_set_id", row["spl_set_id"])
        _add_field(openfda, "package_ndc", row["package_ndc"])

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
    application_number = drug["application_number"]

    if application_number in harmonized_dict:
        openfda = {}

        for row in harmonized_dict[application_number]:
            AddHarmonizedRowToOpenfda(openfda, row)

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
