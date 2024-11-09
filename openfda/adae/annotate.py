#!/usr/bin/python

import collections
import re

import simplejson as json
from openfda import parallel

STRIP_PUNCT_RE = re.compile("[^a-zA-Z -]+")


def normalize_product_name(product_name):
    "Simple drugname normalization: strip punctuation and whitespace and lowercase."
    product_name = product_name.strip().lower()
    return STRIP_PUNCT_RE.sub("", product_name)


def normalize_product_ndc(product_ndc):
    "Simple ndc normalization: strip letters, whitespace, and trim 10 digit ndcs."
    if any(i.isdigit() for i in product_ndc):
        product_ndc = re.sub("[^\d-]", "", product_ndc.strip())
        if len(product_ndc) > 9:
            product_ndc = product_ndc[:9]
    return product_ndc


def read_json_file(json_file):
    for line in json_file:
        yield json.loads(line)


def read_harmonized_file(harmonized_file):
    """
    Create a dictionary that is keyed by NDC package codes as well as drug names. First brand_name, then
    generic_name. Used to join against the animal event drug names.
    """
    return_dict = collections.defaultdict(list)
    for row in read_json_file(harmonized_file):
        if "animal" in row["product_type"].lower():

            drug = row["brand_name"].strip().lower() if "brand_name" in row else None
            ndc = row["product_ndc"].strip() if "product_ndc" in row else None

            if drug:
                return_dict[normalize_product_name(drug)].append(row)
            if ndc:
                return_dict[ndc].append(row)

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
            for key, value in row["unii_indexing"].items():
                if key == "unii":
                    _add_field(openfda, "unii", value)
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


def AnnotateDrugByKey(drug, key, harmonized_dict):
    if key in harmonized_dict:
        openfda = {}

        for row in harmonized_dict[key]:
            AddHarmonizedRowToOpenfda(openfda, row)

        openfda_lists = {}
        for field, value in openfda.items():
            openfda_lists[field] = [s for s in value.keys()]

        drug["openfda"] = openfda_lists


def AnnotateDrug(drug, harmonized_dict):
    if "product_ndc" in drug and drug["product_ndc"]:
        AnnotateDrugByKey(
            drug, normalize_product_ndc(drug["product_ndc"]), harmonized_dict
        )
    if "brand_name" in drug and drug["brand_name"]:
        AnnotateDrugByKey(
            drug, normalize_product_name(drug["brand_name"]), harmonized_dict
        )


def AnnotateEvent(event, harmonized_dict):
    if "drug" not in event:
        return

    for drug in event["drug"]:
        AnnotateDrug(drug, harmonized_dict)


class AnnotateMapper(parallel.Mapper):
    def __init__(self, harmonized_dict):
        self.harmonized_dict = harmonized_dict

    def map_shard(self, map_input, map_output):
        for id, event in map_input:
            AnnotateEvent(event, self.harmonized_dict)
            map_output.add(id, event)
