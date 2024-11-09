#!/usr/bin/python

""" Animal Drug Adverse Event pipeline.
"""

import logging
import os
import re
import sys
import traceback

import arrow
import luigi
import lxml
from lxml import etree

from openfda import common, config, index_util, parallel
from openfda.adae import annotate
from openfda.annotation_table.pipeline import CombineHarmonization
from openfda.common import newest_file_timestamp
from openfda.parallel import NullOutput

ADAE_BUCKET = "s3://openfda-data-adae"
ADAE_LOCAL_DIR = config.data_dir("adae/s3_sync")
BATCH = arrow.utcnow().floor("day").format("YYYYMMDD")
AWS_CLI = "aws"

# TODO: move to an external file once the list grows unmanageable.
NULLIFIED = [
    "US-FDACVM-2018-US-045311.xml",
    "US-FDACVM-2018-US-048571.xml",
    "US-FDACVM-2018-US-046672.xml",
    "US-FDACVM-2017-US-042492.xml",
    "US-FDACVM-2018-US-044065.xml",
    "US-FDACVM-2017-US-070108.xml",
    "US-FDACVM-2017-US-002864.xml",
    "US-FDACVM-2017-US-002866.xml",
    "US-FDACVM-2017-US-052458.xml",
    "US-FDACVM-2017-US-055193.xml",
    "US-FDACVM-2017-US-043931.xml",
    "US-FDACVM-2018-US-002321.xml",
    "US-FDACVM-2018-US-063536.xml",
    "US-FDACVM-2015-US-221044.xml",
    "US-FDACVM2019-US-016263.xml",
    "US-FDACVM-2016-US-062923.xml",
    "US-FDACVM-2017-US-001483.xml",
    "US-FDACVM-2017-US-009155.xml",
    "US-FDACVM-2017-US-028125.xml",
    "US-FDACVM-2017-US-033030.xml",
    "US-FDACVM-2019-US-007584.xml",
    "US-FDACVM-2019-US-000655.xml",
    "US-FDACVM-2019-US-009469.xml",
    "US-FDACVM-2019-US-021451.xml",
    "US-FDACVM-2012-US-036039.xml",
    "US-FDACVM-2019-US-021383.xml",
    "US-FDACVM-2019-US-036949.xml",
    "US-FDACVM-2019-US-056874.xml",
    "US-FDACVM-2020-US-000506.xml",
    "US-FDACVM-2020-US-012144.xml",
    "US-FDACVM-2019-US-008020.xml",
    "US-FDACVM-2020-US-055134.xml",
    "US-FDACVM-2020-US-042720.xml",
    "US-FDACVM-2020-US-045814.xml",
    "US-FDACVM-2020-US-045816.xml",
    "US-FDACVM-2020-US-045820.xml",
    "US-FDACVM-2020-US-045824.xml",
    "US-FDACVM-2016-US-054799.xml",
    "US-FDACVM-2017-US-060740.xml",
    "US-FDACVM-2016-US-062923.xml",
    "US-FDACVM-2017-US-001483.xml",
    "US-FDACVM-2017-US-009155.xml",
    "US-FDACVM-2017-US-028125.xml",
    "US-FDACVM-2017-US-033030.xml",
    "US-FDACVM-2012-US-031040.xml",
    "US-FDACVM-2012-US-031016.xml",
    "US-FDACVM-2014-US-011762.xml",
    "US-FDACVM-2012-US-045510.xml",
    "US-FDACVM-2012-US-031080.xml",
    "US-FDACVM-2012-US-030987.xml",
    "US-FDACVM-2012-US-031012.xml",
    "US-FDACVM-2018-US-026106.xml",
    "US-FDACVM-2019-US-063566.xml",
    "US-FDACVM-2020-US-035313.xml",
    "US-FDACVM-2021-US-024914.xml",
]


class SyncS3(luigi.Task):
    bucket = ADAE_BUCKET
    local_dir = ADAE_LOCAL_DIR
    aws = AWS_CLI

    def output(self):
        return luigi.LocalTarget(ADAE_LOCAL_DIR)

    def run(self):
        common.quiet_cmd(
            [
                self.aws,
                "--profile=" + config.aws_profile(),
                "s3",
                "sync",
                self.bucket,
                self.local_dir,
            ]
        )


class ExtractXML(parallel.MRTask):
    local_dir = ADAE_LOCAL_DIR

    def requires(self):
        return SyncS3()

    def output(self):
        return luigi.LocalTarget(os.path.join(config.data_dir("adae/extracted"), BATCH))

    def mapreduce_inputs(self):
        return parallel.Collection.from_glob(os.path.join(self.local_dir, "*.zip"))

    def map(self, zip_file, value, output):
        output_dir = self.output().path
        common.shell_cmd_quiet("mkdir -p %s", output_dir)
        common.shell_cmd_quiet('7z x "%s" -aoa -bd -y -o%s', zip_file, output_dir)

    def output_format(self):
        return NullOutput()


class XML2JSONMapper(parallel.Mapper):
    UNIT_OF_MEASUREMENTS_MAP = {
        "a": "Year",
        "s": "Second",
        "min": "Minute",
        "h": "Hour",
        "d": "Day",
        "wk": "Week",
        "mo": "Month",
        "g": "Gram",
        "mg": "Milligram",
        "ug": "Microgram",
        "ng": "Nanogram",
        "kg": "Kilogram",
    }

    VALUE_DECODE_MAP = {
        "unit": UNIT_OF_MEASUREMENTS_MAP,
        "numerator_unit": UNIT_OF_MEASUREMENTS_MAP,
        "denominator_unit": UNIT_OF_MEASUREMENTS_MAP,
    }

    NS = {"ns": "urn:hl7-org:v3"}

    DATE_FIELDS = [
        "original_receive_date",
        "onset_date",
        "first_exposure_date",
        "last_exposure_date",
        "manufacturing_date",
        "lot_expiration",
    ]

    def map_shard(self, map_input, map_output):

        XPATH_MAP = {
            "//ns:investigationEvent/ns:id[@root='1.2.3.4']/@extension": [
                "@id",
                "unique_aer_id_number",
            ],
            "//ns:attentionLine/ns:keyWordText[text()='Report Identifier']/following-sibling::ns:value": [
                "report_id"
            ],
            "//ns:outboundRelationship/ns:priorityNumber[@value='1']/following-sibling::ns:relatedInvestigation/ns:effectiveTime/@value": [
                "original_receive_date"
            ],
            "//ns:investigationEvent//ns:primaryInformationRecipient/ns:assignedEntity/ns:representedOrganization/ns:name": [
                "receiver.organization"
            ],
            "//ns:investigationEvent//ns:primaryInformationRecipient/ns:assignedEntity/ns:addr/ns:streetAddressLine": [
                "receiver.street_address"
            ],
            "//ns:investigationEvent//ns:primaryInformationRecipient/ns:assignedEntity/ns:addr/ns:city": [
                "receiver.city"
            ],
            "//ns:investigationEvent//ns:primaryInformationRecipient/ns:assignedEntity/ns:addr/ns:state": [
                "receiver.state"
            ],
            "//ns:investigationEvent//ns:primaryInformationRecipient/ns:assignedEntity/ns:addr/ns:postalCode": [
                "receiver.postal_code"
            ],
            "//ns:investigationEvent//ns:primaryInformationRecipient/ns:assignedEntity/ns:addr/ns:country": [
                "receiver.country"
            ],
            "//ns:outboundRelationship/ns:priorityNumber[@value='1']/following-sibling::ns:relatedInvestigation//ns:code[@codeSystem='2.16.840.1.113883.13.194']/@displayName": [
                "primary_reporter"
            ],
            "//ns:outboundRelationship/ns:priorityNumber[@value='2']/following-sibling::ns:relatedInvestigation//ns:code[@codeSystem='2.16.840.1.113883.13.194']/@displayName": [
                "secondary_reporter"
            ],
            "//ns:investigationCharacteristic/ns:code[@code='T95004']/following-sibling::ns:value/@displayName": [
                "type_of_information"
            ],
            "//ns:investigationCharacteristic/ns:code[@code='T95022']/following-sibling::ns:value/@value": [
                "serious_ae"
            ],
            "//ns:adverseEventAssessment/ns:subject1[@typeCode='SBJ']/ns:primaryRole[@classCode='INVSBJ']/ns:player2[@classCode='ANM']/ns:quantity/@value": [
                "number_of_animals_treated"
            ],
            "//ns:adverseEventAssessment/ns:subject1[@typeCode='SBJ']/ns:primaryRole[@classCode='INVSBJ']/ns:subjectOf2//ns:code[@code='T95005']/following-sibling::ns:value/@value": [
                "number_of_animals_affected"
            ],
            "//ns:adverseEventAssessment/ns:subject1[@typeCode='SBJ']/ns:primaryRole[@classCode='INVSBJ']/ns:player2[@classCode='ANM']/ns:code[@codeSystem='2.16.840.1.113883.4.341']/@displayName": [
                "animal.species"
            ],
            "//ns:adverseEventAssessment/ns:subject1[@typeCode='SBJ']/ns:primaryRole[@classCode='INVSBJ']/ns:player2[@classCode='ANM']/ns:administrativeGenderCode/@displayName": [
                "animal.gender"
            ],
            "//ns:adverseEventAssessment/ns:subject1[@typeCode='SBJ']/ns:primaryRole[@classCode='INVSBJ']/ns:player2[@classCode='ANM']/ns:genderStatusCode/@displayName": [
                "animal.reproductive_status"
            ],
            "//ns:adverseEventAssessment/ns:subject1[@typeCode='SBJ']/ns:primaryRole[@classCode='INVSBJ']/ns:subjectOf2[@typeCode='SBJ']//ns:code[@code='T95010']/following-sibling::ns:value/@displayName": [
                "animal.female_animal_physiological_status"
            ],
            "//ns:adverseEventAssessment/ns:subject1[@typeCode='SBJ']/ns:primaryRole[@classCode='INVSBJ']/ns:subjectOf2[@typeCode='SBJ']//ns:code[@code='T95012']/following-sibling::ns:value/ns:low/@value": [
                "animal.age.min"
            ],
            "//ns:adverseEventAssessment/ns:subject1[@typeCode='SBJ']/ns:primaryRole[@classCode='INVSBJ']/ns:subjectOf2[@typeCode='SBJ']//ns:code[@code='T95012']/following-sibling::ns:value/ns:high/@value": [
                "animal.age.max"
            ],
            "//ns:adverseEventAssessment/ns:subject1[@typeCode='SBJ']/ns:primaryRole[@classCode='INVSBJ']/ns:subjectOf2[@typeCode='SBJ']//ns:code[@code='T95012']/following-sibling::ns:value/ns:low/@unit": [
                "animal.age.unit"
            ],
            "//ns:adverseEventAssessment/ns:subject1[@typeCode='SBJ']/ns:primaryRole[@classCode='INVSBJ']/ns:subjectOf2[@typeCode='SBJ']//ns:code[@code='T95012']/following-sibling::ns:value/following-sibling::ns:methodCode[@codeSystem='2.16.840.1.113883.13.200']/@displayName": [
                "animal.age.qualifier"
            ],
            "//ns:adverseEventAssessment/ns:subject1[@typeCode='SBJ']/ns:primaryRole[@classCode='INVSBJ']/ns:subjectOf2[@typeCode='SBJ']//ns:code[@code='T95011']/following-sibling::ns:value/ns:low/@value": [
                "animal.weight.min"
            ],
            "//ns:adverseEventAssessment/ns:subject1[@typeCode='SBJ']/ns:primaryRole[@classCode='INVSBJ']/ns:subjectOf2[@typeCode='SBJ']//ns:code[@code='T95011']/following-sibling::ns:value/ns:high/@value": [
                "animal.weight.max"
            ],
            "//ns:adverseEventAssessment/ns:subject1[@typeCode='SBJ']/ns:primaryRole[@classCode='INVSBJ']/ns:subjectOf2[@typeCode='SBJ']//ns:code[@code='T95011']/following-sibling::ns:value/ns:low/@unit": [
                "animal.weight.unit"
            ],
            "//ns:adverseEventAssessment/ns:subject1[@typeCode='SBJ']/ns:primaryRole[@classCode='INVSBJ']/ns:subjectOf2[@typeCode='SBJ']//ns:code[@code='T95011']/following-sibling::ns:value/following-sibling::ns:methodCode[@codeSystem='2.16.840.1.113883.13.200']/@displayName": [
                "animal.weight.qualifier"
            ],
            "//ns:adverseEventAssessment/ns:subject1[@typeCode='SBJ']/ns:primaryRole[@classCode='INVSBJ']/ns:subjectOf2[@typeCode='SBJ']//ns:code[@code='T95007']/following-sibling::ns:value/@value": [
                "animal.breed.is_crossbred"
            ],
            "//ns:adverseEventAssessment/ns:subject1[@typeCode='SBJ']/ns:primaryRole[@classCode='INVSBJ']/ns:subjectOf2[@typeCode='SBJ']//ns:code[@code='T95008']/following-sibling::ns:value[@codeSystem='2.16.840.1.113883.4.342']/@displayName": [
                "animal.breed.breed_component"
            ],
            "//ns:adverseEventAssessment/ns:subject1[@typeCode='SBJ']/ns:primaryRole[@classCode='INVSBJ']/ns:subjectOf2[@typeCode='SBJ']//ns:code[@code='C53279' or @code='C82467' or @code='C49495' or @code='C28554' or @code='C21115' or @code='C17998']/following-sibling::ns:value[@value>0]/..": self.outcome,
            "//ns:adverseEventAssessment/ns:subject1[@typeCode='SBJ']/ns:primaryRole[@classCode='INVSBJ']/ns:subjectOf2[@typeCode='SBJ']//ns:code[@code='T95006']/following-sibling::ns:value/@displayName": [
                "health_assessment_prior_to_exposure.condition"
            ],
            "//ns:adverseEventAssessment/ns:subject1[@typeCode='SBJ']/ns:primaryRole[@classCode='INVSBJ']/ns:subjectOf2[@typeCode='SBJ']//ns:code[@code='T95006']/following-sibling::ns:author//ns:code/@displayName": [
                "health_assessment_prior_to_exposure.assessed_by"
            ],
            "//ns:adverseEventAssessment//ns:subjectOf2[@typeCode='SBJ']/ns:substanceAdministration": self.drug,
            "//ns:adverseEventAssessment//ns:subjectOf2[@typeCode='SBJ']/ns:observation/ns:code[@code='T95020']/following-sibling::ns:effectiveTime/ns:low/@value": [
                "onset_date"
            ],
            "//ns:adverseEventAssessment//ns:subjectOf2[@typeCode='SBJ']/ns:observation/ns:code[@code='T95020']/following-sibling::ns:effectiveTime/ns:width/@value": [
                "duration.value"
            ],
            "//ns:adverseEventAssessment//ns:subjectOf2[@typeCode='SBJ']/ns:observation/ns:code[@code='T95020']/following-sibling::ns:effectiveTime/ns:width/@unit": [
                "duration.unit"
            ],
            "//ns:adverseEventAssessment//ns:subjectOf2[@typeCode='SBJ']/ns:observation/ns:code[@code='T95020']/following-sibling::ns:value[@codeSystem='2.16.840.1.113883.4.358' or @codeSystem='2.16.840.1.113883.13.226' or @codeSystem='2.16.840.1.113883.13.227']/..": self.reaction,
            "//ns:adverseEventAssessment//ns:subjectOf2[@typeCode='SBJ']/ns:observation/ns:code[@code='T95021']/following-sibling::ns:value/@displayName": [
                "time_between_exposure_and_onset"
            ],
            "//ns:adverseEventAssessment//ns:subjectOf2[@typeCode='SBJ']/ns:observation/ns:code[@code='T95023']/following-sibling::ns:value/@value": [
                "treated_for_ae"
            ],
        }

        ae = {}
        try:
            if os.path.getsize(map_input.filename) > 0:
                tree = etree.parse(open(map_input.filename))
                self.process_xpath_map(XPATH_MAP, tree, ae)
                map_output.add(ae["unique_aer_id_number"], ae)
            else:
                logging.warning("Zero length input file: " + map_input.filename)
        except lxml.etree.XMLSyntaxError as err:
            logging.warning(
                "Malformed input file: "
                + map_input.filename
                + ". Error was "
                + str(err)
            )
        except Exception:
            traceback.print_exc()
            logging.error(sys.exc_info()[0])
            raise

    def process_xpath_map(self, xpath_map, root_node, json):
        for xpath, json_fields in iter(xpath_map.items()):
            nodeset = root_node.xpath(xpath, namespaces=self.NS)
            if type(nodeset) == list and len(nodeset) > 0:
                self.set_value_in_json(nodeset, json_fields, json)

    def set_value_in_json(self, nodeset, json_fields, json):

        if callable(json_fields):
            json_fields(nodeset, json)
            return

        val = []
        for el in nodeset:
            if type(el) == etree._Element:
                val.append(el.text)
            else:
                val.append(str(el))

        val = val if len(val) > 1 else val[0]

        for field in json_fields:
            if type(field) == str:
                self.set_field_dot_notation(val, field, json)

    def set_field_dot_notation(self, val, field, json):
        # Decode value if needed
        if field in self.VALUE_DECODE_MAP:
            if val in self.VALUE_DECODE_MAP[field]:
                val = self.VALUE_DECODE_MAP[field][val]

        parts = field.split(".")
        if len(parts) > 1:
            first = parts[0]
            if json.get(first) == None:
                json[first] = {}
            self.set_field_dot_notation(val, ".".join(parts[1:]), json[first])
        elif val is not None:
            if not (field in self.DATE_FIELDS and not val):
                json[field] = val

    def isdigit(self, s):
        try:
            float(s)
            return True
        except ValueError:
            return False

    def drug(self, nodeset, json):

        XPATH_MAP = {
            "./ns:effectiveTime/ns:comp/ns:low/@value": ["first_exposure_date"],
            "./ns:effectiveTime/ns:comp/ns:high/@value": ["last_exposure_date"],
            "./ns:effectiveTime/ns:comp/ns:period/@value": [
                "frequency_of_administration.value"
            ],
            "./ns:effectiveTime/ns:comp/ns:period/@unit": [
                "frequency_of_administration.unit"
            ],
            "./ns:performer/ns:assignedEntity/ns:code/@displayName": [
                "administered_by"
            ],
            "./ns:routeCode/@displayName": ["route"],
            "./ns:doseCheckQuantity/ns:numerator/@value": ["dose.numerator"],
            "./ns:doseCheckQuantity/ns:numerator/ns:translation/@displayName": [
                "dose.numerator_unit"
            ],
            "./ns:doseCheckQuantity/ns:denominator/@value": ["dose.denominator"],
            "./ns:doseCheckQuantity/ns:denominator/ns:translation/@displayName": [
                "dose.denominator_unit"
            ],
            ".//ns:code[@code='T95015']/following-sibling::ns:value/@value": [
                "used_according_to_label"
            ],
            ".//ns:code[@code='T95016']/following-sibling::ns:outboundRelationship2//ns:value[@value='true']/preceding-sibling::ns:code/@displayName": [
                "off_label_use"
            ],
            ".//ns:code[@code='T95024']/following-sibling::ns:value/@value": [
                "previous_exposure_to_drug"
            ],
            ".//ns:code[@code='T95025']/following-sibling::ns:value/@value": [
                "previous_ae_to_drug"
            ],
            ".//ns:code[@code='T95026']/following-sibling::ns:value/@value": [
                "ae_abated_after_stopping_drug"
            ],
            ".//ns:code[@code='T95027']/following-sibling::ns:value/@value": [
                "ae_reappeared_after_resuming_drug"
            ],
            "./ns:consumable/ns:instanceOfKind/ns:productInstanceInstance/ns:existenceTime/ns:low/@value": [
                "manufacturing_date"
            ],
            "./ns:consumable/ns:instanceOfKind/ns:productInstanceInstance/ns:lotNumberText": [
                "lot_number"
            ],
            "./ns:consumable/ns:instanceOfKind/ns:productInstanceInstance/ns:lotNumberText/following-sibling::ns:expirationTime/@value": [
                "lot_expiration"
            ],
            "./ns:consumable/ns:instanceOfKind/ns:kindOfProduct/ns:code/@code": [
                "product_ndc"
            ],
            "./ns:consumable/ns:instanceOfKind/ns:kindOfProduct/ns:code/following-sibling::ns:name": [
                "brand_name"
            ],
            "./ns:consumable/ns:instanceOfKind/ns:kindOfProduct/ns:code/following-sibling::ns:formCode/@displayName": [
                "dosage_form"
            ],
            "./ns:consumable/ns:instanceOfKind/ns:kindOfProduct/ns:asManufacturedProduct/ns:manufacturerOrganization/ns:name": [
                "manufacturer.name"
            ],
            "./ns:consumable/ns:instanceOfKind/ns:kindOfProduct/ns:asManufacturedProduct/ns:subjectOf/ns:approval/ns:id/@extension": [
                "manufacturer.registration_number"
            ],
            "./ns:consumable/ns:instanceOfKind/ns:kindOfProduct/ns:instanceOfKind//ns:code[@code='T95017']/following-sibling::ns:value/@value": [
                "number_of_defective_items"
            ],
            "./ns:consumable/ns:instanceOfKind/ns:kindOfProduct/ns:instanceOfKind//ns:code[@code='T95018']/following-sibling::ns:value/@value": [
                "number_of_items_returned"
            ],
            "./ns:consumable/ns:instanceOfKind/ns:kindOfProduct/ns:asSpecializedKind//ns:code[@code='T95013']/following-sibling::ns:name": [
                "atc_vet_code"
            ],
            "./ns:consumable/ns:instanceOfKind/ns:kindOfProduct/ns:ingredient": self.ingredient,
        }

        json["drug"] = []
        for node in nodeset:
            drug = {}
            self.process_xpath_map(XPATH_MAP, node, drug)
            # Convert yyyymm to yyyy-mm that ES can understand.
            if drug.get("lot_expiration") is not None:
                drug["lot_expiration"] = re.sub(
                    r"^(\d{4})(\d{2})$", r"\1-\2", drug["lot_expiration"]
                )

            # A corner-case scenario (US-FDACVM-2014-US-065247.xml)
            if (
                drug.get("dose") is not None
                and drug["dose"].get("denominator") is not None
                and not self.isdigit(drug["dose"]["denominator"])
            ):
                logging.warning(
                    "Non-numeric denominator: " + drug["dose"]["denominator"]
                )
                drug["dose"]["denominator"] = "0"

            json["drug"].append(drug)

    def ingredient(self, nodeset, json):

        XPATH_MAP = {
            "./ns:ingredientSubstance/ns:name": ["name"],
            "./ns:quantity/ns:numerator/@value": ["dose.numerator"],
            "./ns:quantity/ns:numerator/ns:translation/@displayName": [
                "dose.numerator_unit"
            ],
            "./ns:quantity/ns:denominator/@value": ["dose.denominator"],
            "./ns:quantity/ns:denominator/ns:translation/@displayName": [
                "dose.denominator_unit"
            ],
        }

        json["active_ingredients"] = []
        for node in nodeset:
            ingredient = {}
            self.process_xpath_map(XPATH_MAP, node, ingredient)
            if "name" in list(ingredient.keys()):
                ingredient["name"] = ingredient["name"].title()
            json["active_ingredients"].append(ingredient)

    def outcome(self, nodeset, json):

        XPATH_MAP = {
            "./ns:code/@displayName": ["medical_status"],
            "./ns:value/@value": ["number_of_animals_affected"],
        }

        json["outcome"] = []
        for node in nodeset:
            outcome = {}
            self.process_xpath_map(XPATH_MAP, node, outcome)
            json["outcome"].append(outcome)

    def reaction(self, nodeset, json):

        XPATH_MAP = {
            "./ns:value[@codeSystem='2.16.840.1.113883.4.358' or @codeSystem='2.16.840.1.113883.13.226' or @codeSystem='2.16.840.1.113883.13.227']/@codeSystemVersion": [
                "veddra_version"
            ],
            "./ns:value[@codeSystem='2.16.840.1.113883.4.358' or @codeSystem='2.16.840.1.113883.13.226' or @codeSystem='2.16.840.1.113883.13.227']/@code": [
                "veddra_term_code"
            ],
            "./ns:value[@codeSystem='2.16.840.1.113883.4.358' or @codeSystem='2.16.840.1.113883.13.226' or @codeSystem='2.16.840.1.113883.13.227']/@displayName": [
                "veddra_term_name"
            ],
            "./ns:referenceRange/ns:observationRange/ns:value/@value": [
                "number_of_animals_affected"
            ],
            "./ns:referenceRange/ns:observationRange/ns:interpretationCode/@displayName": [
                "accuracy"
            ],
        }

        json["reaction"] = []
        for node in nodeset:
            reaction = {}
            self.process_xpath_map(XPATH_MAP, node, reaction)
            json["reaction"].append(reaction)


class XML2JSON(luigi.Task):
    def requires(self):
        return ExtractXML()

    def output(self):
        return luigi.LocalTarget(os.path.join(config.data_dir("adae/json.db"), BATCH))

    def run(self):
        input_shards = []
        input_dir = self.input().path
        for subdir, dirs, files in os.walk(input_dir):
            for file in files:
                if file.endswith(".xml"):
                    if file not in NULLIFIED:
                        input_shards.append(os.path.join(subdir, file))
                    else:
                        logging.info("Skipping a nullified case: " + file)

        parallel.mapreduce(
            parallel.Collection.from_list(input_shards),
            mapper=XML2JSONMapper(),
            reducer=parallel.IdentityReducer(),
            output_prefix=self.output().path,
        )


# Annotation code left in for posterity. This class is not currently used at the request of CVM.
class AnnotateEvent(luigi.Task):
    def requires(self):
        return [CombineHarmonization(), XML2JSON()]

    def output(self):
        return luigi.LocalTarget(
            os.path.join(config.data_dir("adae/annotate.db"), BATCH)
        )

    def run(self):
        harmonized_file = self.input()[0].path
        parallel.mapreduce(
            parallel.Collection.from_sharded(self.input()[1].path),
            mapper=annotate.AnnotateMapper(
                harmonized_dict=annotate.read_harmonized_file(open(harmonized_file))
            ),
            reducer=parallel.IdentityReducer(),
            output_prefix=self.output().path,
        )


class LoadJSON(index_util.LoadJSONBase):
    index_name = "animalandveterinarydrugevent"
    mapping_file = "./schemas/animalandveterinarydrugevent_mapping.json"
    data_source = XML2JSON()
    use_checksum = False
    optimize_index = True
    last_update_date = lambda _: newest_file_timestamp(ADAE_LOCAL_DIR)


if __name__ == "__main__":
    luigi.run()
