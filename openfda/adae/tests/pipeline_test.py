#!/usr/bin/env python
# coding=utf-8
import os
import shutil
import tempfile
import unittest
from os.path import dirname

import luigi
from mock import MagicMock
from openfda.adae.pipeline import ExtractXML, XML2JSONMapper
from openfda.adae.annotate import normalize_product_ndc
from openfda.tests.api_test_helpers import *


class ADAEPipelineTests(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_extract_xml(self):
        shutil.copyfile(
            os.path.join(dirname(os.path.abspath(__file__)), "file.zip"),
            os.path.join(self.test_dir, "file.zip"),
        )
        extract = ExtractXML()
        extract.local_dir = self.test_dir
        extract.output = MagicMock(return_value=luigi.LocalTarget(self.test_dir))
        extract.map(os.path.join(self.test_dir, "file.zip"), "", [])
        ok_(os.path.isfile(os.path.join(self.test_dir, "file1.xml")))

    def test_normalize_product_ndc(self):
        eq_("57319-485", normalize_product_ndc("NDC57319-485-05"))

    def test_is_digit(self):
        mapper = XML2JSONMapper()
        ok_(mapper.isdigit("1"))
        ok_(mapper.isdigit("0"))
        ok_(mapper.isdigit("1.5"))
        ok_(mapper.isdigit("-10"))
        ok_(mapper.isdigit("-20.5"))
        ok_(mapper.isdigit("+1245.33434"))
        ok_(mapper.isdigit("-1245.33434"))
        ok_(mapper.isdigit("0.00"))
        ok_(not mapper.isdigit("I"))
        ok_(not mapper.isdigit("I0"))
        ok_(not mapper.isdigit("."))

    def test_xml_to_json(self):
        mapper = XML2JSONMapper()
        map_input = MagicMock()
        map_input.filename = os.path.join(
            dirname(os.path.abspath(__file__)), "test.xml"
        )

        map_dict = {}

        def add(id, json):
            map_dict[id] = json

        map_output = MagicMock()
        map_output.add = add
        mapper.map_shard(map_input, map_output)

        uid = "USA-USFDACVM-2016-US-017128"

        ae = map_dict[uid]
        eq_(uid, ae["unique_aer_id_number"])
        eq_(uid, ae["@id"])
        eq_("N141203", ae["report_id"])
        eq_("20160502", ae["original_receive_date"])
        eq_(
            "Food and Drug Administration Center for Veterinary Medicine",
            ae["receiver"]["organization"],
        )
        eq_("7500 Standish Place (HFV-210) Room N403", ae["receiver"]["street_address"])
        eq_("Rockville", ae["receiver"]["city"])
        eq_("MD", ae["receiver"]["state"])
        eq_("20855", ae["receiver"]["postal_code"])
        eq_("USA", ae["receiver"]["country"])
        eq_("Other", ae["primary_reporter"])
        eq_("Animal Owner", ae["secondary_reporter"])
        eq_("Safety Issue", ae["type_of_information"])
        eq_("true", ae["serious_ae"])
        eq_("2", ae["number_of_animals_treated"])
        eq_("3", ae["number_of_animals_affected"])
        eq_("Dog", ae["animal"]["species"])
        eq_("Male", ae["animal"]["gender"])
        eq_("Neutered", ae["animal"]["reproductive_status"])
        eq_("NOT APPLICABLE", ae["animal"]["female_animal_physiological_status"])

        eq_("7.00", ae["animal"]["age"]["min"])
        eq_("17.5", ae["animal"]["age"]["max"])
        eq_("Year", ae["animal"]["age"]["unit"])
        eq_("Measured", ae["animal"]["age"]["qualifier"])

        eq_("6.123", ae["animal"]["weight"]["min"])
        eq_("16.123", ae["animal"]["weight"]["max"])
        eq_("Kilogram", ae["animal"]["weight"]["unit"])
        eq_("Measured", ae["animal"]["weight"]["qualifier"])

        eq_("false", ae["animal"]["breed"]["is_crossbred"])
        eq_("Terrier - Yorkshire", ae["animal"]["breed"]["breed_component"])

        eq_("Ongoing", ae["outcome"][0]["medical_status"])
        eq_("1", ae["outcome"][0]["number_of_animals_affected"])

        eq_("Good", ae["health_assessment_prior_to_exposure"]["condition"])
        eq_("Veterinarian", ae["health_assessment_prior_to_exposure"]["assessed_by"])

        eq_("20150601", ae["onset_date"])
        eq_("14", ae["duration"]["value"])
        eq_("Month", ae["duration"]["unit"])

        eq_("11", ae["reaction"][0]["veddra_version"])
        eq_("2227", ae["reaction"][0]["veddra_term_code"])
        eq_("Dental disease", ae["reaction"][0]["veddra_term_name"])
        eq_("1", ae["reaction"][0]["number_of_animals_affected"])
        eq_("Actual", ae["reaction"][0]["accuracy"])

        eq_("11", ae["reaction"][1]["veddra_version"])
        eq_("1026", ae["reaction"][1]["veddra_term_code"])
        eq_(
            "Localised pain NOS (see other 'SOCs' for specific pain)",
            ae["reaction"][1]["veddra_term_name"],
        )
        eq_("1", ae["reaction"][1]["number_of_animals_affected"])
        eq_("Actual", ae["reaction"][1]["accuracy"])

        eq_("2", ae["reaction"][13]["veddra_version"])
        eq_("99115", ae["reaction"][13]["veddra_term_code"])
        eq_("INEFFECTIVE, HEARTWORM LARVAE", ae["reaction"][13]["veddra_term_name"])
        eq_("1", ae["reaction"][13]["number_of_animals_affected"])
        eq_("Actual", ae["reaction"][13]["accuracy"])

        eq_("10 days", ae["time_between_exposure_and_onset"])
        eq_("true", ae["treated_for_ae"])

        eq_(8, len(ae["drug"]))

        eq_("20150601", ae["drug"][0]["first_exposure_date"])
        eq_("20151201", ae["drug"][0]["last_exposure_date"])
        eq_("1", ae["drug"][0]["frequency_of_administration"]["value"])
        eq_("Day", ae["drug"][0]["frequency_of_administration"]["unit"])
        eq_("Animal Owner", ae["drug"][0]["administered_by"])
        eq_("Oral", ae["drug"][0]["route"])
        eq_("0.50", ae["drug"][0]["dose"]["numerator"])
        eq_("tablet", ae["drug"][0]["dose"]["numerator_unit"])
        eq_("1", ae["drug"][0]["dose"]["denominator"])
        eq_("Unknown", ae["drug"][0]["dose"]["denominator_unit"])
        eq_("false", ae["drug"][0]["used_according_to_label"])
        eq_(["Route Off-Label", "Underdosed"], ae["drug"][0]["off_label_use"])
        eq_("false", ae["drug"][0]["previous_exposure_to_drug"])
        eq_("false", ae["drug"][0]["previous_ae_to_drug"])
        eq_("false", ae["drug"][0]["ae_abated_after_stopping_drug"])
        eq_("true", ae["drug"][0]["ae_reappeared_after_resuming_drug"])
        eq_("20160101", ae["drug"][0]["manufacturing_date"])
        eq_("71423", ae["drug"][0]["lot_number"])
        eq_("20180228", ae["drug"][0]["lot_expiration"])
        eq_("1111-2222", ae["drug"][0]["product_ndc"])
        eq_("Deramaxx Chewable Tablets", ae["drug"][0]["brand_name"])
        eq_("Tablet", ae["drug"][0]["dosage_form"])
        eq_("Elanco US Inc", ae["drug"][0]["manufacturer"]["name"])
        eq_(
            "USA-USFDACVM-N141203", ae["drug"][0]["manufacturer"]["registration_number"]
        )
        eq_("1", ae["drug"][0]["number_of_defective_items"])
        eq_("11", ae["drug"][0]["number_of_items_returned"])
        eq_("QM01AH94", ae["drug"][0]["atc_vet_code"])
        eq_("Deracoxib", ae["drug"][0]["active_ingredients"][0]["name"])
        eq_("25", ae["drug"][0]["active_ingredients"][0]["dose"]["numerator"])
        eq_(
            "Milligram",
            ae["drug"][0]["active_ingredients"][0]["dose"]["numerator_unit"],
        )
        eq_("1", ae["drug"][0]["active_ingredients"][0]["dose"]["denominator"])
        eq_("dose", ae["drug"][0]["active_ingredients"][0]["dose"]["denominator_unit"])

    def test_empty_date_handling(self):
        mapper = XML2JSONMapper()
        map_input = MagicMock()
        map_input.filename = os.path.join(
            dirname(os.path.abspath(__file__)), "test_blank_orig_date.xml"
        )

        map_dict = {}

        def add(id, json):
            map_dict[id] = json

        map_output = MagicMock()
        map_output.add = add
        mapper.map_shard(map_input, map_output)

        uid = "USA-USFDACVM-2016-US-017128"

        ae = map_dict[uid]
        eq_(None, ae.get("original_receive_date"))

    def test_non_numeric_denominator(self):
        mapper = XML2JSONMapper()
        map_input = MagicMock()
        map_input.filename = os.path.join(
            dirname(os.path.abspath(__file__)), "test-non-numeric-denominator.xml"
        )

        map_dict = {}

        def add(id, json):
            map_dict[id] = json

        map_output = MagicMock()
        map_output.add = add
        mapper.map_shard(map_input, map_output)

        uid = "USA-USFDACVM-2016-US-017128"

        ae = map_dict[uid]
        eq_(uid, ae["unique_aer_id_number"])
        eq_(uid, ae["@id"])
        eq_("0", ae["drug"][0]["dose"]["denominator"])
        eq_("Unknown", ae["drug"][0]["dose"]["denominator_unit"])


def main(argv):
    unittest.main(argv=argv)


if __name__ == "__main__":
    unittest.main()
