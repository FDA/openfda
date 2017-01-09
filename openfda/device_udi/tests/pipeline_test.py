#!/usr/bin/env python
# coding=utf-8
import os
import shutil
import tempfile
import unittest
from os.path import dirname

import luigi
from mock import MagicMock
from openfda.device_udi.pipeline import SyncS3DeviceUDI, ExtractXML, XML2JSONMapper, UDIAnnotateMapper
from openfda.tests.api_test_helpers import *


class UDIPipelineTests(unittest.TestCase):
  def setUp(self):
    self.test_dir = tempfile.mkdtemp()

  def tearDown(self):
    shutil.rmtree(self.test_dir)

  def test_annotation(self):
    harmonized_db = MagicMock()
    harmonized_db.get = lambda product_code, _: {"510k": [{"k_number": "K094035"}], \
                                                 "device_pma": [{"pma_number": "P950002"}], \
                                                 "registration": [{"fei_number": "3001451451"}], \
                                                 } if product_code == "OQG" else {}

    ann = UDIAnnotateMapper(harmonized_db)
    mapper = XML2JSONMapper()

    def add_fn(id, json):
      harmonized = ann.harmonize(json)
      eq_("OQG", harmonized["product_codes"][0]["code"])
      eq_(None, harmonized["product_codes"][0]["openfda"].get("pma_number"))
      eq_(None, harmonized["product_codes"][0]["openfda"].get("k_number"))
      eq_(None, harmonized["product_codes"][0]["openfda"].get("fei_number"))

      eq_({}, harmonized["product_codes"][1].get("openfda"))

    map_input = MagicMock()
    map_input.filename = os.path.join(dirname(os.path.abspath(__file__)), "test.xml")
    map_output = MagicMock()
    map_output.add = add_fn
    mapper.map_shard(map_input, map_output)

  def test_xml_to_json(self):
    mapper = XML2JSONMapper()

    map_input = MagicMock()
    map_input.filename = os.path.join(dirname(os.path.abspath(__file__)), "test.xml")

    def add_fn(id, json):
      eq_("gs1_00844588018923", id)
      eq_("gs1_00844588018923", json["@id"])

      eq_("00844588018923", json["identifiers"][0]["id"])
      eq_("Primary", json["identifiers"][0]["type"])
      eq_("GS1", json["identifiers"][0]["issuing_agency"])

      eq_("00844868017264", json["identifiers"][1]["id"])
      eq_("Package", json["identifiers"][1]["type"])
      eq_("GS1", json["identifiers"][1]["issuing_agency"])
      eq_("00844868017288", json["identifiers"][1]["unit_of_use_id"])
      eq_("25", json["identifiers"][1]["quantity_per_package"])
      eq_("2015-11-11", json["identifiers"][1]["package_discontinue_date"])
      eq_("Not in Commercial Distribution", json["identifiers"][1]["package_status"])
      eq_("case", json["identifiers"][1]["package_type"])

      eq_("Published", json["record_status"])
      eq_("2015-09-24", json["publish_date"])
      eq_("2018-09-24", json["commercial_distribution_end_date"])
      eq_("In Commercial Distribution", json["commercial_distribution_status"])
      eq_("CS2 Acet. Cup Sys. - VitalitE", json["brand_name"])
      eq_("1107-0-3258", json["version_or_model_number"])
      eq_("1107-0-3258", json["catalog_number"])
      eq_("CONSENSUS ORTHOPEDICS, INC.", json["company_name"])
      eq_("1", json["device_count_in_base_package"])
      eq_("Acet. Insert, VitalitE", json["device_description"])
      eq_("true", json["is_direct_marking_exempt"])
      eq_("false", json["is_pm_exempt"])
      eq_("false", json["is_hct_p"])
      eq_("false", json["is_kit"])
      eq_("true", json["is_combination_product"])
      eq_("true", json["is_single_use"])
      eq_("true", json["has_lot_or_batch_number"])
      eq_("false", json["has_serial_number"])
      eq_("false", json["has_manufacturing_date"])
      eq_("true", json["has_expiration_date"])
      eq_("false", json["has_donation_id_number"])
      eq_("false", json["is_labeled_as_nrl"])
      eq_("true", json["is_labeled_as_no_nrl"])
      eq_("true", json["is_rx"])
      eq_("false", json["is_otc"])
      eq_("Labeling does not contain MRI Safety Information", json["mri_safety"])
      eq_("+1(916)355-7100", json["customer_contacts"][0]["phone"])
      eq_("ext555", json["customer_contacts"][0]["ext"])
      eq_("fang.wei@fda.hhs.gov", json["customer_contacts"][0]["email"])
      eq_("+1 (555) 555-5555", json["customer_contacts"][1]["phone"])
      eq_(None, json["customer_contacts"][1].get("ext"))
      eq_("jdoe@example.com", json["customer_contacts"][1]["email"])

      eq_("Non-constrained polyethylene acetabular liner", json["gmdn_terms"][0]["name"])
      eq_(
        "A sterile, implantable component of a two-piece acetabulum prosthesis that is inserted\n                    into an acetabular shell prosthesis to provide the articulating surface with a femoral head\n                    prosthesis as part of a total hip arthroplasty (THA). It is made of polyethylene (includes hylamer,\n                    cross-linked polyethylene), and does not include a stabilizing component to limit the range of\n                    motion of the hip.",
        json["gmdn_terms"][0]["definition"])
      eq_("Bone-screw internal spinal fixation system, non-sterile", json["gmdn_terms"][1]["name"])
      eq_(
        "An assembly of non-sterile implantable devices intended to provide immobilization and\n                    stabilization of spinal segments in the treatment of various spinal instabilities or deformities,\n                    also used as an adjunct to spinal fusion [e.g., for degenerative disc disease (DDD)]. Otherwise\n                    known as a pedicle screw instrumentation system, it typically consists of a combination of anchors\n                    (e.g., bolts, hooks, pedicle screws or other types), interconnection mechanisms (incorporating nuts,\n                    screws, sleeves, or bolts), longitudinal members (e.g., plates, rods, plate/rod combinations),\n                    and/or transverse connectors. Non-sterile disposable devices associated with implantation may be\n                    included.",
        json["gmdn_terms"][1]["definition"])
      eq_("OQG", json["product_codes"][0]["code"])
      eq_(
        "Hip Prosthesis, semi-constrained, cemented, metal/polymer, + additive, porous,\n                    uncemented",
        json["product_codes"][0]["name"])
      eq_("MAX", json["product_codes"][1]["code"])
      eq_("Intervertebral fusion device with bone graft, lumbar", json["product_codes"][1]["name"])

      eq_("Millimeter", json["device_sizes"][0]["unit"])
      eq_("32/6", json["device_sizes"][0]["value"])
      eq_("Outer Diameter", json["device_sizes"][0]["type"])
      eq_("Size test here", json["device_sizes"][0]["text"])

      eq_(None, json["device_sizes"][1].get("unit"))
      eq_(None, json["device_sizes"][1].get("value"))
      eq_("Device Size Text, specify", json["device_sizes"][1]["type"])
      eq_(unicode("SPACER LAT PEEK 8° 40L X 18W X 10H", encoding="utf-8"), json["device_sizes"][1]["text"])

      eq_("Storage Environment Temperature", json["storage"][0]["type"])
      eq_("Degrees Celsius", json["storage"][0]["high"]["unit"])
      eq_("8", json["storage"][0]["high"]["value"])
      eq_("Degrees Celsius", json["storage"][0]["low"]["unit"])
      eq_("-30", json["storage"][0]["low"]["value"])
      eq_(None, json["storage"][0].get("special_conditions"))

      eq_("Special Storage Condition, Specify", json["storage"][1]["type"])
      eq_("This device must be stored in a dry location away from temperature\n                    extremes",
          json["storage"][1].get("special_conditions"))
      eq_(None, json["storage"][1].get("high"))
      eq_(None, json["storage"][1].get("low"))

      eq_("false", json["sterilization"]["is_sterile"])
      eq_("true", json["sterilization"]["is_sterilization_prior_use"])
      eq_("Moist Heat or Steam Sterilization", json["sterilization"]["sterilization_methods"])

      eq_(["3001451451", "12223430908"], json["fei_number"])

      eq_("K094035", json["pma_submissions"][0]["submission_number"])
      eq_("000", json["pma_submissions"][0]["supplement_number"])
      eq_("PMN", json["pma_submissions"][0]["submission_type"])
      eq_("P950002", json["pma_submissions"][1]["submission_number"])
      eq_("001", json["pma_submissions"][1]["supplement_number"])
      eq_("PMA", json["pma_submissions"][1]["submission_type"])

    map_output = MagicMock()
    map_output.add = add_fn
    mapper.map_shard(map_input, map_output)

  def test_extract_xml(self):
    shutil.copyfile(os.path.join(dirname(os.path.abspath(__file__)), "file.zip"),
                    os.path.join(self.test_dir, "file.zip"))
    extract = ExtractXML()
    extract.local_dir = self.test_dir
    extract.output = MagicMock(
      return_value=luigi.LocalTarget(self.test_dir))
    extract.run()
    ok_(os.path.isfile(os.path.join(self.test_dir, "file1.xml")))

  def test_s3_sync(self):
    s3_sync = SyncS3DeviceUDI()
    s3_sync.local_dir = self.test_dir

    # Test marker file.
    eq_(s3_sync.flag_file(), os.path.join(self.test_dir, '.last_sync_time'))
    ok_(not s3_sync.complete())
    with open(os.path.join(self.test_dir, '.last_sync_time'), 'w') as out_f:
      out_f.write('')
    ok_(s3_sync.complete())
    os.remove(os.path.join(self.test_dir, '.last_sync_time'))
    ok_(not s3_sync.complete())


def main(argv):
  unittest.main(argv=argv)


if __name__ == '__main__':
  unittest.main()
