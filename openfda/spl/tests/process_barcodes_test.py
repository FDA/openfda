#!/usr/bin/env python
import unittest
import shutil, tempfile, json
from os import path
from openfda.test_common import data_filename
from openfda.spl.process_barcodes import XML2JSON

class ProcessBarCodesTest(unittest.TestCase):

  def setUp(self):
    self.test_dir = tempfile.mkdtemp()

  def tearDown(self):
    shutil.rmtree(self.test_dir)


  def verify_parsing(self, f, actual, msg):
    src_file = data_filename('barcodes/' + f)
    raw_input_file = path.join(self.test_dir, f)
    shutil.copy(src=src_file, dst=raw_input_file)
    XML2JSON(raw_input_file)
    raw_output_file = path.join(self.test_dir, f.replace('.xml', '.json'))
    with open(raw_output_file, 'r') as fjson:
      result = fjson.read()
      if result == '':
        self.assertEquals(actual, result, msg)
      else:
        self.assertDictEqual(actual, json.loads(result), msg)

  def test_escape_xml(self):

    self.verify_parsing('1_otc-bars.xml',
                        {"quality": "247", "id": "b8cd1d5a-9d3b-465e-9e5b-2f67af8dbeb2", "upc": "0361570045106",
                         "barcode_type": "EAN-13"},
                        "1_otc-bars.xml failed, handle UNICODE characters properly")
    self.verify_parsing('2_otc-bars.xml',"",
                        "2_otc-bars.xml failed, must handle unclosed <barcodes> tag properly")
    self.verify_parsing('3_otc-bars.xml',"",
                        "3_otc-bars.xml failed, must handle empty source")
    self.verify_parsing('4_otc-bars.xml',
                        {"quality": "187", "id": "fd5ccac9-a54a-4284-80c8-a5f035c5d9e6", "upc": "0098443601700",
                         "barcode_type": "EAN-13"},
                        "4_otc-bars.xml failed, must handle valid <barcodes>")
    self.verify_parsing('5_otc-bars.xml',"",
                        "5_otc-bars.xml failed, must ignore types other than EAN-13")
    self.verify_parsing('6_otc-bars.xml', "",
                        "6_otc-bars.xml failed, must handle properly blank xmls")

def main(argv):
  unittest.main(argv=argv)

if __name__ == '__main__':
  unittest.main()

