#!/usr/bin/python

''' A unit test for the MAUDE join function
'''
import os
import unittest

from openfda.maude import join_maude
from openfda.test_common import data_filename

class JoinMaudeUnitTest(unittest.TestCase):
  'Join MAUDE Unit Test'

  def set_up(self):
    pass

  def test_join(self):
    pwd = os.path.dirname(__file__)
    master_file = pwd + '/data/mdrfoi.txt'
    patient_file = pwd + '/data/patient.txt'
    device_file = pwd + '/data/foidev.txt'
    text_file = pwd + '/data/foitext.txt'
    out_file = '/tmp/test_output.json'
    reject_file = '/tmp/test_output.rejects'

    expected_maude_json = open(pwd + '/data/maude_one.json').read().strip()
    expected_reject_file = open(pwd + '/data/maude_one.rejects').read().strip()

    join_maude.join_maude(master_file,
                          patient_file,
                          device_file,
                          text_file,
                          out_file)

    actual_maude_json = open('/tmp/test_output.json').read().strip()
    actual_maude_rejects = open('/tmp/test_output.rejects').read().strip()

    self.assertEqual(expected_maude_json,
                     actual_maude_json,
                     pwd + '/data/maude_one.json')

    self.assertEqual(expected_reject_file,
                     actual_maude_rejects,
                     pwd + '/data/maude_one.rejects')

if __name__ == '__main__':
  unittest.main()
