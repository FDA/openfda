#!/usr/bin/env python

''' A set of utility functions used for downloading and extracting files used
    as inputs for pipelines.
'''

import glob
import logging
import os
from os.path import basename, dirname, join

from openfda import common

def extract_and_clean(input_dir, source_encoding, target_encoding, file_type):
  ''' A utility function that extracts all of the zip files in a directory and
      converts the files from a source encoding to a target encoding.
  '''
  for zip_filename in glob.glob(input_dir + '/*.zip'):
    txt_name = zip_filename.replace('zip', file_type)
    txt_name = txt_name.replace('raw', 'extracted')
    common.shell_cmd('mkdir -p %s', dirname(txt_name))
    cmd = 'unzip -p %s | iconv -f %s -t %s -c > %s'
    logging.info('Unzipping and converting %s', zip_filename)
    common.shell_cmd(cmd,
                     zip_filename,
                     source_encoding,
                     target_encoding,
                     txt_name)
