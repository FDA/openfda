#!/usr/bin/python

'''Helper functions for testing.'''

import inspect
import os

def data_filename(fname, caller_frame=1):
  # get the parent frame
  _, parent_file, _, _, _, _ = inspect.getouterframes(
    inspect.currentframe())[caller_frame]
  base_dir = os.path.abspath(os.path.dirname(parent_file))
  return os.path.join(base_dir, 'data', fname)

def open_data_file(fname):
  return open(data_filename(fname, caller_frame=2))
