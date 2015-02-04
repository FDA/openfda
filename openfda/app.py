#!/usr/local/bin/python
#
# Helper for running python programs. Invokes flag parsing logic.
#
# Author: Matt Mohebbi

import inspect
import sys

import gflags as flags
import logging

flags.DEFINE_enum('log_level', 'INFO',
                  ['DEBUG', 'INFO', 'WARN', 'ERROR', 'FATAL'],
                  'Default logging level.')

def run():
  argv = sys.argv
  try:
    # parse flags
    other_argv = flags.FLAGS(argv)

    logging.basicConfig(level=getattr(logging, flags.FLAGS.log_level),
                        format='%(levelname).1s %(created)f %(filename)s:%(lineno)s [%(funcName)s] %(message)s')

    # get main function from previous call frame
    user_main = inspect.currentframe().f_back.f_locals['main']
    user_main(other_argv)

  except flags.FlagsError, e:
    print '%s\n\nUsage: %s ARGS\n%s' % (e, sys.argv[0], flags.FLAGS)
    sys.exit(1)