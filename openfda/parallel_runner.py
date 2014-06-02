#!/usr/bin/python

"""
Helper for running file extraction in parallel.
"""

import multiprocessing

from openfda import multiprocessing_util

def parallel_extract(files, worker):
  manager = multiprocessing.Manager()
  name_queue = manager.Queue()
  pool = multiprocessing_util.Pool()
  pool.map_async(worker, [(f, name_queue) for f in files])
  pool.join()

  rows = []
  while not name_queue.empty():
    rows.append(name_queue.get())
  return rows

