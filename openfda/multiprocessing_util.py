#!/usr/bin/python

"""
Utilities to make it easier to use the python multiprocessing module.
"""

import logging
import multiprocessing
from multiprocessing.pool import Pool as NativePool
import signal
import sys
import time
import traceback

class MultiprocessingLogExceptions(object):
  def __init__(self, callable):
    self.__callable = callable
    return

  def __call__(self, *args, **kwargs):
    try:
      result = self.__callable(*args, **kwargs)

    except Exception as e:
      # Here we add some debugging help. If multiprocessing's
      # debugging is on, it will arrange to log the traceback
      multiprocessing.get_logger().error(traceback.format_exc())
      # Re-raise the original exception so the Pool worker can
      # clean up
      raise

    # It was fine, give a normal answer
    return result

class Pool(NativePool):
  def __init__(self, *args, **kwargs):
    handler = None
    if logging.getLogger().handlers:
      # Use an existing lo handler if already setup.
      handler = logging.getLogger().handlers[0]
    else:
      # Otherwise use some good defaults.
      handler = logging.StreamHandler(sys.stderr)
      handler.setLevel(logging.INFO)
      handler.setFormat(
        logging.Formatter(
        '%(created)f %(filename)s:%(lineno)s [%(funcName)s] %(message)s'))

    multiprocessing.get_logger().addHandler(handler)

    kwargs['initializer'] = lambda: signal.signal(signal.SIGINT,
      signal.SIG_IGN)
    NativePool.__init__(self, *args, **kwargs)

    self.results = []
    self.prev_message = None

  def apply_async(self, func, args=(), kwargs={}, callback=None):
    result = NativePool.apply_async(self,
      MultiprocessingLogExceptions(func),
      args, kwargs, callback)
    self.results.append(result)
    return result

  def map_async(self, func, args=(), kwargs={}, callback=None):
    results = NativePool.map_async(
      self, MultiprocessingLogExceptions(func),
      args, kwargs, callback)
    self.results.extend(results)
    return results

  def progress(self, msg, *args):
    # Don't show any progress if the output is directed to a file.
    if not sys.stderr.isatty():
      return

    text = (msg % args)
    if self.prev_message:
      sys.stderr.write(' ' * len(self.prev_message) + '\r')
    sys.stderr.write(text + '\r')
    self.prev_message = text

  def join(self):
    try:
      while True:
        total_tasks = len(self.results)
        done_tasks = 0
        for result in self.results:
          if result.ready():
            done_tasks += 1

        if done_tasks == total_tasks:
          self.progress('[%d task(s) completed, %d process(es)]',
            done_tasks, self._processes)
          break
        else:
          self.progress('[%d task(s) completed, %d remaining, %d process(es)]',
            done_tasks, total_tasks - done_tasks, self._processes)
          time.sleep(0.001)

    except KeyboardInterrupt:
      NativePool.terminate(self)
      return NativePool.join(self)
