import os
import queue
import sys
import threading
import traceback

OUTPUT_DIR = './watchdog/'

class Watchdog(threading.Thread):
  def __init__(self, *args, **kw):
    threading.Thread.__init__(self, *args, **kw)
    self.daemon = True
    self._queue = queue.Queue(maxsize=1)
    self._log_frequency = kw.get('log_frequency', 30)
    self._output_dir = kw.get('output_dir', OUTPUT_DIR)

  def _output_file(self):
      return os.path.join(self._output_dir, '%s.dump' % os.getpid())

  def run(self):
    while True:
      try:
        self._queue.get(timeout=self._log_frequency)
        return
      except queue.Empty:
        pass

      os.system('mkdir -p "%s"' % self._output_dir)
      with open(self._output_file(), 'w') as out_f:
        for threadid, frame in sys._current_frames().items():
          stack = ''.join(traceback.format_stack(frame))
          print('Thread: %s' % threadid, file=out_f)
          print(stack, file=out_f)
          print('\n\n', file=out_f)

  def __enter__(self):
    self.start()
    return self

  def __exit__(self, type, value, traceback):
    # Sentinel value: wake up the watchdog and have it exit
    self._queue.put(None)
    self.join()
    if os.path.exists(self._output_file()):
      os.unlink(self._output_file())
