import arrow
import logging
import os

import luigi
from luigi.task import flatten

class DependencyTriggeredTask(luigi.Task):
  '''
  A task that re-runs if any of its dependencies are newer than its output, or
  if any dependency is incomplete (and therefore will _become_ newer than it).
  '''
  def complete(self):
    if not self.output().exists():
      return False

    deps = flatten(self.requires())
    logging.debug('Deps: %s', [d.complete() for d in deps])
    for dep in deps:
      if not dep.complete():
        logging.info('Dependencies are incomplete, rerunning task.')
        return False

    inputs = flatten(self.input())
    output_ts = os.path.getmtime(self.output().path)
    input_ts = max([os.path.getmtime(in_file.path) for in_file in inputs])

    if input_ts > output_ts:
      logging.info('Dependencies are newer, rerunning task.')
      return False

    return True


class AlwaysRunTask(luigi.Task):
  '''
  A task that should always run once.

  This is generally used for tasks that are idempotent, and for which the
  "done" state is difficult to determine: e.g. initializing an index, or
  downloading files.

  N.B. Luigi creates *new instances* of tasks when performing completeness
  checks.  This means we can't store our completeness as a local boolean,
  and instead have to use a file on disk as a flag.

  To allow subclasses to use `output` normally, we don't use an `output` file
  here and instead manage completion manually.
  '''
  nonce_timestamp = luigi.Parameter(default=arrow.utcnow().format('YYYY-MM-DD-HH-mm-ss'))

  def __init__(self, *args, **kw):
    luigi.Task.__init__(self, *args, **kw)

  def requires(self):
    return []

  def _nonce_file(self):
    import hashlib
    digest = hashlib.sha1(repr(self)).hexdigest()
    return '/tmp/always-run-task/%s-%s' % (self.nonce_timestamp, digest)

  def complete(self):
    return os.path.exists(self._nonce_file())

  def run(self):
    self._run()

    os.system('mkdir -p "/tmp/always-run-task"')
    with open(self._nonce_file(), 'w') as nonce_file:
      nonce_file.write('finished.')
