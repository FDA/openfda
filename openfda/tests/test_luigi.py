import luigi
import os
from openfda.tasks import DependencyTriggeredTask

from luigi import interface, scheduler, worker

class SimpleTask(luigi.Task):
  def output(self):
    return luigi.LocalTarget('/tmp/a-created')

  def run(self):
    os.system('touch "%s"' % self.output().path)

class DepTask(DependencyTriggeredTask):
  def requires(self):
    return [SimpleTask()]

  def output(self):
    return luigi.LocalTarget('/tmp/b-created')

  def run(self):
    os.system('touch "%s"' % self.output().path)

if __name__ == '__main__':
  os.system('rm -f /tmp/a-created /tmp/b-created')
  interface.setup_interface_logging()
  sch = scheduler.CentralPlannerScheduler()
  w = worker.Worker(scheduler=sch)
  w.add(SimpleTask())
  w.run()
  w.add(DepTask())
  w.run()
  os.system('rm -f /tmp/a-created /tmp/b-created')
  w.add(DepTask())
  w.run()
  os.system('rm -f /tmp/a-created')
  w.add(DepTask())
  w.run()
