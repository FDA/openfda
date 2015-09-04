import luigi
from .mapreduce import Collection, mapreduce
from .mapper import Mapper
from .reducer import Reducer
from .outputs import LevelDBOutput

class MRTask(luigi.Task, Mapper, Reducer):
  def map(self, key, value, output):
    output.add(key, value)

  def reduce(self, key, values, output):
    for value in values:
      output.put(key, value)

  def mapreduce_inputs(self):
    inputs = self.input()
    if not isinstance(inputs, list): inputs = [inputs]
    return Collection.from_list([input.path for input in inputs])

  def num_shards(self):
    return None

  def output_format(self):
    return LevelDBOutput()

  def run(self):
    mapreduce(
     self.mapreduce_inputs(),
     mapper=self,
     reducer=self,
     output_prefix=self.output().path,
     output_format=self.output_format(),
     num_shards=self.num_shards()
    )
