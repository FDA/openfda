import logging

logger = logging.getLogger('mapreduce')

class Mapper(object):
  def map(self, key, value, output):
    raise NotImplementedError

  def map_shard(self, map_input, map_output):
    self.filename = map_input.filename
    self.map_input = map_input
    self.map_output = map_output

    logger.info('Starting mapper: input=%s', map_input)
    mapper = self.map
    for idx, (key, value) in enumerate(map_input):
      if idx % 1000 == 0:
        logger.info('Mapping records=%d input=%s key=%s', idx, map_input, key)
      mapper(key, value, map_output)

class IdentityMapper(Mapper):
  def map(self, key, value, output):
    output.add(key, value)
