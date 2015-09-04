import cPickle
import csv
import leveldb
import logging
import simplejson as json
import xmltodict

logger = logging.getLogger('mapreduce')

class FileSplit(object):
  '''
  A split represents a subset (potentially all) of an input file.

  Splits allow for processing files that can be read by multiple processes
  simultaneously (e.g. lines from a text input).

  The start_pos and end_pos are interpreted by the input format (for example
  they could be line numbers for a line oriented format, or start/stop keys
  for a key-value format).
  '''
  def __init__(self, filename, mr_input, start_pos, end_pos):
    self.filename = filename
    self.mr_input = mr_input
    self.start_pos = start_pos
    self.end_pos = end_pos

  def __repr__(self):
    return 'Split(%s -- %s:%s)' % (self.filename, self.start_pos, self.end_pos)


class MRInput(object):
  class Reader(object):
    def __init__(self, split, **kw):
      self.filename = split.filename
      self.start_pos = split.start_pos
      self.end_pos = split.end_pos
      self.split = split

      for k, v in kw.items():
        setattr(self, k, v)

    def __repr__(self):
      return 'Reader(%s)' % self.filename

    def __iter__(self):
      for count, (k, v) in enumerate(self.entries()):
        yield k, v

  def compute_splits(self, filename):
    '''
    The default behavior for splitting files is to have one split per file.
    '''
    return [FileSplit(filename, self, 0, -1)]

  def create_reader(self, split):
    return self.__class__.Reader(split)

class XMLDictInput(MRInput):
  ''' Simple class for set XML records and streaming dictionaries.
      This input reads all of the records into memory; care should be taken
      when using it with large inputs.
  '''
  def __init__(self, depth=1):
    MRInput.__init__(self)
    self.depth = depth

  def create_reader(self, split):
    return XMLDictInput.Reader(split, depth=self.depth)

  class Reader(MRInput.Reader):
    def entries(self):
      _item = []
      def _handler(_, ord_dict):
        _item.append(json.loads(json.dumps(ord_dict)))
        return True

      xml_file = open(self.filename).read()
      xmltodict.parse(xml_file, item_depth=self.depth, item_callback=_handler)

      for idx, line in enumerate(_item):
        yield str(idx), line


class LineInput(MRInput):
  class Reader(MRInput.Reader):
    def entries(self):
      for idx, line in enumerate(open(self.filename)):
        yield str(idx), line.rstrip()


class FilenameInput(MRInput):
  class Reader(MRInput.Reader):
    def entries(self):
      yield (self.filename, '')


class JSONInput(MRInput):
  class Reader(MRInput.Reader):
    def entries(self):
      with open(self.filename, 'r') as input_file:
        data = json.load(input_file)
        for k, v in data.iteritems():
          yield k, v

class JSONLineInput(MRInput):
  class Reader(MRInput.Reader):
    def entries(self):
      for idx, line in enumerate(open(self.filename)):
        yield str(idx), json.loads(line)

# TODO(hansnelsen): convert all the input parameters to a inputdict_args for
#                   readability and simplicity
class CSVDictLineInput(MRInput):
  def __init__(self,
               delimiter=',',
               quoting=csv.QUOTE_MINIMAL,
               fieldnames=None,
               escapechar=None,
               strip_str=None):
    MRInput.__init__(self)
    self.delimiter = delimiter
    self.quoting = quoting
    self.fieldnames = fieldnames
    self.escapechar = escapechar
    self.strip_str = strip_str

  def create_reader(self, split):
    return CSVDictLineInput.Reader(split,
                                   delimiter=self.delimiter,
                                   quoting=self.quoting,
                                   fieldnames=self.fieldnames,
                                   escapechar=self.escapechar,
                                   strip_str=self.strip_str)

  class Reader(MRInput.Reader):
    def entries(self):
      fh = open(self.filename)
      if self.strip_str:
        # Some source files have null bytes that need to be stripped first
        fh = (s.replace(self.strip_str, '') for s in fh)

      dict_reader = csv.DictReader(fh,
                                   delimiter=self.delimiter,
                                   fieldnames=self.fieldnames,
                                   quoting=self.quoting,
                                   escapechar=self.escapechar)

      for idx, line in enumerate(dict_reader):
        yield str(idx), line

class LevelDBInput(MRInput):
  class Reader(MRInput.Reader):
    def __init__(self, split):
      MRInput.Reader.__init__(self, split)
      self.db = leveldb.LevelDB(self.filename)

    def entries(self):
      for key, value in self.db.RangeIter():
        yield key, cPickle.loads(value)
