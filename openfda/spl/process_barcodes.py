#!/usr/local/bin/python

import StringIO
import logging
import xml.parsers.expat
from os.path import basename

import simplejson as json
import xmltodict
from openfda.common import strip_unicode

XML_ESCAPE_CHARS = {
  '"': '&quot;',
  '\'': '&apos;',
  '<': '&lt;',
  '>': '&gt;',
  '&': '&amp;'
}

BARCODE_TYPES = ['EAN-13']

def XML2JSON(input_file):
  rows = []

  def handle_barcode(_, barcode):
    if isinstance(barcode, dict):
      row_dict = {}
      try:
        all_symbols = []
        symbols = barcode['source']['index']['symbol']
        if isinstance(symbols, list):
          all_symbols.extend(symbols)
        else:
          all_symbols.append(symbols)

        symbol = next((x for x in all_symbols if x['@type'] in BARCODE_TYPES), None)

        if symbol:
          href = barcode['source']['@href']
          barcode_type = symbol['@type']
          quality = symbol['@quality']
          data = symbol['data']

          row_dict['id'] = basename(href)
          row_dict['barcode_type'] = barcode_type
          row_dict['quality'] = quality
          row_dict['upc'] = data
          rows.append(row_dict)

      except KeyError:
        pass
    return True

  def escape_xml(raw_xml):
    # zbar doesn't do xml escaping, so we have to do it here before we hand the
    # xml off to a parser
    lines = []
    #need to handle non-escaped barcodes that have no source
    #if the current line is a barcodes and the previous one was too
    #then prefix the line with a closing barcodes
    previous_line_is_barcodes = False
    for line in raw_xml.split('\n'):
      if '<barcodes' in line and previous_line_is_barcodes:
        line = '</barcodes>' + line
      if '<barcodes' in line:
        previous_line_is_barcodes = True
      if 'source href' in line and previous_line_is_barcodes:
        previous_line_is_barcodes = False
      if 'source href' in line:
        href = line.replace("<source href='", '').replace("'>", '')
        href = href.split('/')[:-1]
        href = '/'.join(href)
        line = "<source href='" + href + "'>"

      line = line.strip()
      if line:
        lines.append(line)

    if len(lines) > 0 and '/barcodes' not in lines[-1]:
      lines.append('</barcodes>')

    return '<allbarcodes>\n' + '\n'.join(lines) + '\n</allbarcodes>'

  out_file = input_file.replace('.xml', '.json')
  out = open(out_file, 'w')
  escaped_xml = StringIO.StringIO(escape_xml(open(input_file).read()))

  try:
    xmltodict.parse(strip_unicode(escaped_xml.getvalue(), True), item_depth=2, item_callback=handle_barcode)
  except xml.parsers.expat.ExpatError as ee:
    logging.info('Error parsing barcode XML file %s', input_file)
    logging.debug(ee)

  for row in rows:
   out.write(json.dumps(row) + '\n')
