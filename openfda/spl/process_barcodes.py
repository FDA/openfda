#!/usr/local/bin/python

import glob
from os.path import basename, dirname
import simplejson as json
import StringIO
import xmltodict

XML_ESCAPE_CHARS = {
  '"': '&quot;',
  '\'': '&apos;',
  '<': '&lt;',
  '>': '&gt;',
  '&': '&amp;'
}

def XML2JSON(input_file):
  rows = []

  def handle_barcode(_, barcode):
    row_dict = {}
    if 'source' in barcode:
      href = barcode['source']['@href']
      row_dict['id'] = basename(href)

      if ('index' in barcode['source'] and
          'symbol' in barcode['source']['index']):
        symbol = barcode['source']['index']['symbol']

        barcode_type = ''
        if '@type' in symbol:
          barcode_type = symbol['@type']
        quality = ''
        if '@quality' in symbol:
          quality = symbol['@quality']
        data = ''
        if 'data' in symbol:
          data = symbol['data']

        row_dict['barcode_type'] = barcode_type
        row_dict['quality'] = quality
        row_dict['upc'] = data
        rows.append(row_dict)

    return True

  def escape_xml(raw_xml):
    # zbar doesn't do xml escaping, so we have to do it here before we hand the
    # xml off to a parser
    lines = ['<allbarcodes>']
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

      lines.append(line)
    lines.append('</allbarcodes>')
    return '\n'.join(lines)

  out_file = input_file.replace('.xml', '.json')
  out = open(out_file, 'w')
  escaped_xml = StringIO.StringIO(escape_xml(open(input_file).read()))
  xmltodict.parse(escaped_xml, item_depth=2, item_callback=handle_barcode)
  for row in rows:
   out.write(json.dumps(row) + '\n')
