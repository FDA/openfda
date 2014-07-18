#!/usr/local/bin/python

import glob
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

rows = []

def handle_barcode(_, barcode):
  if 'source' in barcode:
    href = barcode['source']['@href']
    href_split = href.split('/')
    date_and_setid = href_split[-1].split('_')

    row_dict = {}
    this_date = date_and_setid[0]

    row_dict['date'] = \
    this_date[0:4] + '-' + this_date[4:6] + '-' + this_date[6:8]

    row_dict['set_id'] = date_and_setid[1]

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

def XML2JSON(input_file, output_file):
  out = open(output_file, 'w')
  for xml_filename in glob.glob(input_file):
    escaped_xml = StringIO.StringIO(escape_xml(open(xml_filename).read()))
    xmltodict.parse(escaped_xml, item_depth=2, item_callback=handle_barcode)

  for output in rows:
    out.write(json.dumps(output) + '\n')
