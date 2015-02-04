#!/usr/bin/python

'''This utility prints out a mapping string that is syntactically correct
to be used in the schemas/spl_mapping.json file.
'''
import logging
import os
import simplejson as json
import sys

# TODO(hansnelsen): Added pretty printing for pleasant looking JSON
# TODO(hansnelsen): Add writing directly to schemas/spl_mapping.json once it is
#                   pretty
def generate_mapping():
  sections = open('../openfda/spl/data/sections.csv', 'r')
  mapping_list = []
  openfda = '''
                "openfda": {
                    "properties": {
                        "application_number": {
                            "type": "string",
                            "index": "analyzed"
                        },
                        "application_number_exact": {
                            "type": "string",
                            "index": "not_analyzed"
                        },
                        "brand_name": {
                            "type": "string",
                            "index": "analyzed"
                        },
                        "brand_name_exact": {
                            "type": "string",
                            "index": "not_analyzed"
                        },
                        "substance_name": {
                            "type": "string",
                            "index": "analyzed"
                        },
                        "substance_name_exact": {
                            "type": "string",
                            "index": "not_analyzed"
                        },
                        "dosage_form": {
                            "type": "string",
                            "index": "analyzed"
                        },
                        "dosage_form_exact": {
                            "type": "string",
                            "index": "not_analyzed"
                        },
                        "generic_name": {
                            "type": "string",
                            "index": "analyzed"
                        },
                        "generic_name_exact": {
                            "type": "string",
                            "index": "not_analyzed"
                        },
                        "manufacturer_name": {
                            "type": "string",
                            "index": "analyzed"
                        },
                        "manufacturer_name_exact": {
                            "type": "string",
                            "index": "not_analyzed"
                        },
                        "product_ndc": {
                            "type": "string",
                            "index": "analyzed"
                        },
                        "product_ndc_exact": {
                            "type": "string",
                            "index": "not_analyzed"
                        },
                        "product_type": {
                            "type": "string",
                            "index": "analyzed"
                        },
                        "product_type_exact": {
                            "type": "string",
                            "index": "not_analyzed"
                        },
                        "route": {
                            "type": "string",
                            "index": "analyzed"
                        },
                        "route_exact": {
                            "type": "string",
                            "index": "not_analyzed"
                        },
                        "rxcui": {
                            "type": "string",
                            "index": "analyzed"
                        },
                        "rxcui_exact": {
                            "type": "string",
                            "index": "not_analyzed"
                        },
                        "rxstring": {
                            "type": "string",
                            "index": "analyzed"
                        },
                        "rxstring_exact": {
                            "type": "string",
                            "index": "not_analyzed"
                        },
                        "rxtty": {
                            "type": "string",
                            "index": "analyzed"
                        },
                        "rxtty_exact": {
                            "type": "string",
                            "index": "not_analyzed"
                        },
                        "spl_id": {
                            "type": "string",
                            "index": "analyzed"
                        },
                        "spl_id_exact": {
                            "type": "string",
                            "index": "not_analyzed"
                        },
                        "package_ndc": {
                            "type": "string",
                            "index": "analyzed"
                        },
                        "package_ndc_exact": {
                            "type": "string",
                            "index": "not_analyzed"
                        },
                        "spl_set_id": {
                            "type": "string",
                            "index": "analyzed"
                        },
                        "spl_set_id_exact": {
                            "type": "string",
                            "index": "not_analyzed"
                        },
                        "unii": {
                            "type": "string",
                            "index": "analyzed"
                        },
                        "unii_exact": {
                            "type": "string",
                            "index": "not_analyzed"
                        },
                        "pharm_class_moa": {
                            "type": "string",
                            "index": "analyzed"
                        },
                        "pharm_class_moa_exact": {
                            "type": "string",
                            "index": "not_analyzed"
                        },
                        "pharm_class_pe": {
                            "type": "string",
                            "index": "analyzed"
                        },
                        "pharm_class_pe_exact": {
                            "type": "string",
                            "index": "not_analyzed"
                        },
                        "pharm_class_cs": {
                            "type": "string",
                            "index": "analyzed"
                        },
                        "pharm_class_cs_exact": {
                            "type": "string",
                            "index": "not_analyzed"
                        },
                        "pharm_class_epc": {
                            "type": "string",
                            "index": "analyzed"
                        },
                        "pharm_class_epc_exact": {
                            "type": "string",
                            "index": "not_analyzed"
                        },
                        "nui": {
                            "type": "string",
                            "index": "analyzed"
                        },
                        "nui_exact": {
                            "type": "string",
                            "index": "not_analyzed"
                        },
                        "upc": {
                            "type": "string",
                            "index": "analyzed"
                        },
                        "upc_exact": {
                            "type": "string",
                            "index": "not_analyzed"
                        },
                        "is_original_packager": {
                            "type": "boolean"
                        },
                        "is_original_packager_exact": {
                            "type": "boolean"
                        }
                    }
                }'''
  mapping_header = '''{
      "spl": {
        "_source": {
          "includes": [
            "*"
          ],
          "excludes": [
            "openfda.*_exact"
          ]
        },
        "properties": {
          "set_id": {
            "type": "string",
            "index": "not_analyzed"
          },
          "id": {
            "type": "string",
            "index": "not_analyzed"
          },
          "version": {
            "type": "string",
            "index": "analyzed"
          },
          "effective_time": {
            "type": "date",
            "format": "basic_date||date"
          },
        '''
  mapping_footer = ''',
            "@timestamp": {
              "type": "date",
              "format": "basic_date||date"
              }
          }
          }
      }'''
  mapping_list.append(mapping_header)

  for row in sections:
    name = row.split(',')[1]\
              .replace(':', '')\
              .replace(' & ', ' and ')\
              .replace('/', ' or ')\
              .replace(' ', '_')\
              .lower()\
              .replace('spl_unclassified', 'spl_unclassified_section')\
              .strip()

    row_string = ''' "%(name)s": {
                         "type": "string",
                         "index": "analyzed"
                        },
                       "%(name)s_table": {
                         "type": "string",
                         "index": "no",
                         "include_in_all": false
                      },''' % locals()
    mapping_list.append(row_string)

  mapping_list.append(openfda)
  mapping_list.append(mapping_footer)

  try:
    mapping_string = '\n'.join(mapping_list)
    json.loads(mapping_string)
    return mapping_string
  except:
    logging.info('It appears that something is wrong with your json string')

if __name__ == '__main__':
  fmt_string = '%(created)f %(filename)s:%(lineno)s [%(funcName)s] %(message)s'
  logging.basicConfig(stream=sys.stderr,
                      format=fmt_string,
                      level=logging.DEBUG)
  print generate_mapping()