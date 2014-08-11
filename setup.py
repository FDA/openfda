#!/usr/bin/python

import setuptools

setuptools.setup(
  name='openfda',
  version='0.01',
  maintainer='Medullan',
  maintainer_email='mjoseph@medullan.com',
  url='http://github.com/medullan/openfda',
  install_requires=[
    'BeautifulSoup4',
    'click',
    'flask',
    'leveldb',
    'luigi',
    'lxml',
    'nose',
    'python-gflags',
    'pyyaml',
    'requests',
    'simplejson',
    'xmltodict',
  ],
  description=('A research project to provide open APIs, raw data downloads, '
               'documentation and examples, and a developer community for an '
               'important collection of FDA public datasets.'),
  packages = ['openfda',
              'openfda.annotation_table',
              'openfda.faers',
              'openfda.spl',
              ],
  zip_safe=False,
  test_suite = 'nose.collector',
)
