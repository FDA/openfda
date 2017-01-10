#!/usr/bin/python

import setuptools

setuptools.setup(
  name='openfda',
  version='0.01',
  maintainer='openFDA',
  maintainer_email='open@fda.hhs.gov',
  url='http://github.com/fda/openfda',
  install_requires=[
    'arrow',
    'beautifulsoup4',
    'boto',
    'click',
    'elasticsearch',
    'flask',
    'leveldb',
    'luigi',
    'lxml',
    'nose',
    'mock',
    'coverage',
    'python-gflags',
    'pyyaml',
    'pyelasticsearch',
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
