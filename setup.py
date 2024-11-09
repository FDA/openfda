#!/usr/bin/python

import setuptools
import os

setuptools.setup(
    name="openfda",
    version="1.0",
    maintainer="openFDA",
    maintainer_email="open@fda.hhs.gov",
    url="http://github.com/fda/openfda",
    python_requires=">=3.6",
    install_requires=[
        "arrow",
        "boto",
        "click",
        "elasticsearch<=7.15.1",
        "leveldb",
        "luigi<=2.8.13",
        "lxml",
        "mock<=2.0.0",
        "nose2[coverage_plugin]",
        "python-gflags",
        "requests",
        "setproctitle",
        "simplejson",
        "xmltodict",
        "dictsearch",
        "usaddress",
        "python-crfsuite==0.9.8",
    ],
    dependency_links=[
        "file://"
        + os.path.join(os.getcwd(), "dependencies", "python-crfsuite-0.9.8.tar.gz")
        + "#python_crfsuite-0.9.8"
    ],
    description=(
        "A research project to provide open APIs, raw data downloads, "
        "documentation and examples, and a developer community for an "
        "important collection of FDA public datasets."
    ),
    packages=[
        "openfda",
        "openfda.annotation_table",
        "openfda.faers",
        "openfda.spl",
    ],
    zip_safe=False,
    test_suite="nose2.collector.collector",
)
