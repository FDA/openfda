openFDA
=======

[![Build Status](https://travis-ci.org/FDA/openfda.svg?branch=master)](https://travis-ci.org/FDA/openfda)

openFDA is a research project to provide open APIs, raw data downloads, documentation and examples, and a developer community for an important collection of FDA public datasets.

*Please note: Do not rely on openFDA to make decisions regarding medical care. Always speak to your health provider about the risks and benefits of FDA-regulated products. We may limit or otherwise restrict your access to the API in line with our [Terms of Service](https://open.fda.gov/terms/).*

# Contents

This repository contains the code which powers the `api.fda.gov/drug/event.json` end point:

* A python pipeline written with [Luigi](https://github.com/spotify/luigi) for processing the public [AERS SGML](http://www.fda.gov/Drugs/GuidanceComplianceRegulatoryInformation/Surveillance/AdverseDrugEffects/ucm083765.htm) and [FAERS XML](http://www.fda.gov/Drugs/GuidanceComplianceRegulatoryInformation/Surveillance/AdverseDrugEffects/ucm082193.htm) Quarterly Data Extracts into an Adverse Event JSON format that can be loaded into Elasticsearch. This process includes de-duping follow-up reports and adding an `openfda` section to the drug section of the JSON containing additional drug information from other data sources (including FDA, NLM and VA).

* An [Elasticsearch](http://www.elasticsearch.org/) schema for the Adverse Event JSON format.

* A [Node.js](https://github.com/joyent/node) API Server written with [Express](http://expressjs.com/), [Elasticsearch.js](http://www.elasticsearch.org/guide/en/elasticsearch/client/javascript-api/current/) and [Elastic.js](http://www.fullscale.co/elasticjs/) that communicates with Elasticsearch and provides the `api.fda.gov/drug/event.json` interface (documented in detail at http://open.fda.gov).

# Prerequisites

* Elasticsearch 1.2.0 or later
* Python 2.7.*
* Node 0.10.*

# Packaging

Run `bootstrap.sh` to download and setup a virtualenv for the `openfda` python package and to download and setup the `openfda-api` node package.
