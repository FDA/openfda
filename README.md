openFDA
=======

openFDA is a research project to provide open APIs, raw data downloads, documentation and examples, and a developer community for an important collection of FDA public datasets.

*Please note that openFDA is a beta research project and not for clinical use. While we make every effort to ensure that data and logic are accurate, you should assume all results are unvalidated.*

# Contents

This repository contains the code which powers the `api.fda.gov/drug/event.json` end point:

* A python pipeline written with [Luigi](https://github.com/spotify/luigi) for processing the public [AERS SGML](http://www.fda.gov/Drugs/GuidanceComplianceRegulatoryInformation/Surveillance/AdverseDrugEffects/ucm083765.htm) and [FAERS XML](http://www.fda.gov/Drugs/GuidanceComplianceRegulatoryInformation/Surveillance/AdverseDrugEffects/ucm082193.htm) Quarterly Data Extracts into an Adverse Event JSON format that can be loaded into Elasticsearch. This process includes de-duping follow-up reports and adding an `openfda` section to the drug section of the JSON containing additional drug information from other data sources (including FDA, NLM and VA).

* An [Elasticsearch](http://www.elasticsearch.org/) schema for the Adverse Event JSON format.

* A Node.js API Server written with [Express](http://expressjs.com/), [Elasticsearch.js](http://www.elasticsearch.org/guide/en/elasticsearch/client/javascript-api/current/) and [Elastic.js](http://www.fullscale.co/elasticjs/) that communicates with Elasticsearch and provides the `api.fda.gov/drug/event.json` interface (documented in detail at http://open.fda.gov).

# Prerequisites

* Elasticsearch 1.2.0 or later
* Python 2.7.*
* Node 0.10.*

# Packaging

Run `bootstrap.sh` to download and setup a virtualenv for the `openfda` python package and to download and setup the `openfda-api` node package.
