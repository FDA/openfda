openFDA
=======

# Medullan Disclaimer

This repository was forked from https://github.com/FDA/openfda and is being modified by Medullan.
The goal is to add enhancements that Medullan would like to use into the openFDA API with the intention that the changes will make it into the official openFDA build.

*The Build Status below has been changed to point to Medullan's Travis CI instance which builds this forked repository.*



[![Build Status](https://api.travis-ci.org/medullan/openfda.svg?branch=master)](https://api.travis-ci.org/medullan/openfda)

openFDA is a research project to provide open APIs, raw data downloads, documentation and examples, and a developer community for an important collection of FDA public datasets.

*Please note that openFDA is a beta research project and not for clinical use. While we make every effort to ensure that data and logic are accurate, you should assume all results are unvalidated.*

# Contents

This repository contains the code which powers the `api.fda.gov/drug/event.json`, `api.fda.gov/drug/enforcement.json`, `api.fda.gov/device/enforcement.json`, and `api.fda.gov/food/enforcement.json` end points:

* A python pipeline written with [Luigi](https://github.com/spotify/luigi) for processing data for each endpoint
 * The `event.json` pipeline processes the public [AERS SGML](http://www.fda.gov/Drugs/GuidanceComplianceRegulatoryInformation/Surveillance/AdverseDrugEffects/ucm083765.htm) and [FAERS XML](http://www.fda.gov/Drugs/GuidanceComplianceRegulatoryInformation/Surveillance/AdverseDrugEffects/ucm082193.htm) Quarterly Data Extracts into an Adverse Event JSON format that can be loaded into Elasticsearch. This process includes de-duping follow-up reports and adding an `openfda` section to the drug section of the JSON containing additional drug information from other data sources (including FDA, NLM and VA).
 * The `enforcement.json` pipeline process the public [RES HTML Enforcement Reports](http://www.fda.gov/Safety/Recalls/EnforcementReports/2004/ucm120330.htm) and [RES XML Enforcement Reports](http://www.accessdata.fda.gov/scripts/enforcement/enforce_rpt-Product-Tabs.cfm?xml) into an Enforcement Report JSON format that can be loaded into Elasticsearch. The process includes extracting drug identifiers such as NDC and UPC from free text fields and adding an `openfda` section to the JSON containing additional drug information from other data sources (including FDA, NLM and VA).

* [Elasticsearch](http://www.elasticsearch.org/) schemas for the Adverse Event and Enforcement Report JSON formats.

* A [Node.js](https://github.com/joyent/node) API Server written with [Express](http://expressjs.com/), [Elasticsearch.js](http://www.elasticsearch.org/guide/en/elasticsearch/client/javascript-api/current/) and [Elastic.js](http://www.fullscale.co/elasticjs/) that communicates with Elasticsearch and serves the public end points (documented in detail at http://open.fda.gov).

# Prerequisites

* Elasticsearch 1.2.0 or later
* Python 2.7.*
* Node 0.10.*

# Packaging

Run `bootstrap.sh` to download and setup a virtualenv for the `openfda` python package and to download and setup the `openfda-api` node package.
