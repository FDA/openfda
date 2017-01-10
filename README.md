openFDA
=======

[![Build Status](https://travis-ci.org/FDA/openfda.svg?branch=master)](https://travis-ci.org/FDA/openfda)

openFDA is a research project to provide open APIs, raw data downloads, documentation and examples, and a developer community for an important collection of FDA public datasets.

*Please note: Do not rely on openFDA to make decisions regarding medical care. Always speak to your health provider about the risks and benefits of FDA-regulated products. We may limit or otherwise restrict your access to the API in line with our [Terms of Service](https://open.fda.gov/terms/).*

# Contents

This repository contains the code which powers all of the `api.fda.gov` end points:

* Python pipelines written with [Luigi](https://github.com/spotify/luigi) for processing public FDA data sets (drugs, foods, and medical devices) into a JSON format that can be loaded into Elasticsearch. 

* [Elasticsearch](http://www.elasticsearch.org/) schemas for the available data sets.

* A [Node.js](https://github.com/joyent/node) API Server written with [Express](http://expressjs.com/), [Elasticsearch.js](http://www.elasticsearch.org/guide/en/elasticsearch/client/javascript-api/current/) and [Elastic.js](http://www.fullscale.co/elasticjs/) that communicates with Elasticsearch and provides the `api.fda.gov` JSON interface (documented in detail at http://open.fda.gov).

# Prerequisites

* Elasticsearch 1.7.0 or later
* Python 2.7.*
* Node 0.10.*

# Packaging

Run `bootstrap.sh` to download and setup a virtualenv for the `openfda` python package and to download and setup the `openfda-api` node package.
