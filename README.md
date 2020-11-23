openFDA
=======

[![Build Status](https://travis-ci.org/FDA/openfda.svg?branch=master)](https://travis-ci.org/FDA/openfda)

openFDA is a research project to provide open APIs, raw data downloads, documentation and examples, and a developer community for an important collection of FDA public datasets.

*Please note: Do not rely on openFDA to make decisions regarding medical care. Always speak to your health provider about the risks and benefits of FDA-regulated products. We may limit or otherwise restrict your access to the API in line with our [Terms of Service](https://open.fda.gov/terms/).*

# Contents

This repository contains the code which powers all of the `api.fda.gov` end points:

* Python pipelines written with [Luigi](https://github.com/spotify/luigi) for processing public FDA data sets (drugs, foods, medical devices, and other) into a JSON format that can be loaded into Elasticsearch.

* [Elasticsearch](http://www.elasticsearch.org/) schemas for the available data sets.

* A [Node.js](https://github.com/joyent/node) API Server written with [Express](http://expressjs.com/), [Elasticsearch.js](http://www.elasticsearch.org/guide/en/elasticsearch/client/javascript-api/current/) and [Elastic.js](http://www.fullscale.co/elasticjs/) that communicates with Elasticsearch and provides the `api.fda.gov` JSON interface (documented in detail at https://open.fda.gov).

# Prerequisites

* Elasticsearch 5.6
* Python 3
* Node 14 or above

# Packaging

Run `bootstrap.sh` to download and setup a virtualenv for the `openfda` python package and to download and setup the `openfda-api` node package.

# Running in Docker

If you intend to try and run openFDA yourself, we have put together a `docker-compose.yml` configuration
 that can help you get started. `docker-compose up` will:
1. Start an [Elasticsearch](http://www.elasticsearch.org/) container
2. Start an API container, which will expose port `8000` for queries.
3. Start a Python 3 container that will run the NSDE, CAERS, and Substance Data pipelines and
create corresponding indices in Elasticsearch.

Note: even though the API container starts right away, it will not serve any data until some or all
of the pipelines above have finished running. You can `curl http://localhost:8000/status` to see which
endpoints have become available as the pipelines progress or after they have completed running. Once an
endpoint becomes available, it can be queried using the standard openFDA
[query syntax](https://open.fda.gov/apis/query-syntax/).
For example: `curl -g 'http://localhost:8000/food/event.json?search=products.industry_name:"Soft+Drink/Water"+AND+reactions.exact:DEHYDRATION&limit=10'`

At this point the Python container only runs the NSDE, CAERS, and Substance Data pipelines because those
are relatively lightweight and require no access to internal FDA networks. We will add more pipelines
in case there is substantial interest from the community. However, the three pipelines above provide a good starting
point into understanding openFDA internals and/or customizing openFDA.

## Windows Users

Clone the repository with `git clone https://github.com/FDA/openfda.git --config core.autocrlf=input` in order to circumvent docker issues with building images on a Windows computer.
