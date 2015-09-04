#!/bin/bash

set -x
set -e

sudo docker build -t openfda/elasticsearch.0 .
sudo docker run \
  --net=host\
  -d \
  -v /media/ebs/:/data0\
  -p 9200:9200\
  -p 9300:9300\
  --name elasticsearch-dev\
  openfda/elasticsearch.0\
  /elasticsearch/bin/elasticsearch\
  -Daws.accessKeyId="$AWS_ACCESS_KEY_ID"\
  -Daws.secretKey="$AWS_SECRET_ACCESS_KEY"
