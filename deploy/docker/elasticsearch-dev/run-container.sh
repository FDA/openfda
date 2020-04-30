#!/bin/bash

docker stop elasticsearch-dev
docker rm -f elasticsearch-dev
docker rmi -f openfda/elasticsearch.0

set -x
set -e

sudo docker build -t openfda/elasticsearch.0 .
sudo docker run \
  -d \
  -v /media/ebs/:/data0\
  -p 9200:9200\
  -p 9300:9300\
  -e AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID"\
  -e AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY"\
  --name elasticsearch-dev\
  openfda/elasticsearch.0\
  /elasticsearch/bin/elasticsearch\
  -Des.insecure.allow.root=true\
  -Daws.accessKeyId="$AWS_ACCESS_KEY_ID"\
  -Daws.secretKey="$AWS_SECRET_ACCESS_KEY"
