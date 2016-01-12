# This is based on the ElasticSearch template Dockerfile
# (https://registry.hub.docker.com/u/dockerfile/elasticsearch/dockerfile/)
#
# It has been modified to support multiple data directories

FROM registry.hub.docker.com/java:latest

ENV DEBIAN_FRONTEND noninteractive

RUN apt-get -y update
RUN apt-get -y install wget curl

RUN \
  cd /tmp && \
  wget https://download.elasticsearch.org/elasticsearch/elasticsearch/elasticsearch-1.7.1.tar.gz && \
  tar xvzf elasticsearch-1.7.1.tar.gz && \
  rm -f elasticsearch-1.7.1.tar.gz && \
  mv /tmp/elasticsearch-1.7.1 /elasticsearch

VOLUME ["/data0"]

WORKDIR /elasticsearch

# Install S3 plugin
RUN bin/plugin -install elasticsearch/elasticsearch-cloud-aws/2.7.1
RUN bin/plugin -install mobz/elasticsearch-head

ADD elasticsearch.yml /elasticsearch/config/elasticsearch.yml
ADD logging.yml /elasticsearch/config/logging.yml

# Expose ports.
#   - 9200: HTTP
#   - 9300: transport
EXPOSE 9200
EXPOSE 9300

ENV ES_HEAP_SIZE 15g
ENV MAX_LOCKED_MEMORY unlimited

CMD /elasticsearch/bin/elasticsearch
