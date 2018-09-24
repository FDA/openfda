# This is based on the ElasticSearch template Dockerfile
# (https://registry.hub.docker.com/u/dockerfile/elasticsearch/dockerfile/)
#
# It has been modified to support multiple data directories

FROM openjdk:8

ENV DEBIAN_FRONTEND noninteractive

RUN apt-get -y update
RUN apt-get -y install wget curl

RUN \
  cd /tmp && \
  wget https://download.elastic.co/elasticsearch/release/org/elasticsearch/distribution/tar/elasticsearch/2.4.6/elasticsearch-2.4.6.tar.gz && \
  tar xvzf elasticsearch-2.4.6.tar.gz && \
  rm -f elasticsearch-2.4.6.tar.gz && \
  mv /tmp/elasticsearch-2.4.6 /elasticsearch

VOLUME ["/data0", "/data1", "/data2", "/data3"]

WORKDIR /elasticsearch

# Install S3 plugin
RUN bin/plugin install cloud-aws
RUN bin/plugin install mobz/elasticsearch-head

ADD elasticsearch.yml /elasticsearch/config/elasticsearch.yml
ADD logging.yml /elasticsearch/config/logging.yml

# Define default command.
CMD ["/elasticsearch/bin/elasticsearch", "-Des.insecure.allow.root=true"]

# Expose ports.
#   - 9200: HTTP
#   - 9300: transport
EXPOSE 9200
EXPOSE 9300

# Give ES more memory
ENV ES_HEAP_SIZE 15g
ENV MAX_LOCKED_MEMORY unlimited
