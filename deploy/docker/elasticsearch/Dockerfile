# This is based on the ElasticSearch template Dockerfile
# (https://registry.hub.docker.com/u/dockerfile/elasticsearch/dockerfile/)
#
# It has been modified to support multiple data directories

FROM dockerfile/java:oracle-java7

ENV DEBIAN_FRONTEND noninteractive

RUN apt-get -y update
RUN apt-get -y install wget curl

RUN \
  cd /tmp && \
  wget https://download.elasticsearch.org/elasticsearch/elasticsearch/elasticsearch-1.4.0.tar.gz && \
  tar xvzf elasticsearch-1.4.0.tar.gz && \
  rm -f elasticsearch-1.4.0.tar.gz && \
  mv /tmp/elasticsearch-1.4.0 /elasticsearch

VOLUME ["/data0", "/data1", "/data2", "/data3"]

WORKDIR /elasticsearch

# Install S3 plugin
RUN bin/plugin -install elasticsearch/elasticsearch-cloud-aws/2.4.1
RUN bin/plugin -install mobz/elasticsearch-head

# Mount elasticsearch.yml config
ADD elasticsearch.yml /elasticsearch/config/elasticsearch.yml
ADD logging.yml /elasticsearch/config/logging.yml

# Define default command.
CMD ["/elasticsearch/bin/elasticsearch"]

# Expose ports.
#   - 9200: HTTP
#   - 9300: transport
EXPOSE 9200
EXPOSE 9300

# Give ES more memory
ENV ES_HEAP_SIZE 8g
ENV MAX_LOCKED_MEMORY unlimited
