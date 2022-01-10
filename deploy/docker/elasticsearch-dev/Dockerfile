# This is based on the ElasticSearch template Dockerfile
# (https://registry.hub.docker.com/u/dockerfile/elasticsearch/dockerfile/)
#
# It has been modified to support multiple data directories

FROM docker.elastic.co/elasticsearch/elasticsearch:7.10.2

ADD elasticsearch.yml /usr/share/elasticsearch/config/
ADD log4j2.properties /usr/share/elasticsearch/config/

# Install AWS plugins
# RUN bin/elasticsearch-plugin install discovery-ec2
RUN bin/elasticsearch-plugin install -b repository-s3
RUN bin/elasticsearch-plugin list


USER root
RUN chown elasticsearch:elasticsearch config/elasticsearch.yml
RUN chown elasticsearch:elasticsearch config/log4j2.properties
USER elasticsearch

ENV ES_JAVA_OPTS ""
ENV MAX_LOCKED_MEMORY unlimited

VOLUME ["/data0"]
