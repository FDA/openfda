# This is based on the ElasticSearch template Dockerfile
# (https://registry.hub.docker.com/u/dockerfile/elasticsearch/dockerfile/)
#
# It has been modified to support multiple data directories

FROM docker.elastic.co/elasticsearch/elasticsearch:5.6.16

ADD elasticsearch.yml /usr/share/elasticsearch/config/
ADD log4j2.properties /usr/share/elasticsearch/config/

# Remove annoying X-Pack
RUN bin/elasticsearch-plugin remove x-pack

# Install AWS plugins
# RUN bin/elasticsearch-plugin install discovery-ec2
RUN bin/elasticsearch-plugin install repository-s3


USER root
RUN chown elasticsearch:elasticsearch config/elasticsearch.yml
RUN chown elasticsearch:elasticsearch config/log4j2.properties
USER elasticsearch

ENV ES_JAVA_OPTS ""
ENV MAX_LOCKED_MEMORY unlimited
ENV AWS_ACCESS_KEY_ID ""
ENV AWS_SECRET_ACCESS_KEY ""

VOLUME ["/data0"]
