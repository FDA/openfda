FROM python:2
RUN curl -sL https://deb.nodesource.com/setup_14.x | bash -
RUN apt-get install -y nodejs netcat
WORKDIR /usr/src/openfda
ADD . ./
RUN rm -rf .eggs _python-env openfda.egg-info logs
RUN ./bootstrap.sh
CMD ["./scripts/all-pipelines-docker.sh"]
