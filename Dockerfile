FROM python:3.10.19
RUN curl -sL https://deb.nodesource.com/setup_16.x | bash -
RUN apt-get install -y nodejs netcat-openbsd p7zip-full
WORKDIR /usr/src/openfda
ADD . ./
RUN rm -rf .eggs _python-env openfda.egg-info logs
RUN ./bootstrap.sh
CMD ["./scripts/all-pipelines-docker.sh"]
