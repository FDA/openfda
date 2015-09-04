FROM  registry.hub.docker.com/node:0.10

ADD . /app
WORKDIR /app

EXPOSE 8000
RUN rm -rf node_modules
RUN npm install
CMD ["node","./node_modules/.bin/forever","api.js"]
