FROM node:14

ADD . /app
WORKDIR /app

EXPOSE 8000

RUN npm install pm2 -g
RUN npm ci
CMD ["pm2-runtime", "api.js"]
