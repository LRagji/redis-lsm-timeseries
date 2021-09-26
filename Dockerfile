FROM node:lts-alpine3.14
WORKDIR /usr/src/app
COPY examples/wrap-it-into-microservice/ ./examples/wrap-it-into-microservice/
WORKDIR /usr/src/app/examples/wrap-it-into-microservice/
RUN npm install
EXPOSE 3000
ENTRYPOINT ["node", "service.js"]