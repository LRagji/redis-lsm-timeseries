FROM node:lts-alpine3.14
WORKDIR /usr/src/app
COPY timeseries.js ./
COPY lua-scripts ./lua-scripts/
COPY examples/wrap-it-into-microservice/ ./examples/wrap-it-into-microservice/
WORKDIR /usr/src/app/examples/wrap-it-into-microservice/
RUN npm install
EXPOSE 3000
CMD [ "node", "service.js" ]