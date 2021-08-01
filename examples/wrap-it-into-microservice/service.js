//This is just a sample microservice
const express = require('express')
const redisType = require("ioredis");
const timeseriesType = require("../../index").Timeseries;
const localRedisConnectionString = "redis://127.0.0.1:6379/";
const redisClient = new redisType(localRedisConnectionString);
const store = new timeseriesType(redisClient);
const app = express()
const port = 3000

app.use(express.json());
app.post('/set', (req, res) => {
    const data = new Map();
    Object.keys(req.body).forEach((tagName) => {
        const orderedData = new Map();
        Object.keys(req.body[tagName]).forEach((time) => {
            orderedData.set(BigInt(time), req.body[tagName][time]);
        });
        data.set(tagName, orderedData);
    });
    store.write(data)
        .then(result => res.status(204).json(result))
        .catch(error => res.status(500).json(error));
});

app.post('/get', (req, res) => {
    const ranges = new Map();
    Object.keys(req.body).forEach((tagName) => {
        const range = {};
        range.start = BigInt(req.body[tagName].start);
        range.end = BigInt(req.body[tagName].end);
        ranges.set(tagName, range);
    });
    readData(ranges)
        .then(result => res.json(result))
        .catch(error => res.status(500).json(JSON.stringify(error)));
});

async function readData(ranges) {
    //READ Indexes
    const pages = await store.readIndex(ranges);

    //READ Pages
    let asyncCommands = [];
    pages.forEach((pages, partitionName) => {
        pages.forEach((page) => {
            asyncCommands.push((async () => {
                const sortedMap = await store.readPage(page.page, page.start, page.end);
                return new Map([[partitionName, sortedMap]]);
            })());
        });
    });
    let queryResults = await Promise.allSettled(asyncCommands);
    const result = new Map();
    queryResults.reverse().forEach((e) => {
        if (e.status !== "fulfilled") {
            throw new Error(e.reason);
        }
        e = e.value;
        const partitionName = Array.from(e.keys())[0];
        const existingData = result.get(partitionName) || new Map();
        e.get(partitionName).forEach((v, k) => existingData.set(k, v));
        result.set(partitionName, existingData);
    });
    let returnObject = {};
    result.forEach((v, k) => {
        returnObject[k] = Object.fromEntries(v);
    });
    return returnObject;
}


//Startup
(async () => {
    return await store.initialize();
})()
    .then(epoch => {
        app.listen(port, () => {
            console.log(`Example app listening at http://localhost:${port}`)
        })
    });
