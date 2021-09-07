//This is just a sample microservice
const express = require('express')
const compression = require('compression')
const path = require('path');
const { Worker } = require('worker_threads');
const redisType = require("ioredis");
const timeseriesType = require("../../timeseries");
const localRedisConnectionString = "redis://127.0.0.1:6379/";
const redisClient = new redisType(localRedisConnectionString);
const hash = (tagName) => Array.from(tagName).reduce((acc, c) => acc + c.charCodeAt(0), 0);
const tagToPartitionMapping = (tagName) => hash(tagName) % 10;
const partitionToRedisMapping = (partitionName) => redisClient;
const tagNameToTagId = hash;
const settings = {
    "ActivityKey": "Activity",
    "SamplesPerPartitionKey": "Stats",
    "Seperator": "=",
    "MaximumTagsInOneWrite": 2000,
    "MaximumTagsInOneRead": 100,
    "MaximumTagNameLength": 200,
    "MaximumPartitionsScansInOneRead": 100,
    "PartitionTimeWidth": 5000,
    "PurgeMarker": "P"
}
const store = new timeseriesType(tagToPartitionMapping, partitionToRedisMapping, tagNameToTagId, settings);
const app = express()
const port = 3000
const HotHoldTime = 120 * 1000;

app.use(express.json());

app.use(compression());


app.post('/set', async (req, res) => {
    try {
        const data = new Map();
        Object.keys(req.body).forEach((tagName) => {
            const orderedData = new Map();
            Object.keys(req.body[tagName]).forEach((time) => {
                orderedData.set(BigInt(time), req.body[tagName][time]);
            });
            data.set(tagName, orderedData);
        });
        const response = await store.write(data);
        if (response === true) {
            res.status(204).end();
        }
        else {
            res.status(400).json({ "error": "Failed to save data." });
        }
    }
    catch (err) {
        res.status(500).json(err.stack);
    }
});

app.post('/get', (req, res) => {
    const ranges = new Map();
    Object.keys(req.body).forEach((tagName) => {
        const range = {};
        range.start = BigInt(req.body[tagName].start);
        range.end = BigInt(req.body[tagName].end);
        ranges.set(tagName, range);
    });
    store.read(ranges)
        .then(result => res.json(result))
        .catch(error => res.status(500).json(JSON.stringify(error)));
});

// async function readData(ranges) {
//     //READ Indexes
//     const pages = await store.readIndex(ranges);
//     const tagNames = Array.from(ranges.keys());

//     //READ Pages
//     let asyncCommands = [];
//     pages.forEach((pages, partitionName) => {
//         pages.forEach((page) => {
//             asyncCommands.push((async () => {
//                 const sortedMap = await store.readPage(page.page, (sortKey, tagName) => tagNames.indexOf(tagName) > -1 && page.start <= sortKey && sortKey <= page.end);
//                 return new Map([[partitionName, sortedMap]]);
//             })());
//         });
//     });
//     let queryResults = await Promise.allSettled(asyncCommands);
//     const result = new Map();
//     queryResults.reverse().forEach((e) => {
//         if (e.status !== "fulfilled") {
//             throw new Error(e.reason);
//         }
//         e = e.value;
//         const partitionName = Array.from(e.keys())[0];
//         const existingData = result.get(partitionName) || new Map();
//         e.get(partitionName).forEach((v, k) => existingData.set(k, v));
//         result.set(partitionName, existingData);
//     });
//     let returnObject = {};
//     result.forEach((v, k) => {
//         returnObject[k] = Object.fromEntries(v);
//     });
//     return returnObject;
// }

//Startup
(async () => {
    //await store.initialize();

    const consumerName = `C-${store.instanceName}`;
    app.listen(port, () => {
        console.log(`${consumerName} listening at http://localhost:${port}`);
    });

    // await new Promise((resolve, reject) => {
    //     const worker = new Worker(path.join(__dirname, "purge-worker.js"), {
    //         workerData: {
    //             "HotHoldTime": HotHoldTime,
    //             "coolDownTime": 5000,
    //             "processInOneLoop": 200,
    //             "redisConnectionString": localRedisConnectionString
    //         }
    //     });
    //     worker.on('message', resolve);
    //     worker.on('error', reject);
    //     worker.on('exit', (code) => {
    //         if (code !== 0)
    //             reject(new Error(`Worker stopped with exit code ${code}`));
    //     });
    // });

})()
    .then(consumerName => {
        console.log("Program Exit");
    });
