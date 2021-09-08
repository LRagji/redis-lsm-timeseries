//This is just a sample microservice
const express = require('express')
const compression = require('compression')
const path = require('path');
const { Worker } = require('worker_threads');
const config = require("./config");
const timeseriesType = require("../../timeseries");
const store = new timeseriesType(config.tagToPartitionMapping, config.partitionToRedisMapping, config.tagNameToTagId, config.settings);
const app = express()
const port = 3000

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
            console.error("400:Response Failed: " + response);
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

//Startup
(async () => {
    const consumerName = `C-${store.instanceHash}`;
    app.listen(port, () => {
        console.log(`${consumerName} listening at http://localhost:${port}`);
    });
    const threads = config.shards.map(connectionString =>
        new Promise((resolve, reject) => {
            const worker = new Worker(path.join(__dirname, "purge-worker.js"), {
                workerData: connectionString
            });
            worker.on('message', resolve);
            worker.on('error', reject);
            worker.on('exit', (code) => {
                if (code !== 0)
                    reject(new Error(`Worker stopped with exit code ${code}`));
            });
        }));
    return await Promise.allSettled(threads);
})()
    .then(threadExitData => {
        console.log(threadExitData);
    });
