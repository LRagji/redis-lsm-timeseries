//This is just a sample microservice
const express = require('express')
const fs = require('fs').promises;
const path = require('path');
const redisType = require("ioredis");
const timeseriesType = require("../../index").Timeseries;
const localRedisConnectionString = "redis://127.0.0.1:6379/";
const redisClient = new redisType(localRedisConnectionString);
const store = new timeseriesType(redisClient);
const app = express()
const port = 3000
const purgeLimit = 1//1e8;//~200MB
const redisClientBroker = new redisType(localRedisConnectionString);
const brokerType = require('redis-streams-broker').StreamChannelBroker;


app.use(express.json());

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
        const bytes = await store.write(data);
        // if (bytes > BigInt(purgeLimit)) {
        //     await store.purgeScan(5000, 1000);
        // }
        res.status(200).json(bytes.toString());
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

async function newMessageHandler(payloads) {
    let time = Date.now();
    let multiWrites = payloads.map(async element => {
        // element.data = store.parsePurgePayload(element.raw);
        // let fileData = "";
        // const entryTime = Date.now();
        // element.data.data.forEach((v, k) => {
        //     fileData += `\r\n${k},${entryTime},${Buffer.from(String(v)).toString("base64")}`;
        // });
        // await fs.appendFile(path.join(__dirname, "/raw-db/", element.data.partition + ".txt"), fileData);
        await store.purgeAck(element.id);
        await element.markAsRead(true);
    });
    await Promise.all(multiWrites);
    console.log(`<= T:${Date.now() - time} P:${payloads.length} #`);
    //await store.purgeScan(5000, 5000);
}

//Startup
(async () => {
    await store.initialize(30000);
    const consumerName = `C-${store.instanceName}`;
    const broker = new brokerType(redisClientBroker, store.purgeQueName);
    const consumerGroup = await broker.joinConsumerGroup("MyGroup");
    await consumerGroup.subscribe(consumerName, newMessageHandler, 15000, 1);
    return consumerName;
})()
    .then(consumerName => {
        if (process.argv[2] === "Mute") {
            console.log(`${consumerName} is running in muted mode.`)
        }
        else {
            app.listen(port, () => {
                console.log(`${consumerName} listening at http://localhost:${port}`);
                setInterval(async () => {
                    let time = Date.now();
                    let qued = await store.purgeScan(35000, 10);
                    console.log(`=> T:${Date.now() - time} P:${qued.length}`);
                }, 15000);
            })
        }
    });
