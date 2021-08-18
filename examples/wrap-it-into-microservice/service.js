//This is just a sample microservice
const express = require('express')
const compression = require('compression')
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
const brokerType = require('redis-streams-broker').StreamChannelBroker;
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
                const sortedMap = await store.readPage(page.page, (sortKey) => page.start <= sortKey && sortKey <= page.end);
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
        element.data = store.parsePurgePayload(element.raw);
        let fileData = "";
        const entryTime = Date.now();
        element.data.data.forEach((v, k) => {
            fileData += `\r\n${k},${entryTime},${Buffer.from(String(v)).toString("base64")}`;
        });
        await fs.appendFile(path.join(__dirname, "/raw-db/", element.data.partition + ".txt"), fileData);
        await store.purgeAck(element.id, element.data.partition, element.data.key);
        await element.markAsRead(true);
        return element.data.data.size;
    });
    let samplesWritten = await Promise.all(multiWrites);
    samplesWritten = samplesWritten.reduce((acc, e) => acc + e);
    console.log(`<= T:${Date.now() - time} P:${payloads.length} S:${samplesWritten} #`);
    //await store.purgeScan(5000, 5000);
}

let previousDate = Date.now();
let previousDBSize = 0;

//Startup
(async () => {
    await store.initialize();
    const consumerName = `C-${store.instanceName}`;
    if (process.argv[2] === "Mute") {
        const redisClientBroker = new redisType(localRedisConnectionString);
        const broker = new brokerType(redisClientBroker, store.purgeQueName);
        const consumerGroup = await broker.joinConsumerGroup("MyGroup");
        await consumerGroup.subscribe(consumerName, newMessageHandler, HotHoldTime / 4, 1000);

        //Push for completed
        setInterval(async () => {
            let time = Date.now();
            const dbSize = await redisClient.dbsize();
            let qued = await store.purgeScan(HotHoldTime + 3000, 10000);
            let partitionPurgeRate = "";
            if (previousDBSize != 0) {
                partitionPurgeRate = ((dbSize - previousDBSize) / (time - previousDate)) * HotHoldTime
            }
            previousDBSize = dbSize;
            previousDate = time;
            console.log(`=> T:${Date.now() - previousDate} P:${qued.length} Rate:${partitionPurgeRate}`);
        }, (HotHoldTime / 4) + 3000);

        console.log(`${consumerName} is running in muted mode.`);
        return null
    }
    else {
        return consumerName;
    }

})()
    .then(consumerName => {
        if (consumerName != null) {
            app.listen(port, () => {
                console.log(`${consumerName} listening at http://localhost:${port}`);
            });
        }
    });
