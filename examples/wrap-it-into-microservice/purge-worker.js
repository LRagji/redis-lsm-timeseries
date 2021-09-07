const { parentPort, workerData } = require('worker_threads');
const redisType = require("ioredis");
const fs = require('fs').promises;
const path = require('path');
const timeseriesType = require("../../timeseries");
const scripto = require('redis-scripto2');
const hash = (tagName) => Array.from(tagName).reduce((acc, c) => acc + c.charCodeAt(0), 0);
const tagToPartitionMapping = (tagName) => hash(tagName) % 10;
//const partitionToRedisMapping = (partitionName) => redisClient;
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

async function mainSyncLoop(config) {
    const HotHoldTime = config.HotHoldTime;
    const holdTimeInSeconds = HotHoldTime / 1000;
    const coolDownTime = config.coolDownTime;
    const processInOneLoop = config.processInOneLoop;
    const localRedisConnectionString = config.redisConnectionString;
    const redisClient = new redisType(localRedisConnectionString);
    const store = new timeseriesType(tagToPartitionMapping, (partitionName) => redisClient, tagNameToTagId, settings);
    const scriptManager = new scripto(redisClient);
    //await store.initialize();

    let shutdown = false;
    while (shutdown === false) {
        const startTime = Date.now();
        try {
            let summedAverage = 0.0;
            const acquiredPartitions = await store.purgeAcquire(scriptManager, -1, 10000, (holdTimeInSeconds * 3), processInOneLoop);
            acquiredPartitions.forEach((timeMap, tagId) => {

            });


            for (let index = 0; index < acquiredPartitions.length; index++) {
                const partitionInfo = acquiredPartitions[index];
                let fileData = "";
                partitionInfo.data.forEach((v, k) => {
                    fileData += `\r\n${k},${startTime},${Buffer.from(String(v)).toString("base64")}`;
                });
                await fs.appendFile(path.join(__dirname, "/raw-db/", partitionInfo.key + ".txt"), fileData);
                const releaseMessage = await store.purgeRelease(partitionInfo.name, partitionInfo.key, partitionInfo.releaseToken);
                summedAverage += releaseMessage.rate;
            }
            console.log(`=> T:${Date.now() - startTime} P:${acquiredPartitions.length} Rate:${(summedAverage / acquiredPartitions.length).toFixed(2)}`);
        }
        catch (err) {
            console.error(err);
        }
        finally {
            //Killtime
            await new Promise((acc, rej) => setTimeout(acc, coolDownTime));
        }
    }
}

mainSyncLoop(workerData)
    .then(parentPort.postMessage);