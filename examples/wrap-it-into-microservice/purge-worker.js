const { parentPort, workerData } = require('worker_threads');
const redisType = require("ioredis");
const fs = require('fs').promises;
const path = require('path');
const timeseriesType = require("../../timeseries");
const scripto = require('redis-scripto2');
const hash = (tagName) => Array.from(tagName).reduce((acc, c) => acc + c.charCodeAt(0), 0);
const tagToPartitionMapping = (tagName) => hash(tagName) % 10;
const tagNameToTagId = hash;

async function mainSyncLoop(config) {
    const holdTimeInSeconds = config.HotHoldTime / 1000;
    const coolDownTime = config.coolDownTime;
    const processInOneLoop = config.processInOneLoop;
    const localRedisConnectionString = config.redisConnectionString;
    const redisClient = new redisType(localRedisConnectionString);
    const store = new timeseriesType(tagToPartitionMapping, (partitionName) => redisClient, tagNameToTagId, config.settings);
    const scriptManager = new scripto(redisClient);

    let shutdown = false;
    while (shutdown === false) {
        const startTime = Date.now();
        try {
            let totalSamples = 0.0;
            const acquiredPartitions = await store.purgeAcquire(scriptManager, -1, 10000, (holdTimeInSeconds * 3), processInOneLoop);
            for (let index = 0; index < acquiredPartitions.length; index++) {
                const partitionInfo = acquiredPartitions[index];
                let fileData = "";
                partitionInfo.data.forEach((timeMap, tagId) => {
                    timeMap.forEach((sample, time) => {
                        fileData += `\r\n${tagId},${time},${startTime},${Buffer.from(String(sample)).toString("base64")}`;
                        totalSamples++;
                    })
                });
                await fs.appendFile(path.join(__dirname, "/raw-db/", partitionInfo.releaseToken + ".txt"), fileData);
                const result = await store.purgeRelease(scriptManager, partitionInfo.releaseToken);
                if (result !== true) {
                    throw new Error(`Ack failed! ${partitionInfo.releaseToken}.`);
                }
            }
            const elapsed = Date.now() - startTime;
            console.log(`=> T:${elapsed.toFixed(2)} P:${acquiredPartitions.length} S:${totalSamples} Rate:${((totalSamples / (elapsed / 1000))).toFixed(2)}/sec`);
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