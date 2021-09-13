const { parentPort, workerData } = require('worker_threads');
const redisType = require("ioredis");
const fs = require('fs').promises;
const path = require('path');
const timeseriesType = require("../../timeseries");
const config = require("./config");
const scripto = require('redis-scripto2');

async function mainPurgeLoop(storeInfo) {
    const timeout = 60;
    const reTryTimeout = 60 * 10;//10Mins
    const coolDownTime = 2000;
    const processInOneLoop = 5;
    const store = new timeseriesType(config.tagToPartitionMapping, config.partitionToRedisMapping, config.tagNameToTagId, config.settings);
    const scriptManager = new scripto(new redisType(storeInfo.hot));

    let shutdown = false;
    let pullcounter = 0;
    while (shutdown === false) {
        const startTime = Date.now();
        try {
            let logger = [startTime];
            let totalSamples = 0.0;
            const acquiredPartitions = await store.purgeAcquire(scriptManager, timeout, (2000 * 60), reTryTimeout, 1);
            if (acquiredPartitions.length === 0) {
                pullcounter = processInOneLoop + 1;
            }
            logger.push(Date.now());
            for (let index = 0; index < acquiredPartitions.length; index++) {
                const partitionInfo = acquiredPartitions[index];
                let fileData = "";
                partitionInfo.data.forEach((timeMap, tagId) => {
                    timeMap.forEach((sample, time) => {
                        fileData += `\r\n${tagId},${time},${startTime},${Buffer.from(String(sample)).toString("base64")}`;
                        totalSamples++;
                    })
                });
                await fs.appendFile(path.join(__dirname, storeInfo.cold, partitionInfo.releaseToken + ".txt"), fileData);
                logger.push(Date.now());
                const result = await store.purgeRelease(scriptManager, partitionInfo.releaseToken);
                if (result !== true) {
                    throw new Error(`Ack failed! ${partitionInfo.releaseToken}.`);
                }
                logger.push(Date.now());
            }
            logger.push(Date.now());
            const elapsed = Date.now() - startTime;
            console.log(`=> T:${logger.reduce((acc, e, idx, arr) => acc.toString() + "," + (arr[idx] - arr[idx - 1]).toString())} P:${acquiredPartitions.length} S:${totalSamples} Rate:${((totalSamples / (elapsed / 1000))).toFixed(2)}/sec`);
        }
        catch (err) {
            console.error(err);
        }
        finally {
            pullcounter++;
            if (pullcounter > processInOneLoop) {
                pullcounter = 0;
                //Killtime
                await new Promise((acc, rej) => setTimeout(acc, coolDownTime));
            }

        }
    }
}

mainPurgeLoop(workerData)
    .then(parentPort.postMessage);