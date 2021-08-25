const { parentPort, workerData } = require('worker_threads');
const redisType = require("ioredis");
const fs = require('fs').promises;
const path = require('path');
const timeseriesType = require("../../index").Timeseries;

async function mainSyncLoop(config) {
    const HotHoldTime = config.HotHoldTime;
    const holdTimeInSeconds = HotHoldTime / 1000;
    const coolDownTime = config.coolDownTime;
    const processInOneLoop=config.processInOneLoop;
    const localRedisConnectionString = config.redisConnectionString;
    const redisClient = new redisType(localRedisConnectionString);
    const store = new timeseriesType(redisClient);
    await store.initialize();

    let shutdown = false;
    while (shutdown === false) {
        const startTime = Date.now();
        try {
            let summedAverage = 0.0;
            const acquiredPartitions = await store.purgeAcquire((holdTimeInSeconds + 3), processInOneLoop, holdTimeInSeconds * 3);
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