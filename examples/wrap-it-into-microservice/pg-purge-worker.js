const { parentPort, workerData } = require('worker_threads');
const redisType = require("ioredis");
const fs = require('fs').promises;
const path = require('path');
const timeseriesType = require("../../timeseries");
const config = require("./config");
const scripto = require('redis-scripto2');
const pgp = require('pg-promise')({
    /* initialization options */
    capSQL: true // capitalize all generated SQL
});


async function mainPurgeLoop(storeInfo) {
    const timeout = 60;
    const reTryTimeout = 60 * 10;//10Mins
    const coolDownTime = 2000;
    const processInOneLoop = 100;
    const store = new timeseriesType(config.tagToPartitionMapping, config.partitionToRedisMapping, config.tagNameToTagId, config.settings);
    const scriptManager = new scripto(new redisType(storeInfo.hot));
    const db = pgp(storeInfo.cold);
    const columnDef = new pgp.helpers.ColumnSet(['id', 'start', 'blob'], { table: 'raw' });

    let shutdown = false;
    while (shutdown === false) {
        const startTime = Date.now();
        try {
            let totalSamples = 0.0;
            const data = [];
            const acquiredPartitions = await store.purgeAcquire(scriptManager, timeout, (2000 * 60), reTryTimeout, processInOneLoop);
            for (let index = 0; index < acquiredPartitions.length; index++) {
                const partitionInfo = acquiredPartitions[index];
                let fileData = "";
                partitionInfo.data.forEach((timeMap, tagId) => {
                    timeMap.forEach((sample, time) => {
                        fileData += `\r\n${tagId},${time},${startTime},${Buffer.from(String(sample)).toString("base64")}`;
                        totalSamples++;
                    })
                });
                //await fs.appendFile(path.join(__dirname, "/raw-db/", partitionInfo.releaseToken + ".txt"), fileData);
                data.push({ "id": BigInt(partitionInfo.name), "start": partitionInfo.start, "blob": Buffer.from(fileData) });
                await db.none(pgp.helpers.insert(data, columnDef));
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

mainPurgeLoop(workerData)
    .then(parentPort.postMessage);