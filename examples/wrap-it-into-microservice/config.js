const redisType = require("ioredis");
const scripto = require('redis-scripto2');
const path = require('path');
const tags = new Map();
let stores = [
    //{ "hot": "redis://127.0.0.1:6379/", "cold": "/raw-db/" },
    //{ "hot": "redis://127.0.0.1:6379/", "cold": "postgres://postgres:@localhost:5432/Data" },
];
const maximumNumberOfPartitions = 10;

async function hash(tagName) {
    const existingId = tags.get(tagName);
    if (existingId == null) {
        const hash = await new Promise((acc, rej) => {
            coordinator.run("hash", ["List"], [tagName], (err, result) => {
                if (err !== null) {
                    return rej(err);
                }
                acc(result);
            });
        });
        console.log(`${tagName} ${hash}`);
        tags.set(tagName, parseInt(hash));
        return tags.get(tagName);
    }
    else {
        return existingId;
    }
    //return Array.from(tagName).reduce((acc, c) => acc + c.charCodeAt(0), 0);
}

async function redisShard(partitionName) {
    const index = await hash(partitionName) % shards.length;
    return shards[index].hot;
}

async function tagGrouping(tagName) {
    return await hash(tagName) % maximumNumberOfPartitions;
}

const hotStores = (process.env.hot || process.env.HOT).split(' ');
const coldStores = (process.env.cold || process.env.COLD).split(' ');
const coordinator = new scripto(new redisType(process.env.coo || process.env.COO));
coordinator.loadFromDir(path.join(__dirname, "lua"));
stores = hotStores.map((h, idx) => ({ "hot": h, "cold": coldStores[idx] }));
const shards = stores.map(storeInfo => ({ "hot": new redisType(storeInfo.hot), "cold": storeInfo.cold }));

module.exports = {
    "stores": stores,
    "mode": (process.env.mode || process.env.MODE),
    "tagNameToTagId": hash,
    "tagToPartitionMapping": tagGrouping,
    "partitionToRedisMapping": redisShard,
    "settings": {
        "ActivityKey": "Activity",
        "SamplesPerPartitionKey": "Stats",
        "PurgePendingKey": "Pending",
        "Seperator": "=",
        "InputRatePerf": "I",
        "OutputRatePerf": "O",
        "InputRateTime": "IT",
        "OutputRateTime": "OT",
        "MaximumTagsInOneWrite": 2000,
        "MaximumTagsInOneRead": 100,
        "MaximumTagNameLength": 200,
        "MaximumPartitionsScansInOneRead": 100,
        "PartitionTimeWidth": 60000,
        "PurgeMarker": "P"
    }
}