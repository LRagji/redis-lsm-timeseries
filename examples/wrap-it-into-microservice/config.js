const redisType = require("ioredis");
const scripto = require('redis-scripto2');
const path = require('path');
const identityCache = new Map();
const partitionsKey = "Partitions";
let stores = [
    //{ "hot": "redis://127.0.0.1:6379/", "cold": "/raw-db/" },
    //{ "hot": "redis://127.0.0.1:6379/", "cold": "postgres://postgres:@localhost:5432/Data" },
];
const MaxTagsPerPartition = 2000;

async function hash(tagName, name = "Tags") {
    const inMemoryKey = `${name}${tagName}`;
    const existingId = identityCache.get(inMemoryKey);
    if (existingId == null) {
        const hash = await new Promise((acc, rej) => {
            coordinator.run("identity", [name, name + "Ctr"], [tagName], (err, result) => {
                if (err !== null) {
                    return rej(err);
                }
                acc(result);
            });
        });
        // console.log(`${name}: ${tagName} => ${hash}`);
        identityCache.set(inMemoryKey, parseInt(hash));
        return identityCache.get(inMemoryKey);
    }
    else {
        return existingId;
    }
    //return Array.from(tagName).reduce((acc, c) => acc + c.charCodeAt(0), 0);
}

async function clearPartitionIdentity(partitionName) {
    //console.log("Cleared: " + partitionName);
    const response = await new Promise((acc, rej) => {
        coordinator.run("clear-identity", [partitionsKey], [partitionName], (err, result) => {
            if (err !== null) {
                return rej(err);
            }
            acc(result);
        });
    });
    if (response === 1) {
        const inMemoryKey = `${partitionsKey}${partitionName}`;
        identityCache.delete(inMemoryKey);
    }
    return response === 1;
}

async function redisShard(partitionName) {
    const index = await hash(partitionName, partitionsKey) % shards.length;
    //console.log(`${partitionName} => ${index}`);
    return shards[index].hot;
}

async function tagGrouping(tagName) {
    const tagId = await hash(tagName);
    return tagId - (tagId % MaxTagsPerPartition);
}

const hotStores = (process.env.hot || process.env.HOT).split(' ');
const coldStores = (process.env.cold || process.env.COLD).split(' ');
const coordinator = new scripto(new redisType(process.env.coo || process.env.COO));
coordinator.loadFromDir(path.join(__dirname, "lua"));
stores = hotStores.map((h, idx) => ({ "hot": h, "cold": coldStores[idx] }));
const shards = stores.map(storeInfo => ({ "hot": new redisType(storeInfo.hot), "cold": storeInfo.cold }));
// setInterval(() => {
//     console.log("Internal Cache Size: " + identityCache.size);
// }, 1000 * 60)

module.exports = {
    "clearPartitionIdentity": clearPartitionIdentity,
    "MaxTagsPerPartition": MaxTagsPerPartition,
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