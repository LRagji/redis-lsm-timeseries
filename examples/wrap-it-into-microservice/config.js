const redisType = require("ioredis");
const scripto = require('redis-scripto2');
const path = require('path');
const identityCache = new Map();
const partitionsKey = "Partitions";
const clientsPerShard = parseInt(process.env.clients || process.env.CLIENTS || 3);
let stores = [
    //{ "hot": "redis://127.0.0.1:6379/", "cold": "/raw-db/" },
    //{ "hot": "redis://127.0.0.1:6379/", "cold": "postgres://postgres:@localhost:5432/Data" },
];

const MaxTagsPerPartition = 2000;
const shardBuffer = new SharedArrayBuffer(8);
const shardCounter = new Uint8Array(shardBuffer);
shardCounter[0] = 0;
const cordinatorBuffer = new SharedArrayBuffer(8);
const cordinatorCounter = new Uint8Array(cordinatorBuffer);
cordinatorCounter[0] = 0;
// const idBuffer = new SharedArrayBuffer(16);
// const idCounter = new Uint32Array(idBuffer);
// idCounter[0] = parseInt(process.env.root || process.env.ROOT);

function getInmemoryKey(lastName, firstName) {
    return `${lastName}${firstName}`;
}

async function hash(tagNames, createIfNotFound = true, name = "Tags") {
    let returnMap = new Map();
    const redisQueryTagNames = [];
    tagNames.forEach((tagName) => {
        const inMemoryKey = getInmemoryKey(name, tagName);
        const existingId = identityCache.get(inMemoryKey);
        if (existingId == null) {
            redisQueryTagNames.push(tagName);
        }
        else {
            returnMap.set(tagName, existingId);
        }
    });
    //console.log(`Hit Ratio: ${(((returnMap.size / tagNames.length)) * 100).toFixed(0)}`);
    if (redisQueryTagNames.length > 0) {
        const queryResults = await new Promise((acc, rej) => {
            coordinators[sequentialCount(cordinatorCounter, coordinators.length)].run("identity", [name, name + "Ctr"], [(createIfNotFound === true ? 1 : 0), ...redisQueryTagNames], (err, result) => {
                if (err !== null) {
                    return rej(err);
                }
                acc(result);
            });
        });
        queryResults.forEach(nameToId => {
            identityCache.set(getInmemoryKey(name, nameToId[0]), parseInt(nameToId[1]));
            returnMap.set(nameToId[0], parseInt(nameToId[1]));
        });
        // redisQueryTagNames.forEach(tName => {
        //     let tId = Atomics.add(idCounter, 0, 1);
        //     identityCache.set(getInmemoryKey(name, tName), tId);
        //     returnMap.set(tName, tId);
        // });
    }
    return returnMap;
}

async function clearPartitionIdentity(partitionName) {
    //console.log("Cleared: " + partitionName);
    const response = await new Promise((acc, rej) => {
        coordinators[sequentialCount(cordinatorCounter, coordinators.length)].run("clear-identity", [partitionsKey], [partitionName], (err, result) => {
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

async function redisShard(partitionName, createIfNotFound) {
    const results = await hash([partitionName], createIfNotFound, partitionsKey);
    const index = results.get(partitionName) % shards.length;
    //console.log(`${partitionName} => ${index}`);
    return shards[index].hot[sequentialCount(shardCounter, shards[index].hot.length)];
}

async function tagGrouping(tagName, createIfNotFound) {
    const results = await hash([tagName], createIfNotFound);
    const tagId = results.get(tagName);
    return tagId - (tagId % MaxTagsPerPartition);
}

function sequentialCount(memory, maxcount) {
    return Atomics.add(memory, 0, 1) % maxcount;
}

const hotStores = (process.env.hot || process.env.HOT).split(' ');
const coldStores = (process.env.cold || process.env.COLD || "").split(' ');
const coordinatorConnection = process.env.coo || process.env.COO;
const coordinators = Array.from({ length: (clientsPerShard * 2) }, _ => {
    const scriptManager = new scripto(new redisType(coordinatorConnection));
    scriptManager.loadFromDir(path.join(__dirname, "lua"));
    return scriptManager;
})
stores = hotStores.map((h, idx) => ({ "hot": h, "cold": coldStores[idx] }));
const shards = stores.map(storeInfo => ({ "hot": Array.from({ length: clientsPerShard }, _ => new redisType(storeInfo.hot)), "cold": storeInfo.cold }));
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