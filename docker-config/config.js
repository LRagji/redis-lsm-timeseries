const redisType = require("ioredis");
const tags = new Map();
let stores = [
    { "hot": "redis://172.17.0.2:6379/", "cold": "/external/raw-db/" },
    //{ "hot": "redis://127.0.0.1:6379/", "cold": "postgres://postgres:@localhost:5432/Data" },
    //"redis://127.0.0.1:6380/"
];
const maximumNumberOfPartitions = 10;

module.exports = {
    "stores": stores,
    "tagNameToTagId": hash,
    "tagToPartitionMapping": tagGrouping,
    "partitionToRedisMapping": redisShard,
    "settings": {
        "ActivityKey": "Activity",
        "SamplesPerPartitionKey": "Stats",
        "PurgePendingKey": "Pending",
        "Seperator": "=",
        "MaximumTagsInOneWrite": 2000,
        "MaximumTagsInOneRead": 100,
        "MaximumTagNameLength": 200,
        "MaximumPartitionsScansInOneRead": 100,
        "PartitionTimeWidth": 60000,
        "PurgeMarker": "P"
    }
}

function hash(tagName) {
    const existingId = tags.get(tagName);
    if (existingId == null) {
        tags.set(tagName, (tags.size + 1));
        return tags.get(tagName);
    }
    else {
        return existingId;
    }
    //return Array.from(tagName).reduce((acc, c) => acc + c.charCodeAt(0), 0);
}

function redisShard(partitionName) {
    const index = hash(partitionName) % shards.length;
    return shards[index].hot;
}

function tagGrouping(tagName) {
    return hash(tagName) % maximumNumberOfPartitions;
}

const shards = stores.map(storeInfo => ({ "hot": new redisType(storeInfo.hot), "cold": storeInfo.cold }));

