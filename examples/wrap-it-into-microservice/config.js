const redisType = require("ioredis");
let shards = [
    "redis://127.0.0.1:6379/",
    "redis://127.0.0.1:6380/"
];
const maximumNumberOfPartitions = 10;

module.exports = {
    "shards": shards,
    "tagNameToTagId": hash,
    "tagToPartitionMapping": tagGrouping,
    "partitionToRedisMapping": redisShard,
    "settings": {
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
}

function hash(tagName) {
    return Array.from(tagName).reduce((acc, c) => acc + c.charCodeAt(0), 0);
}

function redisShard(partitionName) {
    const index = hash(partitionName) % shards.length;
    return shards[index].client;
}

function tagGrouping(tagName) {
    return hash(tagName) % maximumNumberOfPartitions;
}

shards = shards.map(connectionString => ({ "client": new redisType(connectionString) }));

