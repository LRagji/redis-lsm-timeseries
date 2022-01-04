const Crypto = require("crypto");
const redisType = require("ioredis");
const path = require("path");
const pssfns = require("purgeable-sorted-set-family");
const fs = require('fs')

//Domain constants
const WITHSCORES = "WITHSCORES";
const LUA_SCRIPT_DIR_NAME = "lua-scripts";
const PURGE_ACQUIRE_SCRIPT_NAME = "purge-acquire";
const PURGE_RELEASE_SCRIPT_NAME = "purge-release";

class RedisClient {
    redisClient
    filenameToCommand = new Map();

    constructor(redisConnectionString) {
        this.redisClient = new redisType(redisConnectionString);
    }

    async shutdown() {
        await this.redisClient.quit();
        this.redisClient.disconnect();
    }

    async acquire(token) {
        //console.time(token);
    }

    async release(token) {
        //console.timeEnd(token);
    }

    async run(commandArgs) {
        //console.log(commandArgs);
        const v = await this.redisClient.send_command(commandArgs.shift(), ...commandArgs);
        //console.log(v);
        return v;
    }

    async pipeline(commands) {
        // console.log(commands);
        const result = await this.redisClient.multi(commands)
            .exec();
        const finalResult = result.map(r => {
            let err = r.shift();
            if (err != null) {
                throw err;
            }
            return r[0];
        });
        // console.log(finalResult);
        return finalResult;
    }

    async script(filename, keys, args) {
        let command = this.filenameToCommand.get(filename);
        if (command == null) {
            const contents = await new Promise((acc, rej) => {
                fs.readFile(filename, "utf8", (err, data) => {
                    if (err !== null) {
                        rej(err);
                    };
                    acc(data);
                });
            });
            command = Crypto.createHash("sha256").update(contents, "binary").digest("hex")
            this.redisClient.defineCommand(command, { lua: contents });
            this.filenameToCommand.set(filename, command);
        }
        // console.log(keys);
        // console.log(args);
        //console.log(filename);
        // @ts-ignore
        const results = await this.redisClient[command](keys.length, keys, args);
        //console.log(results);
        return results;
    }
}

module.exports = class Timeseries {

    instanceHash

    constructor(tagPartitionResolver, partitionRedisConnectionResolver, tagsNumericIdentityResolver,
        settings = {
            "ActivityKey": "Activity",
            "SamplesPerPartitionKey": "Stats",
            "PurgePendingKey": "Pending",
            "InputRatePerf": "I",
            "OutputRatePerf": "O",
            "InputRateTime": "IT",
            "OutputRateTime": "OT",
            "Seperator": "=",
            "MaximumTagsInOneWrite": 2000,
            "MaximumTagsInOneRead": 100,
            "MaximumTagNameLength": 200,
            "MaximumPartitionsScansInOneRead": 100,
            "PartitionTimeWidth": 60000,
            "PurgeMarker": "P"
        }) {
        //Validations
        if ({}.toString.call(tagPartitionResolver) !== '[object AsyncFunction]') {
            throw new Error(`Invalid parameter "tagPartitionResolver" should be a async function, but is ${{}.toString.call(tagPartitionResolver)}`);
        }
        if ({}.toString.call(partitionRedisConnectionResolver) !== '[object AsyncFunction]') {
            throw new Error(`Invalid parameter "partitionRedisConnectionResolver" should be a async function, but is ${{}.toString.call(partitionRedisConnectionResolver)}`);
        }
        if ({}.toString.call(tagsNumericIdentityResolver) !== '[object AsyncFunction]') {
            throw new Error(`Invalid parameter "tagsNumericIdentityResolver" should be a async function, but is ${{}.toString.call(tagsNumericIdentityResolver)}`);
        }
        if (settings.ActivityKey === settings.SamplesPerPartitionKey) {
            throw new Error(`Invalid settings "ActivityKey" & "SamplesPerPartitionKey" cannot be same.`);
        }
        try {
            settings.MaximumPartitionsScansInOneRead = BigInt(settings.MaximumPartitionsScansInOneRead);
        }
        catch (err) {
            throw new Error("Parameter 'settings.MaximumPartitionsScansInOneRead' is invalid: " + err.message);
        }
        try {
            settings.PartitionTimeWidth = BigInt(settings.PartitionTimeWidth);
        }
        catch (err) {
            throw new Error("Parameter 'settings.PartitionTimeWidth' is invalid: " + err.message);
        }
        this._shard = {};
        this._NDPart = {};
        partitionRedisConnectionResolver("Laukik", true)
            .then((conString) => {
                const redisClient = new RedisClient(conString);
                this._shard = new pssfns.RemotePSSF(async (ops) => redisClient);
                this._NDPart = new pssfns.NDimensionalPartitionedSortedSet([100n, 100n], _ => this._shard);
                console.log("System Active");
            });


        this._tagPartitionResolver = tagPartitionResolver;
        this._partitionRedisConnectionResolver = partitionRedisConnectionResolver;
        this._tagsNumericIdentityResolver = async (tagNames, createIdentity, log) => {
            const redisMaximumScore = 9007199254740992n// Maximum score:https://redis.io/commands/ZADD
            const maxLimit = (redisMaximumScore / settings.PartitionTimeWidth);
            const tagIds = await tagsNumericIdentityResolver(tagNames, createIdentity, undefined, log);
            let validatingArray = Array.from(tagNames);
            tagIds.forEach((tagId, tagName) => {
                if (!(Number.isNaN(tagId))) {
                    tagId = BigInt(tagId);
                    if (tagId < 1n) {
                        throw new Error(`Tag ${tagName} numeric id's cannot be less than 1.`);
                    }

                    if (tagId > maxLimit) {
                        throw new Error(`Tag ${tagName} numeric id's cannot be greater than ${maxLimit}.`);
                    }
                }
                tagIds.set(tagName, tagId);
                validatingArray = validatingArray.filter(tN => tN !== tagName);
            })
            if (validatingArray.length > 0 && createIdentity === true) {
                throw new Error(`Requested identities(${tagNames.length}) doesnt not match fetched count(${tagIds.size}), ${validatingArray.join(',')}.`);
            }
            return tagIds;
        }

        this._settings = settings;
        this._settings.version = "1.0"

        let settingsToBeHashed = {};
        Object.assign(settingsToBeHashed, this._settings);
        delete settingsToBeHashed.MaximumTagsInOneRead;
        delete settingsToBeHashed.MaximumTagsInOneWrite;
        delete settingsToBeHashed.MaximumPartitionsScansInOneRead;
        settingsToBeHashed.PartitionTimeWidth = settingsToBeHashed.PartitionTimeWidth.toString();
        this.instanceHash = this._settingsHash(settingsToBeHashed);

        //local functions
        this.write = this.write.bind(this);
        this.read = this.read.bind(this);
        this.purgeAcquire = this.purgeAcquire.bind(this);
        this.purgeRelease = this.purgeRelease.bind(this);
        this.diagnostic = this.diagnostic.bind(this);
        this._maximum = this._maximum.bind(this);
        this._minimum = this._minimum.bind(this);
        this._settingsHash = this._settingsHash.bind(this);
        this._assembleRedisKey = this._assembleRedisKey.bind(this);
        this._extractPartitionInfo = this._extractPartitionInfo.bind(this);
        this._parsePartitionData = this._parsePartitionData.bind(this);
        this._computeTagSpaceStart = this._computeTagSpaceStart.bind(this);
        this._validateTransformReadParameters = this._validateTransformReadParameters.bind(this);
        this._validateTransformWriteParameters = this._validateTransformWriteParameters.bind(this);
        this._sortAndSequence = this._sortAndSequence.bind(this);
    }

    async write(tagTimeMap, requestId = Date.now()) {

        let sDate = Date.now();
        let log = " ";
        const transformed = await this._validateTransformWriteParameters(tagTimeMap, requestId);
        log += " W.T: " + (Date.now() - sDate) + ` [ ${transformed.log} ]`;
        sDate = Date.now();

        if (transformed.error !== null) {
            return Promise.reject(transformed.error);
        }
        let results = await this._NDPart.write(transformed.payload);
        //log += " W.CO: " + ctr;
        log += " W.R: " + (Date.now() - sDate);
        sDate = Date.now();

        if (results.failed.length > 0) {
            throw new Error(`Failed to complete operation:${results.failed}`);
        }
        log += " W.F: " + (Date.now() - sDate);
        sDate = Date.now();
        return log;
    }

    async read(tagRanges) {
        const transformed = await this._validateTransformReadParameters(tagRanges);

        if (transformed.error !== null) {
            return Promise.reject(transformed.error);
        }
        const asyncCommands = transformed.queries.reduce((promiseHandles, q) => {
            promiseHandles.push((async () => {
                const tagIdToNameMap = q.tags;
                const data = await this._NDPart.rangeRead(q.start, q.end);
                if (data.error != undefined) {
                    throw data.error;
                }
                else {
                    return data.data.reduce((tagTimeMap, p) => {
                        const tagName = tagIdToNameMap.get(p.dimensions[1]) || "ImpossibleTagName";
                        const timeMap = tagTimeMap.get(tagName) || new Map();
                        const packet = JSON.parse(p.payload);
                        const existingPacket = timeMap.get(p.dimensions[0]);
                        if (existingPacket == null || existingPacket.r < packet.r) {
                            timeMap.set(p.dimensions[0], packet);
                        }
                        else if (existingPacket.r > packet.r) {
                            timeMap.set(p.dimensions[0], existingPacket);
                        }
                        else {
                            if (existingPacket.c > packet.c) {
                                timeMap.set(p.dimensions[0], existingPacket);
                            }
                            else {
                                timeMap.set(p.dimensions[0], packet);
                            }
                        }
                        tagTimeMap.set(tagName, timeMap);
                        return tagTimeMap;
                    }, new Map());
                }
            })());
            return promiseHandles;
        }, []);

        let results = await Promise.allSettled(asyncCommands);
        let returnData = new Map();
        results.forEach(e => {
            if (e.status !== "fulfilled") {
                throw new Error("Failed to complete operation:" + e.reason);
            } else {
                //Asumption the range will always have one tag once
                e.value.forEach((timeMap, tagName) => {
                    let payLoadTimeMap = new Map();
                    timeMap.forEach((payload, time) => payLoadTimeMap.set(time, payload.p));
                    returnData.set(tagName, payLoadTimeMap);
                    return returnData;
                });
            }
        });

        return returnData;
    }

    async purgeAcquire(timeThreshold, countThreshold, reAcquireTimeout, partitionsToAcquire = 10, tagIdToNameResolver = (tagIds) => Promise.resolve(tagIds.reduce((map, e) => { map.set(e, e); return map; }, new Map()))) {

        try {
            timeThreshold = parseInt(timeThreshold);
        }
        catch (err) {
            return Promise.reject("Parameter 'timeThreshold' is invalid: " + err.message);
        }

        try {
            countThreshold = parseInt(countThreshold);
        }
        catch (err) {
            return Promise.reject("Parameter 'countThreshold' is invalid: " + err.message);
        }

        try {
            reAcquireTimeout = parseInt(reAcquireTimeout);
        }
        catch (err) {
            return Promise.reject("Parameter 'reAcquireTimeout' is invalid: " + err.message);
        }

        if (partitionsToAcquire <= 0) {
            throw new Error("Parameter 'partitionsToAcquire' cannot be less then or equal to zero.");
        }

        if (countThreshold <= 0 && reAcquireTimeout <= 0 && timeThreshold <= 0) {
            throw new Error("Parameter 'countThreshold' 'reAcquireTimeout' 'timeThreshold' cannot be less then or equal to zero.");
        }

        const returnData = [];
        const tokenizedData = await this._shard.purgeBegin(timeThreshold, countThreshold, null, reAcquireTimeout, partitionsToAcquire);
        if (tokenizedData.error != undefined) {
            throw tokenizedData.error;
        }
        const transFormedData = this._NDPart.parseTokenizedDimentionalData(tokenizedData.data);
        const tagIds = Array.from(
            Array.from(transFormedData.values())
                .map(e => e[1])
                .reduce((s, e) => { s.add(e); return s; }, new Set()));
        const tagIdToNameMapping = await tagIdToNameResolver(tagIds);// Need a tag reverse id map lookup
        transFormedData.forEach((dData, token) => {
            const payload = { releaseToken: token, data: new Map() };
            dData.forEach((data) => {
                const tagId = data.dimensions[1];
                const time = data.dimensions[0];
                const currentPayload = JSON.parse(data.payload);//Possible to blow up for bigint
                const tagName = tagIdToNameMapping.get(tagId);
                const timeMap = payload.data.get(tagName) || new Map();
                let existingPayload = timeMap.get(time);
                if (existingPayload == null ||
                    existingPayload.r < currentPayload.r ||
                    (existingPayload.r === currentPayload.r && existingPayload.c < currentPayload.c)) {
                    existingPayload = currentPayload;
                }
                timeMap.set(time, existingPayload);
                payload.data.set(tagName, timeMap);
            });
            payload.data.forEach((timeMap, tagName) => {
                timeMap.forEach((payload, time) => {
                    timeMap.set(time, payload.p);
                });
            });
            returnData.push(payload);
        });
        return returnData;
    }

    async purgeRelease(releaseToken) {

        if (releaseToken == undefined || releaseToken === "") {
            return Promise.reject("Invalid parameter 'releaseToken'.");
        }

        const tokens = [releaseToken];
        const released = await this._shard.purgeEnd(tokens);
        const failedTokens = released.failed;
        released.succeeded.reduce((ft, t) => {
            if (tokens.indexOf(t) === -1) {
                ft.push(t);
            }
            return ft;
        }, failedTokens);

        return failedTokens.length === 0;
    }

    async diagnostic(redisClient) {
        const timeInMilliseconds = Date.now();
        const response = await redisClient.pipeline()
            .getset(this._assembleRedisKey(this._settings.InputRatePerf), 0)//Input rate
            .getset(this._assembleRedisKey(this._settings.OutputRatePerf), 0)//Output rate
            .getset(this._assembleRedisKey(this._settings.InputRateTime), timeInMilliseconds)//Input rate time
            .getset(this._assembleRedisKey(this._settings.OutputRateTime), timeInMilliseconds)//output rate time.
            .exec();
        const inputCount = parseInt(response[0][1] || 1);
        const outputCount = parseInt(response[1][1] || 1);
        const inputTime = parseInt(response[2][1] || 1);
        const outputTime = parseInt(response[3][1] || 1);

        return {
            "inputRate": inputCount / (inputTime === 1 ? inputTime : ((timeInMilliseconds - inputTime) / 1000)),
            "outputRate": outputCount / (outputTime === 1 ? outputTime : ((timeInMilliseconds - outputTime) / 1000)),
            "deltaRate": (inputCount - outputCount) / (outputTime === 1 ? outputTime : ((timeInMilliseconds - outputTime) / 1000))
        }
    }

    async _validateTransformReadParameters(tagRanges) {
        const returnObject = { "error": null, ranges: new Map() };
        if (tagRanges instanceof Map === false) {
            returnObject.error = `Parameter 'tagRanges' should be of type 'Map' instead of ${typeof (tagRanges)}`;
            return returnObject;
        }

        if (tagRanges.size > this._settings.MaximumTagsInOneRead) {
            returnObject.error = `Parameter 'tagRanges' cannot have tags more than ${this._settings.MaximumTagsInOneRead}.`;
            return returnObject;
        }

        const errors = [];
        const keys = Array.from(tagRanges.keys());
        const tagNameToIdMapping = await this._tagsNumericIdentityResolver(keys, false, true);
        const sequencedTagNameToIdMapping = this._sortAndSequence(Array.from(tagNameToIdMapping.entries()), (a, b) => parseInt((a[1] - b[1]).toString()), (a, b) => !Number.isNaN(a) && b[1] - a[1] === 1);
        returnObject.queries = sequencedTagNameToIdMapping.reduce((acc, sequencedChunk) => {
            return acc.concat(sequencedChunk.reduce((queries, tagNameToId) => {
                const tagName = tagNameToId[0];
                const tagId = tagNameToId[1];
                const range = tagRanges.get(tagName);
                if (Number.isNaN(tagId)) {
                    errors.push(`Tag(${tagName}) doesnot exists.`);
                    return queries;
                }
                try {
                    range.start = BigInt(range.start);
                }
                catch (error) {
                    errors.push(`Invalid start range for ${tagName}: ${error.message}`);
                    return queries;
                };
                try {
                    range.end = BigInt(range.end);
                }
                catch (error) {
                    errors.push(`Invalid end range for ${tagName}: ${error.message}`);
                    return queries;
                };
                if (tagName.length > this._settings.MaximumTagNameLength) {
                    errors.push(`Tag "${tagName}" has name which extends character limit(${this._settings.MaximumTagNameLength}).`);
                    return queries;
                }
                if (range.end < range.start) {
                    errors.push(`Invalid range; start should be smaller than end for ${tagName}.`);
                    return queries;
                }

                if (queries.length > 0 && queries[queries.length - 1].range.start === range.start && queries[queries.length - 1].range.end === range.end) {
                    queries[queries.length - 1].end = [range.end, tagId];
                    queries[queries.length - 1].tags.set(tagId, tagName);
                }
                else {
                    const query = { tags: new Map() };
                    query.tags.set(tagId, tagName);
                    query.range = range;
                    query.start = [range.start, tagId];
                    query.end = [range.end, tagId];
                    queries.push(query);
                }
                return queries;
            }, []));
        }, []);
        if (returnObject.queries.length === 0 && errors.length == 0) {
            returnObject.error = `Parameter 'partitionRanges' should contain atleast one range for query.`;
            return returnObject;
        }

        if (errors.length > 0) {
            returnObject.error = "Parameter 'partitionRanges' has multiple Errors: " + errors.join(' , ');
            return returnObject;
        }

        return returnObject;
    }

    async _validateTransformWriteParameters(tagTimeMap, requestId) {
        let logPartitionCounter = 0, logTagIdCounter = 0, logSumPartiton = 0, logSumTag = 0;
        const returnObject = { "error": null, payload: [] };
        const errors = [];
        let sampleCounter = 0;
        try {
            requestId = BigInt(requestId);
        }
        catch (err) {
            throw new Error("Parameter 'requestId' is invalid: " + err.message);
        }

        if (tagTimeMap instanceof Map === false) {
            returnObject.error = `Parameter 'tagTimeMap' should be of type 'Map' instead of ${typeof (tagTimeMap)}`;
            return returnObject;
        }
        const tagKeys = Array.from(tagTimeMap.keys());
        let sDate = Date.now();
        const tagNameToIdMapping = await this._tagsNumericIdentityResolver(tagKeys, true, true);
        logSumTag += (Date.now() - sDate);
        logTagIdCounter++;
        for (let tagKeyIndex = 0; tagKeyIndex < tagKeys.length; tagKeyIndex++) {
            const tagName = tagKeys[tagKeyIndex];
            const timeDataMap = tagTimeMap.get(tagName);
            if (timeDataMap instanceof Map === false) {
                errors.push(`Tag "${tagName}" has element which is not of type "Map".`);
            }
            else if (tagName.length > this._settings.MaximumTagNameLength) {
                errors.push(`Tag "${tagName}" has name which extends character limit(${this._settings.MaximumTagNameLength}).`);
            }
            else {
                const keys = Array.from(timeDataMap.keys());
                for (let keyIndex = 0; keyIndex < keys.length; keyIndex++) {
                    let sampleTime = keys[keyIndex];
                    const sample = timeDataMap.get(sampleTime);
                    if (sampleCounter > this._settings.MaximumTagsInOneWrite) {
                        returnObject.error = `Sample size exceeded limit of ${this._settings.MaximumTagsInOneWrite}.`;
                        return returnObject;
                    }
                    sampleTime = BigInt(sampleTime);
                    const tagId = BigInt(tagNameToIdMapping.get(tagName));
                    const serializedSample = JSON.stringify({ 'p': sample, 'r': requestId.toString(), 'c': sampleCounter });
                    returnObject.payload.push({ payload: serializedSample, dimensions: [sampleTime, tagId] });
                    sampleCounter++;
                };
            }
        };

        if (sampleCounter === 0 && errors.length == 0) {
            returnObject.error = `Parameter 'tagTimeMap' should contain atleast one item to insert.`;
            return returnObject;
        }

        if (errors.length > 0) {
            returnObject.error = "Parameter 'tagTimeMap' has multiple Errors: " + errors.join(' , ');
            return returnObject;
        }

        returnObject.log = `c:${logPartitionCounter} avg:${(logSumPartiton / logPartitionCounter).toFixed(0)} c:${logTagIdCounter} avg:${(logSumTag / logTagIdCounter).toFixed(0)}`;
        return returnObject;
    }

    _parsePartitionData(rawResponse, partitionStart, getTimeMap, setTimeMap) {
        for (let index = 0; index < rawResponse.length; index += 2) {
            const score = BigInt(rawResponse[index + 1]);
            const packet = JSON.parse(rawResponse[index]);
            const time = (score % this._settings.PartitionTimeWidth) + partitionStart;
            const tagId = ((score - (score % this._settings.PartitionTimeWidth)) / this._settings.PartitionTimeWidth) + 1n;
            const timeMap = getTimeMap(tagId);
            packet.r = BigInt(packet.r);
            let exisitingData = timeMap.get(time);
            if (exisitingData == null ||
                exisitingData.r < packet.r ||
                (exisitingData.r === packet.r && exisitingData.c < packet.c)) {
                exisitingData = packet;
            }
            timeMap.set(time, exisitingData);
            setTimeMap(tagId, timeMap);
        }
    }

    _computeTagSpaceStart(tagId) {
        const tagIdSpaceEnd = (tagId * this._settings.PartitionTimeWidth) - 1n;
        const tagIdSpaceStart = tagIdSpaceEnd - (this._settings.PartitionTimeWidth - 1n);
        return tagIdSpaceStart;
    }

    _extractPartitionInfo(partitionName) {
        let seperatorIndex = partitionName.lastIndexOf(this._settings.Seperator);
        if (seperatorIndex < 0 || (seperatorIndex + 1) >= partitionName.length) {
            throw new Error("Seperator misplaced @" + seperatorIndex);
        }
        const start = BigInt(partitionName.substring(seperatorIndex + 1));//0
        const key = partitionName.substring(0, seperatorIndex);//ABC
        return { "start": start, "partition": key };
    }

    _settingsHash(settings) {
        return Crypto.createHash("sha256").update(JSON.stringify(settings), "binary").digest("hex");
    }

    _assembleRedisKey(key) {
        return `${this.instanceHash}${this._settings.Seperator}${key}`;
    }

    _maximum(lhs, rhs) {
        return lhs > rhs ? lhs : rhs;
    }

    _minimum(lhs, rhs) {
        return lhs < rhs ? lhs : rhs;
    }

    _sortAndSequence(numbers, sortFunction = (a, b) => a - b, sequenceFunction = (a, b) => b - a === 1) {
        const sortedNumbers = numbers.sort(sortFunction);
        return sortedNumbers.reduce((acc, e) => {
            if (acc.length === 0) {
                acc.push([e]);
            }
            else {
                const lastActiveSequence = acc[acc.length - 1];
                const lastSequnceNumber = lastActiveSequence[lastActiveSequence.length - 1];
                if (sequenceFunction(lastSequnceNumber, e) === true) {
                    acc[acc.length - 1].push(e);
                }
                else {
                    acc.push([e]);
                }
            }
            return acc;
        }, []);
    }
}