const Crypto = require("crypto");
const path = require("path");

//Domain constants
const WITHSCORES = "WITHSCORES";
const LUA_SCRIPT_DIR_NAME = "lua-scripts";
const PURGE_ACQUIRE_SCRIPT_NAME = "purge-acquire";
const PURGE_RELEASE_SCRIPT_NAME = "purge-release";

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

        this._tagPartitionResolver = tagPartitionResolver;
        this._partitionRedisConnectionResolver = partitionRedisConnectionResolver;
        this._tagsNumericIdentityResolver = async (tagNames, createIdentity) => {
            const redisMaximumScore = 9007199254740992n// Maximum score:https://redis.io/commands/ZADD
            const maxLimit = (redisMaximumScore / settings.PartitionTimeWidth);
            const tagIds = await tagsNumericIdentityResolver(tagNames, createIdentity);
            let validatingArray = Array.from(tagNames);
            tagIds.forEach((tagId, tagName) => {
                tagId = BigInt(tagId);
                if (tagId < 1n) {
                    throw new Error(`Tag ${tagName} numeric id's cannot be less than 1.`);
                }

                if (tagId > maxLimit) {
                    throw new Error(`Tag ${tagName} numeric id's cannot be greater than ${maxLimit}.`);
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

        let asyncCommands = [];
        let ctr = 0;
        transformed.payload.forEach((samples, partitionName) => {
            asyncCommands.push((async () => {
                const redisClient = await this._partitionRedisConnectionResolver(partitionName, true);
                const scoredSamples = Array.from(samples, kvp => kvp.reverse()).flatMap(kvp => kvp);
                const serverTime = await redisClient.time();
                ctr++;
                return await redisClient.pipeline()
                    .zadd(this._assembleRedisKey(partitionName), ...scoredSamples)//Main partition
                    .zadd(this._assembleRedisKey(this._settings.ActivityKey), serverTime[0], partitionName)//Activity for partition
                    .zincrby(this._assembleRedisKey(this._settings.SamplesPerPartitionKey), (scoredSamples.length / 2), partitionName)//Sample count for partition
                    .incrby(this._assembleRedisKey(this._settings.InputRatePerf), (scoredSamples.length / 2))//Input rate for diagnostics.
                    .exec();

            })());
        });

        let results = await Promise.allSettled(asyncCommands);
        log += " W.CO: " + ctr;
        log += " W.R: " + (Date.now() - sDate);
        sDate = Date.now();
        results.forEach(e => {
            if (e.status !== "fulfilled") {
                throw new Error("Failed to complete operation:" + e.reason);
            }
        });
        log += " W.F: " + (Date.now() - sDate);
        sDate = Date.now();
        return log;
    }

    async read(tagRanges) {
        const transformed = await this._validateTransformReadParameters(tagRanges);

        if (transformed.error !== null) {
            return Promise.reject(transformed.error);
        }

        let asyncCommands = [];
        const keys = Array.from(transformed.ranges.keys());
        for (let keyIndex = 0; keyIndex < keys.length; keyIndex++) {
            const partitionName = keys[keyIndex];
            const ranges = transformed.ranges.get(partitionName);
            const redisClient = await this._partitionRedisConnectionResolver(partitionName, false);
            const purgedPartitionName = `${partitionName}${this._settings.Seperator}${this._settings.PurgeMarker}`;
            asyncCommands = asyncCommands.concat(ranges.map(async range => {
                const multiResponse = await redisClient.pipeline()
                    .zrangebyscore(this._assembleRedisKey(purgedPartitionName), range.start, range.end, WITHSCORES) //Purging partition Query
                    .zrangebyscore(this._assembleRedisKey(partitionName), range.start, range.end, WITHSCORES) //Active partition Query
                    .exec();
                range.response = multiResponse.reduce((accum, e) => accum[1].concat(e[1]));//Sequence matters cause of time sorted keys.
                return range;
            }));
        };

        let results = await Promise.allSettled(asyncCommands);
        let returnData = new Map();
        results.forEach(e => {
            if (e.status !== "fulfilled") {
                throw new Error("Failed to complete operation:" + e.reason);
            } else {
                this._parsePartitionData(e.value.response, e.value.timeOffset, (tagId) => {
                    if (e.value.tagId !== tagId) {
                        throw new Error(`Parsing Failed! Tagids donot match ${e.value.tagName} (${e.value.tagId}) <> (${tagId}).`);
                    }
                    return returnData.get(e.value.tagName) || new Map();
                }, (tagId, timeMap) => {
                    if (e.value.tagId !== tagId) {
                        throw new Error(`Parsing Failed! Tagids donot match ${e.value.tagName} (${e.value.tagId}) <> (${tagId}).`);
                    }
                    return returnData.set(e.value.tagName, timeMap);
                });
            }
        });
        returnData.forEach((timeMap, tagName) => {
            timeMap.forEach((payload, time) => {
                timeMap.set(time, payload.p);
            });
            returnData.set(tagName, timeMap);
        });
        return returnData;
    }

    async purgeAcquire(scriptoServer, timeThreshold, countThreshold, reAcquireTimeout, partitionsToAcquire = 10) {

        if (scriptoServer == null) {
            return Promise.reject("Parameter 'scriptoServer' is invalid: Should be an instance of redis-scripto.");
        }
        try {
            timeThreshold = BigInt(timeThreshold);
        }
        catch (err) {
            return Promise.reject("Parameter 'timeThreshold' is invalid: " + err.message);
        }

        try {
            countThreshold = BigInt(countThreshold);
        }
        catch (err) {
            return Promise.reject("Parameter 'countThreshold' is invalid: " + err.message);
        }

        try {
            reAcquireTimeout = BigInt(reAcquireTimeout);
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
        scriptoServer.loadFromDir(path.join(__dirname, LUA_SCRIPT_DIR_NAME));

        const keys = [
            this._assembleRedisKey(this._settings.ActivityKey),
            this._assembleRedisKey(this._settings.SamplesPerPartitionKey),
            this._assembleRedisKey(this._settings.PurgePendingKey)
        ];
        const args = [
            partitionsToAcquire,
            timeThreshold,
            countThreshold,
            reAcquireTimeout,
            this.instanceHash,
            this._settings.Seperator,
            this._settings.PurgeMarker
        ];
        const acquiredData = await new Promise((acc, rej) => {
            scriptoServer.run(PURGE_ACQUIRE_SCRIPT_NAME, keys, args, (err, result) => {
                if (err !== null) {
                    return rej(err);
                }
                acc(result);
            });
        });

        return acquiredData.map(serializedData => {
            const acquiredPartitionInfo = {};
            const partitionInfo = this._extractPartitionInfo(serializedData[0]);
            acquiredPartitionInfo.releaseToken = serializedData[0];
            acquiredPartitionInfo.name = partitionInfo.partition;
            acquiredPartitionInfo.start = partitionInfo.start;
            acquiredPartitionInfo.data = new Map();
            this._parsePartitionData(serializedData[1], partitionInfo.start, tagId => acquiredPartitionInfo.data.get(tagId) || new Map(), acquiredPartitionInfo.data.set.bind(acquiredPartitionInfo.data));
            acquiredPartitionInfo.data.forEach((timeMap, tagName) => {
                timeMap.forEach((payload, time) => {
                    timeMap.set(time, payload.p);
                });
                acquiredPartitionInfo.data.set(tagName, timeMap);
            })
            return acquiredPartitionInfo;
        });
    }

    async purgeRelease(scriptoServer, releaseToken) {

        if (scriptoServer == null) {
            return Promise.reject("Parameter 'scriptoServer' is invalid: Should be an instance of redis-scripto.");
        }

        if (releaseToken == undefined || releaseToken === "") {
            return Promise.reject("Invalid parameter 'releaseToken'.");
        }
        const keys = [
            this._assembleRedisKey(this._settings.PurgePendingKey),
            this._assembleRedisKey(`${releaseToken}${this._settings.Seperator}${this._settings.PurgeMarker}`),
            this._assembleRedisKey(this._settings.OutputRatePerf),
        ];
        const args = [releaseToken];
        const response = await new Promise((acc, rej) => {
            scriptoServer.run(PURGE_RELEASE_SCRIPT_NAME, keys, args, (err, result) => {
                if (err !== null) {
                    return rej(err);
                }
                acc(result);
            });
        });
        return response === 1;
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
        const tagNameToIdMapping = await this._tagsNumericIdentityResolver(keys, false);
        for (let keyIndex = 0; keyIndex < keys.length; keyIndex++) {
            const tagName = keys[keyIndex];
            const range = tagRanges.get(tagName);
            let start, end;
            try {
                start = BigInt(range.start);
                start = start - (start % this._settings.PartitionTimeWidth);
            }
            catch (error) {
                errors.push(`Invalid start range for ${tagName}: ${error.message}`);
                return;
            };
            try {
                end = BigInt(range.end);
            }
            catch (error) {
                errors.push(`Invalid end range for ${tagName}: ${error.message}`);
                return;
            };
            if (tagName.length > this._settings.MaximumTagNameLength) {
                errors.push(`Tag "${tagName}" has name which extends character limit(${this._settings.MaximumTagNameLength}).`);
                return;
            }
            if (range.end < range.start) {
                errors.push(`Invalid range; start should be smaller than end for ${tagName}.`);
                return;
            }
            const tagId = tagNameToIdMapping.get(tagName);
            const tagOffset = this._computeTagSpaceStart(tagId);
            while (start < end) {
                const partitionName = `${await this._tagPartitionResolver(tagName, false)}${this._settings.Seperator}${start}`;
                let readFrom = this._maximum(start, BigInt(range.start));
                let readTill = this._minimum((start + (this._settings.PartitionTimeWidth - 1n)), end)
                readFrom = readFrom === 0n ? readFrom : readFrom - start;
                readTill = readTill === 0n ? readTill : readTill - start;
                readFrom += tagOffset;
                readTill += tagOffset;
                const ranges = returnObject.ranges.get(partitionName) || [];
                ranges.push({ "start": readFrom, "end": readTill, "tagName": tagName, "tagId": tagId, "timeOffset": start });
                returnObject.ranges.set(partitionName, ranges);
                start += this._settings.PartitionTimeWidth;
                if (returnObject.ranges.size > this._settings.MaximumPartitionsScansInOneRead) {
                    errors.push(`Please reduce tags or time range for query; Max allowed partition scans ${this._settings.MaximumPartitionsScansInOneRead} & currently its ${returnObject.ranges.size}.`);
                    break;
                }
            }
        };

        if (returnObject.ranges.size === 0 && errors.length == 0) {
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
        const returnObject = { "error": null, payload: new Map() };
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
        const tagNameToIdMapping = await this._tagsNumericIdentityResolver(tagKeys, true);
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
                    const partitionStart = sampleTime - (sampleTime % this._settings.PartitionTimeWidth);
                    const tagId = tagNameToIdMapping.get(tagName);
                    sDate = Date.now();
                    const partitionName = `${await this._tagPartitionResolver(tagName, true)}${this._settings.Seperator}${partitionStart}`;//
                    logSumPartiton += (Date.now() - sDate);
                    logPartitionCounter++;
                    if (partitionName === this._settings.ActivityKey) {
                        returnObject.error = `Conflicting partition name with Reserved Key for "Activity" (${partitionName}).`;
                        return returnObject;
                    }
                    if (partitionName === this._settings.SamplesPerPartitionKey) {
                        returnObject.error = `Conflicting partition name with Reserved Key for "SamplesPerPartitionKey" (${partitionName}).`;
                        return returnObject;
                    }
                    if (partitionName === this._settings.PurgePendingKey) {
                        returnObject.error = `Conflicting partition name with Reserved Key for "PurgePendingKey" (${partitionName}).`;
                        return returnObject;
                    }
                    if (partitionName === this._settings.InputRatePerf) {
                        returnObject.error = `Conflicting partition name with Reserved Key for "InputRatePerf" (${partitionName}).`;
                        return returnObject;
                    }
                    if (partitionName === this._settings.OutputRatePerf) {
                        returnObject.error = `Conflicting partition name with Reserved Key for "OutputRatePerf" (${partitionName}).`;
                        return returnObject;
                    }
                    if (partitionName === this._settings.InputRateTime) {
                        returnObject.error = `Conflicting partition name with Reserved Key for "InputRateTime" (${partitionName}).`;
                        return returnObject;
                    }
                    if (partitionName === this._settings.OutputRateTime) {
                        returnObject.error = `Conflicting partition name with Reserved Key for "OutputRateTime" (${partitionName}).`;
                        return returnObject;
                    }
                    const serializedSample = JSON.stringify({ 'p': sample, 'r': requestId.toString(), 'c': sampleCounter });
                    const sampleScore = this._computeTagSpaceStart(tagId) + (sampleTime - partitionStart);
                    const scoreTable = returnObject.payload.get(partitionName) || new Map();
                    scoreTable.set(serializedSample, sampleScore);
                    returnObject.payload.set(partitionName, scoreTable);
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
}