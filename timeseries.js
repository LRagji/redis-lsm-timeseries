const Crypto = require("crypto");

//Domain constants
const WITHSCORES = "WITHSCORES";

module.exports = class Timeseries {

    instanceHash

    constructor(tagPartitionResolver, partitionRedisConnectionResolver, tagNumericIdentityResolver,
        settings = {
            "ActivityKey": "Activity",
            "SamplesPerPartitionKey": "Stats",
            "Seperator": "=",
            "MaximumTagsInOneWrite": 2000,
            "MaximumTagsInOneRead": 100,
            "MaximumTagNameLength": 200,
            "MaximumPartitionsScansInOneRead": 100,
            "PartitionTimeWidth": 60000,
            "PurgeMarker": "P"
        }) {
        //Validations
        if ({}.toString.call(tagPartitionResolver) !== '[object Function]') {
            throw new Error(`Invalid parameter "tagPartitionResolver" should be a function.`);
        }
        if ({}.toString.call(partitionRedisConnectionResolver) !== '[object Function]') {
            throw new Error(`Invalid parameter "partitionRedisConnectionResolver" should be a function.`);
        }
        if ({}.toString.call(tagNumericIdentityResolver) !== '[object Function]') {
            throw new Error(`Invalid parameter "tagNumericIdentityResolver" should be a function.`);
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
        this._tagNumericIdentityResolver = tagName => BigInt(tagNumericIdentityResolver(tagName));
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
        this._maximum = this._maximum.bind(this);
        this._minimum = this._minimum.bind(this);
        this._settingsHash = this._settingsHash.bind(this);
        this._assembleRedisKey = this._assembleRedisKey.bind(this);
        this._validateTransformReadParameters = this._validateTransformReadParameters.bind(this);
        this._validateTransformWriteParameters = this._validateTransformWriteParameters.bind(this);
    }

    async write(tagTimeMap, requestId = Date.now()) {

        const transformed = this._validateTransformWriteParameters(tagTimeMap, requestId);

        if (transformed.error !== null) {
            return Promise.reject(transformed.error);
        }

        let asyncCommands = [];
        transformed.payload.forEach((samples, partitionName) => {
            asyncCommands.push((async () => {
                const redisClient = this._partitionRedisConnectionResolver(partitionName);
                const scoredSamples = Array.from(samples, kvp => kvp.reverse()).flatMap(kvp => kvp);
                const serverTime = await redisClient.time();
                await redisClient.zadd(this._assembleRedisKey(partitionName), ...scoredSamples);//Main partition
                await redisClient.zadd(this._assembleRedisKey(this._settings.ActivityKey), serverTime[0], partitionName);//Activity for partition
                await redisClient.zincrby(this._assembleRedisKey(this._settings.SamplesPerPartitionKey), (scoredSamples.length / 2), partitionName);//Sample count for partition
            })());
        });

        let results = await Promise.allSettled(asyncCommands);
        results.forEach(e => {
            if (e.status !== "fulfilled") {
                throw new Error("Failed to complete operation:" + e.reason);
            }
        });

        return true;
    }

    async read(tagRanges) {
        const transformed = this._validateTransformReadParameters(tagRanges);

        if (transformed.error !== null) {
            return Promise.reject(transformed.error);
        }

        let asyncCommands = [];
        transformed.ranges.forEach((ranges, partitionName) => {
            const redisClient = this._partitionRedisConnectionResolver(partitionName);
            const purgedPartitionName = `${partitionName}${this._settings.Seperator}${this._settings.PurgeMarker}`;
            asyncCommands = asyncCommands.concat(ranges.map(async range => {
                const purgingDataResponse = await redisClient.zrangebyscore(this._assembleRedisKey(purgedPartitionName), range.start, range.end, WITHSCORES);//Purging partition Query
                const activeDataResponse = await redisClient.zrangebyscore(this._assembleRedisKey(partitionName), range.start, range.end, WITHSCORES);//Active partition Query
                range.response = purgingDataResponse.concat(activeDataResponse);//Sequence matters cause of time sorted keys.
                return range;
            }));
        });

        let results = await Promise.allSettled(asyncCommands);
        let returnData = new Map();
        results.forEach(e => {
            if (e.status !== "fulfilled") {
                throw new Error("Failed to complete operation:" + e.reason);
            } else {
                const timeMap = returnData.get(e.value.tagName) || new Map();
                for (let index = 0; index < e.value.response.length; index += 2) {
                    const time = (BigInt(e.value.response[index + 1]) - e.value.tagId) + e.value.timeOffset;
                    const packet = JSON.parse(e.value.response[index]);
                    packet.rid = BigInt(packet.rid);
                    let exisitingData = timeMap.get(time);
                    if (exisitingData == null ||
                        exisitingData.rid < packet.rid ||
                        (exisitingData.rid === packet.rid && exisitingData.ctr < packet.ctr)) {
                        exisitingData = packet;
                    }
                    timeMap.set(time, exisitingData);
                }
                returnData.set(e.value.tagName, timeMap);
            }
        });
        returnData.forEach((timeMap, tagName) => {
            timeMap.forEach((payload, time) => {
                timeMap.set(time, payload.pay);
            });
            returnData.set(tagName, timeMap);
        })
        return returnData;
    }

    _validateTransformReadParameters(tagRanges) {
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
        tagRanges.forEach((range, tagName) => {
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
            const tagId = this._tagNumericIdentityResolver(tagName);
            while (start < end) {
                const partitionName = `${this._tagPartitionResolver(tagName)}${this._settings.Seperator}${start}`;
                let readFrom = this._maximum(start, BigInt(range.start));
                let readTill = this._minimum((start + (this._settings.PartitionTimeWidth - 1n)), end)
                readFrom = readFrom === 0n ? readFrom : readFrom - start;
                readTill = readTill === 0n ? readTill : readTill - start;
                readFrom += tagId;
                readTill += tagId;
                const ranges = returnObject.ranges.get(partitionName) || [];
                ranges.push({ "start": readFrom, "end": readTill, "tagName": tagName, "tagId": tagId, "timeOffset": start });
                returnObject.ranges.set(partitionName, ranges);
                start += this._settings.PartitionTimeWidth;
                if (returnObject.ranges.size > this._settings.MaximumPartitionsScansInOneRead) {
                    errors.push(`Please reduce tags or time range for query; Max allowed partition scans ${this._settings.MaximumPartitionsScansInOneRead} & currently its ${returnObject.ranges.size}.`);
                    break;
                }
            }
        });

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

    _validateTransformWriteParameters(tagTimeMap, requestId) {
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

        tagTimeMap.forEach((timeDataMap, tagName) => {
            if (timeDataMap instanceof Map === false) {
                errors.push(`Tag "${tagName}" has element which is not of type "Map".`);
            }
            else if (tagName.length > this._settings.MaximumTagNameLength) {
                errors.push(`Tag "${tagName}" has name which extends character limit(${this._settings.MaximumTagNameLength}).`);
            }
            else {
                timeDataMap.forEach((sample, sampleTime) => {
                    if (sampleCounter > this._settings.MaximumTagsInOneWrite) {
                        returnObject.error = `Sample size exceeded limit of ${this._settings.MaximumTagsInOneWrite}.`;
                        return returnObject;
                    }
                    sampleTime = BigInt(sampleTime);
                    const partitionStart = sampleTime - (sampleTime % this._settings.PartitionTimeWidth);
                    const partitionName = `${this._tagPartitionResolver(tagName)}${this._settings.Seperator}${partitionStart}`;
                    if (partitionName === this._settings.ActivityKey) {
                        returnObject.error = `Conflicting partition name with Reserved Key for "Activity" (${partitionName}).`;
                        return returnObject;
                    }
                    if (partitionName === this._settings.SamplesPerPartitionKey) {
                        returnObject.error = `Conflicting partition name with Reserved Key for "SamplesPerPartitionKey" (${partitionName}).`;
                        return returnObject;
                    }
                    const serializedSample = JSON.stringify({ 'pay': sample, 'rid': requestId.toString(), 'ctr': sampleCounter });
                    const tagId = this._tagNumericIdentityResolver(tagName);
                    const sampleScore = BigInt(tagId) + (sampleTime - partitionStart);
                    const scoreTable = returnObject.payload.get(partitionName) || new Map();
                    scoreTable.set(serializedSample, sampleScore);
                    returnObject.payload.set(partitionName, scoreTable);
                    sampleCounter++;
                });
            }
        });

        if (sampleCounter === 0 && errors.length == 0) {
            returnObject.error = `Parameter 'tagTimeMap' should contain atleast one item to insert.`;
            return returnObject;
        }

        if (errors.length > 0) {
            returnObject.error = "Parameter 'tagTimeMap' has multiple Errors: " + errors.join(' , ');
            return returnObject;
        }

        return returnObject;
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