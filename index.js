const Scripto = require("redis-scripto");
const Crypto = require("crypto");
const shortid = require("shortid");
const path = require("path");


//Domain constants
const EPOCHKey = "EPOCH";
const SetOptionDonotOverwrite = "NX";
const DecimalRadix = 10;
const Seperator = "-";
const safeMaxItemLimit = 2000;
const safeIndexedTagsRead = 100;
const RecentAcitivityKey = "RecentActivity";
const MonitoringKey = "MonitorConfig";
const SafeKeyNameLength = 200;
const WITHSCORES = "WITHSCORES";
const scriptNamePurgeAcquire = "purge-acquire";
const scriptNamePurgeRelease = "purge-release";
const AccumalatingFlag = "acc";
const PendingPurgeKey = "Pen"
const PurgeFlag = "pur"

class SortedStore {

    instanceName
    instanceHash

    constructor(redisClient, scriptManager = new Scripto(redisClient)) {
        this._scriptManager = scriptManager;
        this._redisClient = redisClient;
        this._scriptManager.loadFromDir(path.join(__dirname, "lua-scripts"));
        shortid.characters("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_>");

        //local functions
        this.initialize = this.initialize.bind(this);
        this.write = this.write.bind(this);
        this._validateTransformParameters = this._validateTransformParameters.bind(this);
        this._settingsHash = this._settingsHash.bind(this);
        this._assembleKey = this._assembleKey.bind(this);
        this._parseRedisData = this._parseRedisData.bind(this);
        this._extractPartitionInfo = this._extractPartitionInfo.bind(this);
        this.readIndex = this.readIndex.bind(this);
        this.readPage = this.readPage.bind(this);
        this.purgeAcquire = this.purgeAcquire.bind(this);
        this.purgeRelease = this.purgeRelease.bind(this);
    }

    async initialize(orderedPartitionWidth = 120000n, partitionDistribution = name => name) {
        try {
            this._orderedPartitionWidth = BigInt(orderedPartitionWidth);
        }
        catch (err) {
            return Promise.reject("Parameter 'orderedPartitionWidth' is invalid: " + err.message);
        }

        if ({}.toString.call(partitionDistribution) !== '[object Function]') {
            return Promise.reject(`Invalid parameter "partitionDistribution" should be a function.`);
        }

        this.instanceHash = this._settingsHash({ "version": 1.0, "partitionWidth": this._orderedPartitionWidth.toString() });
        const serverTime = await this._redisClient.time();
        const serverTimeInMilliSeconds = BigInt(serverTime[0] + serverTime[1].substring(0, 3));
        await this._redisClient.set(this._assembleKey(EPOCHKey), serverTimeInMilliSeconds, SetOptionDonotOverwrite);
        this._epoch = await this._redisClient.get(this._assembleKey(EPOCHKey));
        this._epoch = parseInt(this._epoch, DecimalRadix);

        if (Number.isNaN(this._epoch)) {
            this._epoch = undefined;
            return Promise.reject(`Initialization Failed: EPOCH is misplaced with ${this._epoch}.`);
        }

        this._partitionDistribution = partitionDistribution;
        this.instanceName = shortid.generate();
        this._epoch = BigInt(this._epoch);
        return this._epoch;

    }

    async write(keyValuePairs) {

        if (this._epoch == null) {
            return Promise.reject("Please initialize the instance by calling 'initialize' first before any calls.");
        }

        const serverTime = await this._redisClient.time();
        const serverTimeInMilliSeconds = BigInt(serverTime[0] + serverTime[1].substring(0, 3));

        const transformed = this._validateTransformParameters(keyValuePairs, serverTimeInMilliSeconds);

        if (transformed.error !== null) {
            return Promise.reject(transformed.error);
        }

        let asyncCommands = [];
        transformed.payload.forEach((partition, partitionName) => {
            //TODO:Someday we can move this to MULTI-Redis Transaction, only promisying that call is tough across redis libraries.
            asyncCommands.push(this._redisClient.zadd(this._assembleKey(partitionName), ...partition.data));//Main partition table update.
            partition.partitionKey.forEach(partKey => asyncCommands.push(this._redisClient.zadd(this._assembleKey(partKey), partition.relativePartitionStart, partitionName)));//Indexing updating.
            asyncCommands.push(this._redisClient.zadd(this._assembleKey(RecentAcitivityKey), partition.relativeActivity, partitionName));//Recent activity
        });

        let results = await Promise.allSettled(asyncCommands);
        results.forEach(e => {
            if (e.status !== "fulfilled") {
                throw new Error("Failed to complete operation:" + e.reason);
            }
        });

        const info = await this._redisClient.info('Memory');
        const serverMemory = BigInt(info.split('\r\n')[1].split(':')[1]);
        return serverMemory;
    }

    _validateTransformParameters(keyValuePairs, serverTime) {
        const returnObject = { "error": null, payload: new Map() };
        const sampleIngestionTime = serverTime;
        const errors = [];
        let itemCounter = 0;

        if (keyValuePairs instanceof Map === false) {
            returnObject.error = `Parameter 'keyValuePairs' should be of type 'Map' instead of ${typeof (keyValuePairs)}`;
            return returnObject;
        }

        keyValuePairs.forEach((orderedMap, partitionKey) => {
            if (orderedMap instanceof Map === false) {
                errors.push(`Key "${partitionKey}" has element which is not of type "Map".`);
            }
            else if (partitionKey.length > SafeKeyNameLength) {
                errors.push(`Key "${partitionKey}" has name which extends character limit(${SafeKeyNameLength}).`);
            }
            else {
                orderedMap.forEach((item, sortKey) => {
                    if (itemCounter > safeMaxItemLimit) {
                        returnObject.error = `Sample size exceeded limit of ${safeMaxItemLimit}.`;
                        return returnObject;
                    }
                    sortKey = BigInt(sortKey);
                    const partitionStart = sortKey - (sortKey % this._orderedPartitionWidth);
                    const distributedPartitionName = `${this._partitionDistribution(partitionKey)}${Seperator}${partitionStart}${Seperator}${AccumalatingFlag}`;
                    const serializedItem = JSON.stringify({ 'p': item, 'u': `${sampleIngestionTime}-${this.instanceName}-${itemCounter}`, 't': `${partitionKey}` });
                    const relativeKeyFromPartitionStart = sortKey - partitionStart;
                    const epochRelativePartitionStart = this._epoch - partitionStart;
                    const epochRelativeActivityTime = sampleIngestionTime - this._epoch;
                    const orderedTable = returnObject.payload.get(distributedPartitionName) || { data: [], "relativePartitionStart": epochRelativePartitionStart, "relativeActivity": epochRelativeActivityTime, "partitionKey": new Set() };
                    orderedTable.data.push(relativeKeyFromPartitionStart);
                    orderedTable.data.push(serializedItem);
                    orderedTable.partitionKey.add(partitionKey);
                    returnObject.payload.set(distributedPartitionName, orderedTable);
                    itemCounter++;
                });
            }
        });

        if (itemCounter === 0 && errors.length == 0) {
            returnObject.error = `Parameter 'keyValuePairs' should contain atleast one item to insert.`;
            return returnObject;
        }

        if (errors.length > 0) {
            returnObject.error = "Parameter 'keyValuePairs' has multiple Errors: " + errors.join(' , ');
            return returnObject;
        }

        return returnObject;
    }

    _settingsHash(settings) {
        return Crypto.createHash("sha256").update(JSON.stringify(settings), "binary").digest("hex");
    }

    _assembleKey(key) {
        return `${this.instanceHash}${Seperator}${key}`;
    }

    async readIndex(partitionRanges) {

        if (this._epoch == null) {
            return Promise.reject("Please initialize the instance by calling 'initialize' first before any calls.");
        }

        if (partitionRanges instanceof Map === false) {
            return Promise.reject(`Parameter 'partitionRanges' should be of type 'Map' instead of ${typeof (partitionRanges)}`);
        }

        if (partitionRanges.size > safeIndexedTagsRead) {
            return Promise.reject(`Parameter 'partitionRanges' cannot have partitions more than ${safeIndexedTagsRead}.`);
        }

        const errors = [];
        const ranges = new Map();
        partitionRanges.forEach((range, partitionKey) => {
            let start, end;
            try {
                start = BigInt(range.start);
                start = start - (start % this._orderedPartitionWidth);
                start = this._epoch - start;
            }
            catch (error) {
                errors.push(`Invalid start range for ${partitionKey}: ${error.message}`);
                return;
            };
            try {
                end = BigInt(range.end);
                end = this._epoch - end;
            }
            catch (error) {
                errors.push(`Invalid end range for ${partitionKey}: ${error.message}`);
                return;
            };
            if (partitionKey.length > SafeKeyNameLength) {
                errors.push(`Key "${partitionKey}" has name which extends character limit(${SafeKeyNameLength}).`);
                return;
            }
            if (range.end < range.start) {
                errors.push(`Invalid range; start should be smaller than end for ${partitionKey}.`);
                return;
            }
            ranges.set(partitionKey, { "rangeStart": start, "rangeEnd": end, "actualStart": range.start, "actualEnd": range.end });
        });

        if (ranges.size === 0 && errors.length == 0) {
            return Promise.reject(`Parameter 'partitionRanges' should contain atleast one range for query.`);
        }

        if (errors.length > 0) {
            return Promise.reject("Parameter 'partitionRanges' has multiple Errors: " + errors.join(' , '));
        }

        let asyncCommands = [];
        ranges.forEach((range, partitionKey) => {
            asyncCommands.push((async () => {
                let entries = { "partitionKey": partitionKey, pages: [] };
                const response = await this._redisClient.zrangebyscore(this._assembleKey(partitionKey), range.rangeEnd, range.rangeStart, WITHSCORES);
                for (let index = 0; index < response.length; index += 2) {
                    entries.pages.push({ "page": response[index], "sortWeight": parseFloat(response[index + 1]), "start": range.actualStart, "end": range.actualEnd });
                }
                return entries;
            })());
        });
        let pages = new Map();
        let results = await Promise.allSettled(asyncCommands);
        results.forEach((result) => {
            if (result.status !== "fulfilled") {
                throw new Error("Failed to complete operation:" + result.reason);
            }
            pages.set(result.value.partitionKey, result.value.pages);
        });
        return pages;
    }

    async readPage(pagename, filter = (sortKey, partitionKey) => true) {
        let partitionStart;

        if (this._epoch == null) {
            return Promise.reject("Please initialize the instance by calling 'initialize' first before any calls.");
        }

        if (pagename == undefined || pagename.length == 0 || pagename.length > (SafeKeyNameLength * 2)) {
            return Promise.reject(`Parameter "pagename" should be a valid string with characters not exceeding ${SafeKeyNameLength * 2}.`);
        }
        try {
            partitionStart = this._extractPartitionInfo(pagename).start;
        }
        catch (error) {
            return Promise.reject(`Invalid 'pagename': ${error.message}`);
        };

        if ({}.toString.call(filter) !== '[object Function]') {
            return Promise.reject(`Invalid parameter "filter" should be a function.`);
        }

        const response = await this._redisClient.zrange(this._assembleKey(pagename), 0, -1, WITHSCORES);
        const returnMap = this._parseRedisData(response, partitionStart, filter);

        return returnMap;
    }

    _extractPartitionInfo(partitionName) {
        let seperatorIndex = partitionName.lastIndexOf(Seperator);
        partitionName = partitionName.substring(0, seperatorIndex);
        seperatorIndex = partitionName.lastIndexOf(Seperator);
        if (seperatorIndex < 0 || (seperatorIndex + 1) >= partitionName.length) {
            throw new Error("Seperator misplaced @" + seperatorIndex);
        }
        const start = BigInt(partitionName.substring(seperatorIndex + 1));
        const key = partitionName.substring(0, seperatorIndex);
        return { "start": start, "key": key };
    }

    _parseRedisData(redisData, partitionStart, filter = (sortKey, partitionKey) => true) {
        let returnMap = new Map();
        for (let index = 0; index < redisData.length; index += 2) {
            const sortKey = partitionStart + BigInt(redisData[index + 1]);
            const data = JSON.parse(redisData[index]);
            if (filter(sortKey, data.t)) {
                returnMap.set(Number(sortKey), data.p);
            }
        }
        return returnMap;
    }

    async purgeAcquire(partitionAgeThresholdInSeconds = 300, maxPartitionsToAcquire = 10, releaseTimeoutSeconds = 100) {

        if (this._epoch == null) {
            return Promise.reject("Please initialize the instance by calling 'initialize' first before any calls.");
        }

        try {
            partitionAgeThresholdInSeconds = BigInt(partitionAgeThresholdInSeconds);
        }
        catch (err) {
            return Promise.reject("Parameter 'partitionAgeThresholdInSeconds' is invalid: " + err.message);
        }
        try {
            maxPartitionsToAcquire = BigInt(maxPartitionsToAcquire);
        }
        catch (err) {
            return Promise.reject("Parameter 'maxPartitionsToAcquire' is invalid: " + err.message);
        }
        try {
            releaseTimeoutSeconds = BigInt(releaseTimeoutSeconds);
        }
        catch (err) {
            return Promise.reject("Parameter 'releaseTimeoutSeconds' is invalid: " + err.message);
        }

        if (partitionAgeThresholdInSeconds <= 0) {
            return Promise.reject("Parameter 'partitionAgeThresholdInSeconds' is invalid & should greater than 1.")
        }

        if (maxPartitionsToAcquire <= 0) {
            return Promise.reject("Parameter 'maxPartitionsToAcquire' is invalid & should greater than 1.")
        }

        if (releaseTimeoutSeconds <= 0) {
            return Promise.reject("Parameter 'releaseTimeoutSeconds' is invalid & should greater than 1.")
        }
        const keys = [
            this._assembleKey(RecentAcitivityKey),
            this._assembleKey(PendingPurgeKey)
        ];
        const args = [
            partitionAgeThresholdInSeconds,
            (this._epoch / 1000n),
            maxPartitionsToAcquire,
            releaseTimeoutSeconds,
            this.instanceName,
            this.instanceHash,
            Seperator,
            PurgeFlag
        ];

        const acquiredData = await new Promise((acc, rej) => {
            this._scriptManager.run(scriptNamePurgeAcquire, keys, args, (err, result) => {
                if (err !== null) {
                    return rej(err);
                }
                acc(result);
            });
        });

        return acquiredData.map(serializedData => {
            let acquiredPartitionInfo = JSON.parse(serializedData[0]);
            const acquiredPartitionName = acquiredPartitionInfo[0];
            const acquiredPartitionLog = acquiredPartitionInfo[1];
            acquiredPartitionInfo = this._extractPartitionInfo(acquiredPartitionName);
            acquiredPartitionInfo.name = acquiredPartitionName;
            acquiredPartitionInfo.releaseToken = serializedData[0];
            acquiredPartitionInfo.history = acquiredPartitionLog;
            acquiredPartitionInfo.data = this._parseRedisData(serializedData[1], acquiredPartitionInfo.start);
            return acquiredPartitionInfo;
        });

        // [
        //     {
        //         start: 0n,
        //         key: 'SerialTag',
        //         name: 'SerialTag-0-pur',
        //         releaseToken: '["SerialTag-0-pur",["LJPiT520WP"]]',
        //         history: [ 'LJPiT520WP' ],
        //         data: Map { 1 => 'One', 2 => 'Two', 3 => 'Three', 4 => 'Four' }
        //       }
        // ]
    }

    async purgeRelease(partitionName, partitionKey, purgeReleaseToken) {
        if (this._epoch == null) {
            return Promise.reject("Please initialize the instance by calling 'initialize' first before any calls.");
        }
        if (partitionName == undefined || partitionName === "") {
            return Promise.reject("Invalid parameter 'partitionName'.");
        }
        if (partitionKey == undefined || partitionKey === "") {
            return Promise.reject("Invalid parameter 'partitionKey'.");
        }
        if (purgeReleaseToken == undefined || purgeReleaseToken === "") {
            return Promise.reject("Invalid parameter 'purgeReleaseToken'.");
        }
        const keys = [
            this._assembleKey(PendingPurgeKey),
            this._assembleKey(partitionKey),
            this._assembleKey(partitionName),
            this._assembleKey(MonitoringKey)
        ];
        const args = [
            purgeReleaseToken,
            partitionName
        ];
        const response = await new Promise((acc, rej) => {
            this._scriptManager.run(scriptNamePurgeRelease, keys, args, (err, result) => {
                if (err !== null) {
                    return rej(err);
                }
                acc(result);
            });
        });
        return { "success": response[0], "rate": parseFloat(response[1]) }
    }
}


exports.Timeseries = SortedStore;