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
const SafeKeyNameLength = 200;
const WITHSCORES = "WITHSCORES";

class SortedStore {

    constructor(redisClient, scriptManager = new Scripto(redisClient)) {
        this._scriptManager = scriptManager;
        this._redisClient = redisClient;
        this._scriptManager.loadFromDir(path.join(__dirname, "scripts"));

        //local functions
        this.initialize = this.initialize.bind(this);
        this.write = this.write.bind(this);
        this._validateTransformParameters = this._validateTransformParameters.bind(this);
        this._settingsHash = this._settingsHash.bind(this);
        this._assembleKey = this._assembleKey.bind(this);
        this.readIndex = this.readIndex.bind(this);
        this.readPage = this.readPage.bind(this);
    }

    async initialize(orderedPartitionWidth = 86400000n) {
        this._orderedPartitionWidth = BigInt(orderedPartitionWidth);
        this.SettingsHash = this._settingsHash({ "version": 1.0, "partitionWidth": this._orderedPartitionWidth.toString() });
        await this._redisClient.set(this._assembleKey(EPOCHKey), Date.now().toString(DecimalRadix), SetOptionDonotOverwrite);
        this._epoch = await this._redisClient.get(this._assembleKey(EPOCHKey));
        this._epoch = parseInt(this._epoch, DecimalRadix);
        if (Number.isNaN(this._epoch)) {
            this._epoch = undefined;
            return Promise.reject(`Initialization Failed: EPOCH is misplaced with ${this._epoch}.`);
        }
        else {
            this._instanceIdentifier = shortid.generate();
            this._epoch = BigInt(this._epoch);
            return Promise.resolve(this._epoch);
        }
    }

    async write(keyValuePairs) {

        if (this._epoch == null) {
            return Promise.reject("Please initialize the instance by calling 'initialize' first before any calls.");
        }

        const transformed = this._validateTransformParameters(keyValuePairs);

        if (transformed.error !== null) {
            return Promise.reject(transformed.error);
        }

        let asyncCommands = [];
        transformed.payload.forEach((partition, partitionName) => {
            //TODO:Someday we can move this to MULTI-Redis Transaction, only promisying that call is tough across redis libraries.
            asyncCommands.push(this._redisClient.zadd(this._assembleKey(partitionName), ...partition.data));//Main partition table update.
            asyncCommands.push(this._redisClient.zadd(this._assembleKey(partition.partitionKey), partition.relativePartitionStart, partitionName));//Indexing updating.
            asyncCommands.push(this._redisClient.zadd(this._assembleKey(RecentAcitivityKey), partition.relativeActivity, partitionName));//Recent activity
        });

        let results = await Promise.allSettled(asyncCommands);
        results.forEach(e => {
            if (e.status !== "fulfilled") {
                throw new Error("Failed to complete operation:" + e.reason);
            }
        });

        return Promise.resolve(true);
    }

    _validateTransformParameters(keyValuePairs) {
        const returnObject = { "error": null, payload: new Map() };
        const sampleIngestionTime = BigInt(Date.now());
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
                    const partitionName = `${partitionKey}${Seperator}${partitionStart}`;
                    const serializedItem = JSON.stringify({ 'p': item, 'u': `${sampleIngestionTime}-${this._instanceIdentifier}-${itemCounter}` });
                    const relativeKeyFromPartitionStart = sortKey - partitionStart;
                    const epochRelativePartitionStart = this._epoch - partitionStart;
                    const epochRelativeActivityTime = sampleIngestionTime - this._epoch;
                    const orderedTable = returnObject.payload.get(partitionName) || { data: [], "relativePartitionStart": epochRelativePartitionStart, "relativeActivity": epochRelativeActivityTime, "partitionKey": partitionKey };
                    orderedTable.data.push(relativeKeyFromPartitionStart);
                    orderedTable.data.push(serializedItem);
                    returnObject.payload.set(partitionName, orderedTable);
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
        return `${this.SettingsHash}${Seperator}${key}`;
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
        return Promise.resolve(pages);
    }

    async readPage(pagename, start, end) {
        let partitionStart;
        if (this._epoch == null) {
            return Promise.reject("Please initialize the instance by calling 'initialize' first before any calls.");
        }

        if (pagename == undefined || pagename.length == 0 || pagename.length > (SafeKeyNameLength * 2)) {
            return Promise.reject(`Parameter "pagename" should be a valid string with characters not exceeding ${SafeKeyNameLength * 2}.`);
        }
        try {
            const seperatorIndex = pagename.lastIndexOf(Seperator);
            if (seperatorIndex < 0 || (seperatorIndex + 1) >= pagename.length) {
                throw new Error("Seperator misplaced @" + seperatorIndex);
            }
            partitionStart = BigInt(pagename.substring(seperatorIndex + 1));
        }
        catch (error) {
            return Promise.reject(`Invalid 'pagename': ${error.message}`);
        };
        try {
            start = BigInt(start);
        }
        catch (error) {
            return Promise.reject(`Invalid start range for ${pagename}: ${error.message}`);
        };
        try {
            end = BigInt(end);
        }
        catch (error) {
            return Promise.reject(`Invalid end range for ${pagename}: ${error.message}`);
        };

        let returnMap = new Map();
        const response = await this._redisClient.zrange(this._assembleKey(pagename), 0, -1, WITHSCORES);
        for (let index = 0; index < response.length; index += 2) {
            const sortKey = partitionStart + BigInt(response[index + 1]);
            if (start <= sortKey && sortKey <= end) {
                const item = JSON.parse(response[index]).p;
                returnMap.set(Number(sortKey), item);
            }
        }

        return Promise.resolve(returnMap);
    }
}


exports.Timeseries = SortedStore;