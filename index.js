const Scripto = require("redis-scripto");
const shortid = require("shortid");
const path = require("path");

//Domain constants
const EPOCHKey = "EPOCH";
const SetOptionDonotOverwrite = "NX";
const DecimalRadix = 10;
const Seperator = "-";
const safeMaxItemLimit = 200000;
const PartitionWidth = 86400000n;
const RecentAcitivityKey = "RecentActivity";

class SortedStore {

    constructor(redisClient, scriptManager = new Scripto(redisClient)) {
        this._scriptManager = scriptManager;
        this._redisClient = redisClient;
        this._scriptManager.loadFromDir(path.join(__dirname, "scripts"));

        //local functions
        this.initialize = this.initialize.bind(this);
        this.write = this.write.bind(this);
        this._validateTransformParameters = this._validateTransformParameters.bind(this);

    }

    async initialize() {
        await this._redisClient.set(EPOCHKey, Date.now().toString(DecimalRadix), SetOptionDonotOverwrite);
        this._epoch = await this._redisClient.get(EPOCHKey);
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
        //TODO:Settings like Partition width etc can cause problem in multiinstance enviroment one possible option is to isolate new settings with different space to play in with hashing settings and prefixing keys 
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
            asyncCommands.push(this._redisClient.zadd(partitionName, ...partition.data));//Main partition table update.
            asyncCommands.push(this._redisClient.zadd(partition.partitionKey, partition.relativePartitionStart, partitionName));//Indexing updating.
            asyncCommands.push(this._redisClient.zadd(RecentAcitivityKey, partition.relativeActivity, partitionName));//Recent activity
        });

        await Promise.allSettled(asyncCommands);

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
            else {
                orderedMap.forEach((item, sortKey) => {
                    if (itemCounter > safeMaxItemLimit) {
                        returnObject.error = `Sample size exceeded limit of ${safeMaxItemLimit}.`;
                        return returnObject;
                    }
                    sortKey = BigInt(sortKey);
                    const partitionStart = sortKey - (sortKey % PartitionWidth);
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


}


exports.Timeseries = SortedStore;