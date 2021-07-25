const Scripto = require("redis-scripto");
const path = require("path");

//Domain constants
const EPOCHKey = "EPOCH";
const SetOptionDonotOverwrite = "NX";
const DecimalRadix = 10;

class SortedStore {

    constructor(redisClient, scriptManager = new Scripto(redisClient)) {
        this._scriptManager = scriptManager;
        this._redisClient = redisClient;
        this._scriptManager.loadFromDir(path.join(__dirname, "scripts"));

        //local functions
        this.initialize = this.initialize.bind(this);
    }

    async initialize() {
        await this._redisClient.set(EPOCHKey, Date.now().toString(DecimalRadix), SetOptionDonotOverwrite);
        this._epoch = await this._redisClient.get(EPOCHKey);
        this._epoch = parseInt(this._epoch, DecimalRadix);
        if (Number.isNaN(this._epoch)) {
            return Promise.reject(`Initialization Failed: EPOCH is misplaced with ${this._epoch}.`);
        }
        else {
            return Promise.resolve(this._epoch);
        }
    }

}


exports.Timeseries = SortedStore;