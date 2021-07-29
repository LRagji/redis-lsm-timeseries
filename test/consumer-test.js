const assert = require('assert');
const Crypto = require("crypto");
const localRedisConnectionString = "redis://127.0.0.1:6379/";
const redisClient = require("./get-me-redis-client")(localRedisConnectionString);
const targetType = require('../index').Timeseries;
let target = null;

describe('Timeseries consumer tests', function () {
    this.beforeAll(async function () {
        target = new targetType(redisClient);
    });
    this.afterAll(async function () {
        await redisClient.quit();
    });
    this.beforeEach(async function () {
        //Clean all keys
        await target._redisClient.flushall();
    });

    it('Should initialize correctly & return EPOCH', async function () {

        //SETUP
        let actualEPOCH = await target.initialize();
        let expectedEPOCH = await redisClient.get(target.SettingsHash + "-EPOCH");

        //VERIFY
        assert.strictEqual(actualEPOCH.toString(), expectedEPOCH);
    });

    it('Should fail with initialization exception when EPOCH is set to invalid', async function () {

        //SETUP
        let hash = Crypto.createHash("sha256").update(JSON.stringify({ "version": 1.0, "partitionWidth": 86400000n.toString() }), "binary").digest("base64")
        await redisClient.set(hash + "-EPOCH", "Laukik");//This is to simulate key is set but not epoch i.e:Timestamp ;

        //VERIFY
        await assert.rejects(target.initialize, err => assert.strictEqual(err, 'Initialization Failed: EPOCH is misplaced with undefined.') == undefined);
    });

    it('Should not allow write when initialize is not called', async function () {

        //VERIFY
        await assert.rejects(() => target.write(new Map()), err => assert.strictEqual(err, "Please initialize the instance by calling 'initialize' first before any calls.") == undefined);

    });

    it('Should not allow write when input parameter is not a map', async function () {

        //SETUP
        await target.initialize();

        //VERIFY
        await assert.rejects(() => target.write([]), err => assert.strictEqual(err, "Parameter 'keyValuePairs' should be of type 'Map' instead of object") == undefined);

        await assert.rejects(() => target.write(new Map([["Tag", []]])), err => assert.strictEqual(err, `Parameter 'keyValuePairs' has multiple Errors: Key "Tag" has element which is not of type "Map".`) == undefined);
    });

    it('Should not allow write when input doesnot contain single item', async function () {
        //VERIFY
        await assert.rejects(() => target.write(new Map([["Empty", new Map()]])), err => assert.strictEqual(err, "Parameter 'keyValuePairs' should contain atleast one item to insert.") == undefined);
    });

    it('Should not allow write when samples are more than 2000 samples', async function () {

        //SETUP
        await target.initialize();
        let orderedData = new Map();
        let startDate = Date.now();
        for (let orderCounter = 0; orderCounter < 10; orderCounter++) {
            orderedData.set((startDate + orderCounter), orderCounter.toString());
        }
        let inputData = new Map();
        for (let partitionCounter = 0; partitionCounter < 100000; partitionCounter++) {
            inputData.set(`Tag-${partitionCounter}`, orderedData);
        }

        //VERIFY
        await assert.rejects(() => target.write(inputData), err => assert.strictEqual(err, "Sample size exceeded limit of 2000.") == undefined);

    });

    it('Should write correct data when presented', async function () {

        //SETUP
        const partitionWidth = 86400000;
        const recentActivityKey = "RecentActivity";
        const Seperator = "-";
        const EPOCH = parseInt(await target.initialize());
        let orderedData = new Map();
        let startDate = Date.now();
        for (let orderCounter = 0; orderCounter < 2000; orderCounter++) {
            orderedData.set((startDate + orderCounter), orderCounter.toString());
        }
        let inputData = new Map();
        for (let partitionCounter = 0; partitionCounter < 1; partitionCounter++) {
            inputData.set(`Tag-${partitionCounter}`, orderedData);
        }

        //EXECUTE
        const returnValue = await target.write(inputData);

        //VERIFY
        assert.strictEqual(true, returnValue);
        let tags = Array.from(inputData.keys());
        for (let tagCounter = 0; tagCounter < tags.length; tagCounter++) {
            let samples = [];
            const tagName = tags[tagCounter];
            inputData.get(tagName).forEach((sample, time) => {
                samples.push({ "t": time, "s": sample });
            });

            for (let sampleCounter = 0; sampleCounter < samples.length; sampleCounter++) {
                let sample = samples[sampleCounter];
                const partitionStart = sample.t - (sample.t % partitionWidth);
                const partitionName = `${tagName}${Seperator}${partitionStart}`;
                const score = sample.t - partitionStart;
                let errorMessage = `${partitionName} for score ${score} `;
                //Data
                let redisData = await redisClient.zrangebyscore((target.SettingsHash + "-" + partitionName), score, score);
                redisData = JSON.parse(redisData[0]);
                assert.strictEqual(sample.s, redisData.p, (errorMessage + `has different data E:${sample.s} A:${redisData.p}.`));

                //Index
                const indexScore = EPOCH - partitionStart;
                redisData = await redisClient.zrangebyscore((target.SettingsHash + "-" + tagName), indexScore, indexScore);
                assert.strictEqual(partitionName, redisData[0], (errorMessage + `has different Index E:${partitionName} A:${redisData[0]}.`));

                //Recent Activity
                redisData = await redisClient.zrank((target.SettingsHash + "-" + recentActivityKey), partitionName);
                assert.strictEqual(true, parseInt(redisData) > -1, (errorMessage + ` doesnt have :${partitionName} in RecentActivity.`));
            };
        }

    });
});