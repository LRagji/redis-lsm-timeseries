const assert = require('assert');
const Crypto = require("crypto");
const localRedisConnectionString = "redis://127.0.0.1:6379/";
let redisClient;
const targetType = require('../index').Timeseries;
let target = null;

describe('Timeseries consumer tests', function () {

    this.beforeEach(async function () {
        redisClient = require("./get-me-redis-client")(localRedisConnectionString);
        await redisClient.flushall();
        target = new targetType(redisClient);
    });

    this.afterEach(async function () {
        await redisClient.quit();
        redisClient = null;
    });

    it('Should initialize correctly & return EPOCH', async function () {

        //SETUP
        let actualEPOCH = await target.initialize();
        let expectedEPOCH = await redisClient.get(target.instanceHash + "-EPOCH");

        //VERIFY
        assert.strictEqual(actualEPOCH.toString(), expectedEPOCH);
    });

    it('Should fail with initialization exception when EPOCH is set to invalid', async function () {

        //SETUP
        let hash = Crypto.createHash("sha256").update(JSON.stringify({ "version": 1.0, "partitionWidth": 120000n.toString() }), "binary").digest("hex")
        await redisClient.set(hash + "-EPOCH", "Laukik");//This is to simulate key is set but not epoch i.e:Timestamp ;

        //VERIFY
        await assert.rejects(target.initialize, err => assert.strictEqual(err, 'Initialization Failed: EPOCH is misplaced with undefined.') == undefined);
    });

    it('Should fail with initialization exception when partitionDistribution is set to invalid', async function () {

        //VERIFY
        await assert.rejects(() => target.initialize(undefined, "kkkk"), err => assert.strictEqual(err, 'Invalid parameter "partitionDistribution" should be a function.') == undefined);
    });

    it('Should fail with initialization exception when orderedPartitionWidth is set to invalid', async function () {

        //VERIFY
        await assert.rejects(() => target.initialize("ggg"), err => assert.strictEqual(err, "Parameter 'orderedPartitionWidth' is invalid: Cannot convert ggg to a BigInt") == undefined);
    });

    it('Should not allow write when initialize is not called', async function () {

        //VERIFY
        await assert.rejects(() => target.write(new Map()), err => assert.strictEqual(err, "Please initialize the instance by calling 'initialize' first before any calls.") == undefined);

    });

    it('Should not allow write when input parameter is not of a proper datatype', async function () {

        //SETUP
        await target.initialize();

        //VERIFY
        await assert.rejects(() => target.write([]), err => assert.strictEqual(err, "Parameter 'keyValuePairs' should be of type 'Map' instead of object") == undefined);

        await assert.rejects(() => target.write(new Map([["Tag", []]])), err => assert.strictEqual(err, `Parameter 'keyValuePairs' has multiple Errors: Key "Tag" has element which is not of type "Map".`) == undefined);
    });

    it('Should not allow write when input doesnot contain single item', async function () {
        //SETUP
        await target.initialize();

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

    it('Should not allow write when partition name is bigger than 200 characters', async function () {

        //SETUP
        await target.initialize();
        let orderedData = new Map();
        let startDate = Date.now();
        for (let orderCounter = 0; orderCounter < 10; orderCounter++) {
            orderedData.set((startDate + orderCounter), orderCounter.toString());
        }
        let inputData = new Map();
        for (let partitionCounter = 0; partitionCounter < 1; partitionCounter++) {
            inputData.set(`ipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumlorem:1234567890-${partitionCounter}`, orderedData);
        }

        //VERIFY
        await assert.rejects(() => target.write(inputData), err => assert.strictEqual(err, 'Parameter \'keyValuePairs\' has multiple Errors: Key "ipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumlorem:1234567890-0" has name which extends character limit(200).') == undefined);

    });

    it('Should write correct data when presented PERF', async function () {

        //SETUP
        const partitionWidth = 10;
        const recentActivityKey = "RecentActivity";
        const Seperator = "-";
        const acquiringFlag = "acc";
        const EPOCH = parseInt(await target.initialize(partitionWidth));
        let orderedData = new Map();
        let startDate = Date.now();
        for (let orderCounter = 0; orderCounter < 200; orderCounter++) {
            orderedData.set((startDate + orderCounter), orderCounter.toString());
        }
        let inputData = new Map();
        for (let partitionCounter = 0; partitionCounter < 10; partitionCounter++) {
            inputData.set(`TagDCJf38X0DrgIZNCgyp4+RZC0rkoLtvaUokoj7cKTE7MSomethings-${partitionCounter}`, orderedData);
        }

        //EXECUTE
        const serverMemoryBytes = await target.write(inputData);

        //VERIFY
        assert.strictEqual(true, serverMemoryBytes > 0n);
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
                const partitionName = `${tagName}${Seperator}${partitionStart}${Seperator}${acquiringFlag}`;
                const score = sample.t - partitionStart;
                let errorMessage = `${partitionName} for score ${score} `;
                //Data
                let redisData = await redisClient.zrangebyscore((target.instanceHash + "-" + partitionName), score, score);
                redisData = JSON.parse(redisData[0]);
                assert.strictEqual(sample.s, redisData.p, (errorMessage + `has different data E:${sample.s} A:${redisData.p}.`));

                //Index
                const indexScore = EPOCH - partitionStart;
                redisData = await redisClient.zrangebyscore((target.instanceHash + "-" + tagName), indexScore, indexScore);
                assert.strictEqual(partitionName, redisData[0], (errorMessage + `has different Index E:${partitionName} A:${redisData[0]}.`));

                //Recent Activity
                redisData = await redisClient.zrank((target.instanceHash + "-" + recentActivityKey), partitionName);
                assert.strictEqual(true, parseInt(redisData) > -1, (errorMessage + ` doesnt have :${partitionName} in RecentActivity.`));
            };
        }

    }).timeout(-1);

    it('Should write & read correct data when sample time is greater than EPOCH', async function () {

        //SETUP
        const partitionWidth = 10;
        const recentActivityKey = "RecentActivity";
        const Seperator = "-";
        const EPOCH = parseInt(await target.initialize(partitionWidth));
        const SampleOneTimestamp = EPOCH + 21;
        let inputData = new Map();
        inputData.set("GapTag", new Map([[SampleOneTimestamp, "One"]]));

        //EXECUTE
        const serverMemoryBytes = await target.write(inputData);

        //READ
        let ranges = new Map();
        ranges.set("GapTag", { start: EPOCH, end: SampleOneTimestamp });
        const readResult = await readData(ranges);

        //VERIFY
        assert.strictEqual(true, serverMemoryBytes > 0n);

        //Data
        const partitionStart = SampleOneTimestamp - (SampleOneTimestamp % partitionWidth);
        const partitionName = "GapTag" + Seperator + partitionStart.toString() + Seperator + "acc";
        const partitionKey = target._assembleKey(partitionName);
        const partionShouldExists = await redisClient.exists(partitionKey);
        assert.strictEqual(partionShouldExists, 1);
        const payloadScore = SampleOneTimestamp - partitionStart
        let actualPayload = await redisClient.zrangebyscore(partitionKey, payloadScore, payloadScore);
        assert.strictEqual(actualPayload.length, 1);
        actualPayload = JSON.parse(actualPayload[0]);
        assert.strictEqual(actualPayload.p, "One");
        const instanceName = actualPayload.u.split(Seperator)[1];
        assert.strictEqual(instanceName, target.instanceName);

        //Index
        const indexKey = target._assembleKey("GapTag");
        const payloadInsertTime = BigInt(actualPayload.u.split(Seperator)[0]);
        assert.strictEqual(payloadInsertTime >= BigInt(EPOCH), true, "Difference was " + (BigInt(EPOCH) - payloadInsertTime));
        const indexShouldExists = await redisClient.exists(indexKey);
        assert.strictEqual(indexShouldExists, 1);
        const indexScore = EPOCH - partitionStart;
        const indexEntry = await redisClient.zrangebyscore(indexKey, indexScore, indexScore);
        assert.strictEqual(indexEntry.length, 1);
        assert.strictEqual(indexEntry[0], partitionName);

        //RecentActivity
        const racKey = target._assembleKey(recentActivityKey);
        const recentActivityShouldExists = await redisClient.exists(racKey);
        assert.strictEqual(recentActivityShouldExists, 1);
        const racScore = payloadInsertTime - BigInt(EPOCH);
        const racEntry = await redisClient.zrangebyscore(racKey, racScore, racScore);
        assert.strictEqual(racEntry.length, 1);
        assert.strictEqual(racEntry[0], partitionName);

        //Read results
        assert.deepStrictEqual(readResult, inputData);

    });

    it('Should write & read correct data when sample time is smaller than EPOCH', async function () {

        //SETUP
        const partitionWidth = 10;
        const recentActivityKey = "RecentActivity";
        const Seperator = "-";
        const EPOCH = parseInt(await target.initialize(partitionWidth));
        const SampleOneTimestamp = EPOCH - 21;
        let inputData = new Map();
        inputData.set("GapTag", new Map([[SampleOneTimestamp, "One"]]));

        //EXECUTE
        const serverMemoryBytes = await target.write(inputData);

        //READ
        let ranges = new Map();
        ranges.set("GapTag", { start: SampleOneTimestamp, end: EPOCH });
        const readResult = await readData(ranges);

        //VERIFY
        assert.strictEqual(true, serverMemoryBytes > 0n);

        //Data
        const partitionStart = SampleOneTimestamp - (SampleOneTimestamp % partitionWidth);
        const partitionName = "GapTag" + Seperator + partitionStart.toString() + Seperator + "acc";
        const partitionKey = target._assembleKey(partitionName);
        const partionShouldExists = await redisClient.exists(partitionKey);
        assert.strictEqual(partionShouldExists, 1);
        const payloadScore = SampleOneTimestamp - partitionStart
        let actualPayload = await redisClient.zrangebyscore(partitionKey, payloadScore, payloadScore);
        assert.strictEqual(actualPayload.length, 1);
        actualPayload = JSON.parse(actualPayload[0]);
        assert.strictEqual(actualPayload.p, "One");
        const instanceName = actualPayload.u.split(Seperator)[1];
        assert.strictEqual(instanceName, target.instanceName);

        //Index
        const indexKey = target._assembleKey("GapTag");
        const payloadInsertTime = BigInt(actualPayload.u.split(Seperator)[0]);
        assert.strictEqual(payloadInsertTime >= EPOCH, true, `InsertTime ${payloadInsertTime} EPOCH ${EPOCH}`);
        const indexShouldExists = await redisClient.exists(indexKey);
        assert.strictEqual(indexShouldExists, 1);
        const indexScore = EPOCH - partitionStart;
        const indexEntry = await redisClient.zrangebyscore(indexKey, indexScore, indexScore);
        assert.strictEqual(indexEntry.length, 1);
        assert.strictEqual(indexEntry[0], partitionName);

        //RecentActivity
        const racKey = target._assembleKey(recentActivityKey);
        const recentActivityShouldExists = await redisClient.exists(racKey);
        assert.strictEqual(recentActivityShouldExists, 1);
        const racScore = payloadInsertTime - BigInt(EPOCH);
        const racEntry = await redisClient.zrangebyscore(racKey, racScore, racScore);
        assert.strictEqual(racEntry.length, 1);
        assert.strictEqual(racEntry[0], partitionName);

        //Read results
        assert.deepStrictEqual(readResult, inputData);

    });

    it('Should write & read correct data when multiple writes have occured', async function () {

        //SETUP
        const partitionWidth = 10;
        const recentActivityKey = "RecentActivity";
        const Seperator = "-";
        const EPOCH = parseInt(await target.initialize(partitionWidth));
        const SampleOneTimestamp = EPOCH + 21;
        const SampleTwoTimestamp = SampleOneTimestamp - 100;
        let inputData = new Map();
        inputData.set("GapTag", new Map([[SampleOneTimestamp, "One"]]));
        inputData.set("GapTag2", new Map([[SampleTwoTimestamp, "One2"]]));

        //EXECUTE
        const serverMemoryBytes = await target.write(inputData);

        //READ
        let ranges = new Map();
        ranges.set("GapTag", { start: SampleTwoTimestamp, end: SampleOneTimestamp });
        ranges.set("GapTag2", { start: SampleTwoTimestamp, end: SampleOneTimestamp });
        const readResult = await readData(ranges);

        //VERIFY
        assert.strictEqual(true, serverMemoryBytes > 0n);

        //Data
        const partitionStart = SampleOneTimestamp - (SampleOneTimestamp % partitionWidth);
        const partitionName = "GapTag" + Seperator + partitionStart.toString() + Seperator + "acc";
        const partitionKey = target._assembleKey(partitionName);
        const partionShouldExists = await redisClient.exists(partitionKey);
        assert.strictEqual(partionShouldExists, 1);
        const payloadScore = SampleOneTimestamp - partitionStart
        let actualPayload = await redisClient.zrangebyscore(partitionKey, payloadScore, payloadScore);
        assert.strictEqual(actualPayload.length, 1);
        actualPayload = JSON.parse(actualPayload[0]);
        assert.strictEqual(actualPayload.p, "One");
        const instanceName = actualPayload.u.split(Seperator)[1];
        assert.strictEqual(instanceName, target.instanceName);

        //Index
        const indexKey = target._assembleKey("GapTag");
        const payloadInsertTime = BigInt(actualPayload.u.split(Seperator)[0]);
        assert.strictEqual(payloadInsertTime >= EPOCH, true);
        const indexShouldExists = await redisClient.exists(indexKey);
        assert.strictEqual(indexShouldExists, 1);
        const indexScore = EPOCH - partitionStart;
        const indexEntry = await redisClient.zrangebyscore(indexKey, indexScore, indexScore);
        assert.strictEqual(indexEntry.length, 1);
        assert.strictEqual(indexEntry[0], partitionName);

        //RecentActivity
        const racKey = target._assembleKey(recentActivityKey);
        const recentActivityShouldExists = await redisClient.exists(racKey);
        assert.strictEqual(recentActivityShouldExists, 1);
        const racScore = payloadInsertTime - BigInt(EPOCH);
        const racEntry = await redisClient.zrangebyscore(racKey, racScore, racScore);
        assert.strictEqual(racEntry.length, 2);
        assert.strictEqual(racEntry.indexOf(partitionName) > -1, true);

        //Read results
        assert.deepStrictEqual(readResult, inputData);

    });

    it('Should read correct indexes in reverse order when correct data been written', async function () {

        //SETUP
        const partitionWidth = 5;
        const sortkeyLength = 10;
        await target.initialize(partitionWidth);
        let orderedData = new Map();
        let pageSets = new Set();
        let startDate = Date.now();
        for (let orderCounter = 0; orderCounter < sortkeyLength; orderCounter++) {
            const time = (startDate + orderCounter);
            orderedData.set(time, orderCounter.toString());
            pageSets.add(time - (time % partitionWidth));
        }
        let inputData = new Map();
        let ranges = new Map();
        for (let partitionCounter = 0; partitionCounter < 100; partitionCounter++) {
            inputData.set(`Tag-${partitionCounter}`, orderedData);
            ranges.set(`Tag-${partitionCounter}`, { start: (startDate - sortkeyLength), end: (startDate + sortkeyLength) });
        }

        //WRITE
        await target.write(inputData);

        //READ
        const pages = await target.readIndex(ranges);

        //VERIFY
        ranges.forEach((range, key) => {
            const currentPage = pages.get(key);
            const expected = Array.from(pageSets).reduce((acc, e) => {
                acc.push(`${key}-${e}-pur`,`${key}-${e}-acc`);
                return acc;
            }, []).reverse();
            assert.notStrictEqual(undefined, currentPage, `Page not found for ${key} between ${range.start} to ${range.end}.`);
            assert.deepStrictEqual(currentPage.map(e => e.page), expected, `Not all pages found E:${expected} A:${currentPage} for ${key} between ${range.start} to ${range.end}.`);
            //Verify weight odering.
            currentPage.reduce((pPage, cPage) => {
                assert.strictEqual(pPage.sortWeight <= cPage.sortWeight, true, `Current weight:${cPage.sortWeight} Previous weight:${pPage.sortWeight} for ${key} between ${range.start} to ${range.end}.`);
                return cPage;
            })
        });
    });

    it('Should not allow read when initialize is not called', async function () {

        //VERIFY
        await assert.rejects(() => target.readIndex(new Map()), err => assert.strictEqual(err, "Please initialize the instance by calling 'initialize' first before any calls.") == undefined);

    });

    it('Should not allow read when input parameter is not of a proper datatype', async function () {

        //SETUP
        await target.initialize();

        //VERIFY
        await assert.rejects(() => target.readIndex([]), err => assert.strictEqual(err, "Parameter 'partitionRanges' should be of type 'Map' instead of object") == undefined);
    });

    it('Should not allow read when requested partitions are more than 100', async function () {

        //SETUP
        const partitionWidth = 5;
        const sortkeyLength = 10;
        await target.initialize(partitionWidth);
        let orderedData = new Map();
        let startDate = Date.now();
        for (let orderCounter = 0; orderCounter < sortkeyLength; orderCounter++) {
            const time = (startDate + orderCounter);
            orderedData.set(time, orderCounter.toString());
        }
        let ranges = new Map();
        for (let partitionCounter = 0; partitionCounter < 102; partitionCounter++) {
            ranges.set(`Tag-${partitionCounter}`, { start: (startDate - sortkeyLength), end: (startDate + sortkeyLength) });
        }

        //VERIFY
        await assert.rejects(() => target.readIndex(ranges), err => assert.strictEqual(err, "Parameter 'partitionRanges' cannot have partitions more than 100.") == undefined);
    });

    it('Should not allow read when requested ranges have start missing', async function () {

        //SETUP
        const partitionWidth = 5;
        const sortkeyLength = 10;
        await target.initialize(partitionWidth);
        let orderedData = new Map();
        let startDate = Date.now();
        for (let orderCounter = 0; orderCounter < sortkeyLength; orderCounter++) {
            const time = (startDate + orderCounter);
            orderedData.set(time, orderCounter.toString());
        }
        let ranges = new Map();
        for (let partitionCounter = 0; partitionCounter < 2; partitionCounter++) {
            ranges.set(`Tag-${partitionCounter}`, {});
        }

        //VERIFY
        await assert.rejects(() => target.readIndex(ranges), err => assert.strictEqual(err, "Parameter 'partitionRanges' has multiple Errors: Invalid start range for Tag-0: Cannot convert undefined to a BigInt , Invalid start range for Tag-1: Cannot convert undefined to a BigInt") == undefined);
    });

    it('Should not allow read when requested ranges have end missing', async function () {

        //SETUP
        const partitionWidth = 5;
        const sortkeyLength = 10;
        await target.initialize(partitionWidth);
        let orderedData = new Map();
        let startDate = Date.now();
        for (let orderCounter = 0; orderCounter < sortkeyLength; orderCounter++) {
            const time = (startDate + orderCounter);
            orderedData.set(time, orderCounter.toString());
        }
        let ranges = new Map();
        for (let partitionCounter = 0; partitionCounter < 2; partitionCounter++) {
            ranges.set(`Tag-${partitionCounter}`, { start: (startDate - sortkeyLength) });
        }

        //VERIFY
        await assert.rejects(() => target.readIndex(ranges), err => assert.strictEqual(err, "Parameter 'partitionRanges' has multiple Errors: Invalid end range for Tag-0: Cannot convert undefined to a BigInt , Invalid end range for Tag-1: Cannot convert undefined to a BigInt") == undefined);
    });

    it('Should not allow read when requested ranges have start and end donot form a range', async function () {

        //SETUP
        const partitionWidth = 5;
        const sortkeyLength = 10;
        await target.initialize(partitionWidth);
        let orderedData = new Map();
        let startDate = Date.now();
        for (let orderCounter = 0; orderCounter < sortkeyLength; orderCounter++) {
            const time = (startDate + orderCounter);
            orderedData.set(time, orderCounter.toString());
        }
        let ranges = new Map();
        for (let partitionCounter = 0; partitionCounter < 2; partitionCounter++) {
            ranges.set(`Tag-${partitionCounter}`, { end: (startDate - sortkeyLength), start: (startDate + sortkeyLength) });
        }

        //VERIFY
        await assert.rejects(() => target.readIndex(ranges), err => assert.strictEqual(err, "Parameter 'partitionRanges' has multiple Errors: Invalid range; start should be smaller than end for Tag-0. , Invalid range; start should be smaller than end for Tag-1.") == undefined);
    });

    it('Should not allow read when no range is requested', async function () {

        //SETUP
        const partitionWidth = 5;
        const sortkeyLength = 10;
        await target.initialize(partitionWidth);
        let orderedData = new Map();
        let startDate = Date.now();
        for (let orderCounter = 0; orderCounter < sortkeyLength; orderCounter++) {
            const time = (startDate + orderCounter);
            orderedData.set(time, orderCounter.toString());
        }
        let ranges = new Map();
        for (let partitionCounter = 0; partitionCounter < 0; partitionCounter++) {
            ranges.set(`Tag-${partitionCounter}`, { end: (startDate - sortkeyLength), start: (startDate + sortkeyLength) });
        }

        //VERIFY
        await assert.rejects(() => target.readIndex(ranges), err => assert.strictEqual(err, "Parameter 'partitionRanges' should contain atleast one range for query.") == undefined);
    });

    it('Should not allow read when partition key is above 200 characters', async function () {

        //SETUP
        const partitionWidth = 5;
        const sortkeyLength = 10;
        await target.initialize(partitionWidth);
        let orderedData = new Map();
        let startDate = Date.now();
        for (let orderCounter = 0; orderCounter < sortkeyLength; orderCounter++) {
            const time = (startDate + orderCounter);
            orderedData.set(time, orderCounter.toString());
        }

        let ranges = new Map();
        for (let partitionCounter = 0; partitionCounter < 1; partitionCounter++) {
            ranges.set(`ipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumlorem:1234567890-${partitionCounter}`, { end: (startDate - sortkeyLength), start: (startDate + sortkeyLength) });
        }

        //VERIFY
        await assert.rejects(() => target.readIndex(ranges), err => assert.strictEqual(err, 'Parameter \'partitionRanges\' has multiple Errors: Key "ipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumloreipsumlorem:1234567890-0" has name which extends character limit(200).') == undefined);
    });

    it('Should read correct data for gaps and sequential data when bigger read range is provided', async function () {

        //SETUP
        const partitionWidth = 5;
        let inputData = new Map();
        let ranges = new Map();
        let expected = new Map();

        inputData.set("GapTag", new Map([[1, "One"], [2, "Two"], [10, "Ten"], [20, "Twenty"]]));
        inputData.set("SerialTag", new Map([[1, "One"], [2, "Two"], [3, "Three"], [4, "Four"]]));

        ranges.set("GapTag", { start: 0, end: 50 });
        ranges.set("SerialTag", { start: 0, end: 50 });

        expected = inputData;

        await target.initialize(partitionWidth);

        //WRITE
        await target.write(inputData);

        //READ
        const result = await readData(ranges);

        //VERIFY
        assert.deepStrictEqual(result, expected);
    });

    it('Should read correct data for gaps and sequential data when read range is outside presented data', async function () {

        //SETUP
        const partitionWidth = 5;
        let inputData = new Map();
        let ranges = new Map();
        let expected = new Map();

        inputData.set("GapTag", new Map([[1, "One"], [2, "Two"], [10, "Ten"], [20, "Twenty"]]));
        inputData.set("SerialTag", new Map([[1, "One"], [2, "Two"], [3, "Three"], [4, "Four"]]));

        ranges.set("GapTag", { start: 50, end: 100 });
        ranges.set("SerialTag", { start: 50, end: 50 });

        await target.initialize(partitionWidth);

        //WRITE
        await target.write(inputData);

        //READ
        const result = await readData(ranges);

        //VERIFY
        assert.deepStrictEqual(result, expected);
    });

    it('Should read correct data for gaps and sequential data when read range is single data point', async function () {

        //SETUP
        const partitionWidth = 5;
        let inputData = new Map();
        let ranges = new Map();
        let expected = new Map();

        inputData.set("GapTag", new Map([[1, "One"], [2, "Two"], [10, "Ten"], [20, "Twenty"]]));
        inputData.set("SerialTag", new Map([[1, "One"], [2, "Two"], [3, "Three"], [4, "Four"]]));

        ranges.set("GapTag", { start: 1, end: 1 });
        ranges.set("SerialTag", { start: 4, end: 4 });

        expected.set("GapTag", new Map([[1, "One"]]));
        expected.set("SerialTag", new Map([[4, "Four"]]));

        await target.initialize(partitionWidth);

        //WRITE
        await target.write(inputData);

        //READ
        const result = await readData(ranges);

        //VERIFY
        assert.deepStrictEqual(result, expected);
    });

    it('Should read correct data for gaps and sequential data when read range is partially overlapping with data', async function () {

        //SETUP
        const partitionWidth = 5;
        let inputData = new Map();
        let ranges = new Map();
        let expected = new Map();

        inputData.set("GapTag", new Map([[1, "One"], [2, "Two"], [10, "Ten"], [20, "Twenty"]]));
        inputData.set("SerialTag", new Map([[1, "One"], [2, "Two"], [3, "Three"], [4, "Four"]]));

        ranges.set("GapTag", { start: 0, end: 1 });
        ranges.set("SerialTag", { start: 4, end: 10 });

        expected.set("GapTag", new Map([[1, "One"]]));
        expected.set("SerialTag", new Map([[4, "Four"]]));

        await target.initialize(partitionWidth);

        //WRITE
        await target.write(inputData);

        //READ
        const result = await readData(ranges);

        //VERIFY
        assert.deepStrictEqual(result, expected);
    });

    it('Should read correct data for gaps and sequential data when read range is subset of the data range', async function () {

        //SETUP
        const partitionWidth = 5;
        let inputData = new Map();
        let ranges = new Map();
        let expected = new Map();

        inputData.set("GapTag", new Map([[1, "One"], [2, "Two"], [10, "Ten"], [20, "Twenty"]]));
        inputData.set("SerialTag", new Map([[1, "One"], [2, "Two"], [3, "Three"], [4, "Four"]]));

        ranges.set("GapTag", { start: 2, end: 10 });
        ranges.set("SerialTag", { start: 3, end: 4 });

        expected.set("GapTag", new Map([[2, "Two"], [10, "Ten"]]));
        expected.set("SerialTag", new Map([[3, "Three"], [4, "Four"]]));

        await target.initialize(partitionWidth);

        //WRITE
        await target.write(inputData);

        //READ
        const result = await readData(ranges);

        //VERIFY
        assert.deepStrictEqual(result, expected);
    });

    it('Should read chunk of data when correct data when presented', async function () {

        //SETUP
        const partitionWidth = 10;
        await target.initialize(partitionWidth)
        let orderedData = new Map();
        let startDate = Date.now();
        for (let orderCounter = 0; orderCounter < 200; orderCounter++) {
            orderedData.set((startDate + orderCounter), orderCounter.toString());
        }
        let inputData = new Map();
        let ranges = new Map();
        for (let partitionCounter = 0; partitionCounter < 10; partitionCounter++) {
            inputData.set(`TagDCJf38X0DrgIZNCgyp4+RZC0rkoLtvaUokoj7cKTE7MSomethings-${partitionCounter}`, orderedData);
            ranges.set(`TagDCJf38X0DrgIZNCgyp4+RZC0rkoLtvaUokoj7cKTE7MSomethings-${partitionCounter}`, { start: startDate, end: (startDate * 2) });
        }

        //EXECUTE
        const returnValue = await target.write(inputData);

        //READ
        const result = await readData(ranges);

        //VERIFY
        assert.deepStrictEqual(result, inputData);

    });

    it('Should not allow readPage when not initialized', async function () {

        //VERIFY
        await assert.rejects(() => target.readPage("", 0, 0), err => assert.strictEqual(err, "Please initialize the instance by calling 'initialize' first before any calls.") == undefined);

    });

    it('Should not allow readPage when incorrect page info is passed', async function () {
        //SETUP
        await target.initialize();

        //VERIFY when pagename is empty string
        await assert.rejects(() => target.readPage("", 0, 0), err => assert.strictEqual(err, `Parameter "pagename" should be a valid string with characters not exceeding 400.`) == undefined);

        //VERIFY when pagename doesnt has seperator
        await assert.rejects(() => target.readPage("Laukik", 0, 0), err => assert.strictEqual(err, `Invalid 'pagename': Seperator misplaced @-1`) == undefined);

        //VERIFY when pagename has seperator towards end
        await assert.rejects(() => target.readPage("Laukik-", 0, 0), err => assert.strictEqual(err, `Invalid 'pagename': Seperator misplaced @-1`) == undefined);

    });

    it('Should not allow readPage when incorrect filter parameter is specified', async function () {
        //SETUP
        await target.initialize();

        //VERIFY when filter is not function
        await assert.rejects(() => target.readPage("Laukik-9-acc", 0), err => assert.strictEqual(err, `Invalid parameter "filter" should be a function.`) == undefined);

    });

    it('Should read correct updated data.', async function () {

        //SETUP
        const partitionWidth = 5;
        let inputData = new Map();
        let ranges = new Map();
        let expected = new Map();

        inputData.set("GapTag", new Map([[1, "One"], [1, "Two"], [1, "Ten"], [1, "Twenty"]]));
        inputData.set("SerialTag", new Map([[55, "One"], [55, "Two"], [56, "Three"], [55, "Four"]]));

        ranges.set("GapTag", { start: 0, end: 10 });
        ranges.set("SerialTag", { start: 0, end: 100 });

        expected.set("GapTag", new Map([[1, "Twenty"]]));
        expected.set("SerialTag", new Map([[55, "Four"], [56, "Three"]]));

        await target.initialize(partitionWidth);

        //WRITE
        await target.write(inputData);

        //READ
        const result = await readData(ranges);

        //VERIFY
        assert.deepStrictEqual(result, expected);
    });

    it('Should mark partition for purging when correct parameters are presented.', async function () {

        //SETUP
        const partitionWidth = 5;
        let inputData = new Map();
        const recentActivityKey = "RecentActivity";
        const PendingPurgeKey = "Pen"

        inputData.set("GapTag", new Map([[1, "One"], [2, "Two"], [10, "Ten"], [20, "Twenty"]]));
        inputData.set("SerialTag", new Map([[1, "One"], [2, "Two"], [3, "Three"], [4, "Four"]]));

        await target.initialize(partitionWidth);

        //WRITE
        const bytes = await target.write(inputData);

        //Killtime
        await new Promise((acc, rej) => setTimeout(acc, 1500));

        //PURGE
        const acquiredPartitions = await target.purgeAcquire(1, 10, 1000);
        const recentActivityContainsPartitionKey1 = await redisClient.zrank(target._assembleKey(recentActivityKey), acquiredPartitions[0].name);
        const recentActivityContainsPartitionKey2 = await redisClient.zrank(target._assembleKey(recentActivityKey), acquiredPartitions[1].name);
        const recentActivityContainsPartitionKey3 = await redisClient.zrank(target._assembleKey(recentActivityKey), acquiredPartitions[2].name);
        const recentActivityContainsPartitionKey4 = await redisClient.zrank(target._assembleKey(recentActivityKey), acquiredPartitions[3].name);

        const recentActivityContainsPartitionOldKey1 = await redisClient.zrank(target._assembleKey(recentActivityKey), acquiredPartitions[0].name.replace("pur", "acc"));
        const recentActivityContainsPartitionOldKey2 = await redisClient.zrank(target._assembleKey(recentActivityKey), acquiredPartitions[1].name.replace("pur", "acc"));
        const recentActivityContainsPartitionOldKey3 = await redisClient.zrank(target._assembleKey(recentActivityKey), acquiredPartitions[2].name.replace("pur", "acc"));
        const recentActivityContainsPartitionOldKey4 = await redisClient.zrank(target._assembleKey(recentActivityKey), acquiredPartitions[3].name.replace("pur", "acc"));

        const partitionKeyExists1 = await redisClient.exists(target._assembleKey(acquiredPartitions[0].name));
        const partitionKeyExists2 = await redisClient.exists(target._assembleKey(acquiredPartitions[1].name));
        const partitionKeyExists3 = await redisClient.exists(target._assembleKey(acquiredPartitions[2].name));
        const partitionKeyExists4 = await redisClient.exists(target._assembleKey(acquiredPartitions[3].name));

        const partitionKeyExistsOld1 = await redisClient.exists(target._assembleKey(acquiredPartitions[0].name.replace("pur", "acc")));
        const partitionKeyExistsOld2 = await redisClient.exists(target._assembleKey(acquiredPartitions[1].name.replace("pur", "acc")));
        const partitionKeyExistsOld3 = await redisClient.exists(target._assembleKey(acquiredPartitions[2].name.replace("pur", "acc")));
        const partitionKeyExistsOld4 = await redisClient.exists(target._assembleKey(acquiredPartitions[3].name.replace("pur", "acc")));

        const pendingContainsPartitionKey1 = await redisClient.zscore(target._assembleKey(PendingPurgeKey), acquiredPartitions[0].releaseToken);
        const pendingContainsPartitionKey2 = await redisClient.zscore(target._assembleKey(PendingPurgeKey), acquiredPartitions[1].releaseToken);
        const pendingContainsPartitionKey3 = await redisClient.zscore(target._assembleKey(PendingPurgeKey), acquiredPartitions[2].releaseToken);
        const pendingContainsPartitionKey4 = await redisClient.zscore(target._assembleKey(PendingPurgeKey), acquiredPartitions[3].releaseToken);

        const indexContainsPartitionKey1 = await redisClient.zscore(target._assembleKey(acquiredPartitions[0].key), acquiredPartitions[0].name);
        const indexContainsPartitionKey2 = await redisClient.zscore(target._assembleKey(acquiredPartitions[1].key), acquiredPartitions[1].name);
        const indexContainsPartitionKey3 = await redisClient.zscore(target._assembleKey(acquiredPartitions[2].key), acquiredPartitions[2].name);
        const indexContainsPartitionKey4 = await redisClient.zscore(target._assembleKey(acquiredPartitions[3].key), acquiredPartitions[3].name);

        const indexContainsPartitionOldKey1 = await redisClient.zscore(target._assembleKey(acquiredPartitions[0].key), acquiredPartitions[0].name.replace("pur", "acc"));
        const indexContainsPartitionOldKey2 = await redisClient.zscore(target._assembleKey(acquiredPartitions[1].key), acquiredPartitions[1].name.replace("pur", "acc"));
        const indexContainsPartitionOldKey3 = await redisClient.zscore(target._assembleKey(acquiredPartitions[2].key), acquiredPartitions[2].name.replace("pur", "acc"));
        const indexContainsPartitionOldKey4 = await redisClient.zscore(target._assembleKey(acquiredPartitions[3].key), acquiredPartitions[3].name.replace("pur", "acc"));

        //VERIFY
        assert.deepStrictEqual(bytes > 1n, true);
        assert.deepStrictEqual(acquiredPartitions.length === 4, true, `A:${acquiredPartitions.length} E:${4}`);
        assert.deepStrictEqual(acquiredPartitions[0], {
            start: 0n,
            key: 'GapTag',
            name: 'GapTag-0-pur',
            releaseToken: `["GapTag-0-pur",["${target.instanceName}"]]`,
            history: [target.instanceName],
            data: new Map([[1, 'One'], [2, 'Two']])
        });
        assert.deepStrictEqual(acquiredPartitions[1], {
            start: 10n,
            key: 'GapTag',
            name: 'GapTag-10-pur',
            releaseToken: `["GapTag-10-pur",["${target.instanceName}"]]`,
            history: [target.instanceName],
            data: new Map([[10, 'Ten']])
        });
        assert.deepStrictEqual(acquiredPartitions[2], {
            start: 20n,
            key: 'GapTag',
            name: 'GapTag-20-pur',
            releaseToken: `["GapTag-20-pur",["${target.instanceName}"]]`,
            history: [target.instanceName],
            data: new Map([[20, 'Twenty']])
        });
        assert.deepStrictEqual(acquiredPartitions[3], {
            start: 0n,
            key: 'SerialTag',
            name: 'SerialTag-0-pur',
            releaseToken: `["SerialTag-0-pur",["${target.instanceName}"]]`,
            history: [target.instanceName],
            data: new Map([[1, 'One'], [2, 'Two'], [3, 'Three'], [4, 'Four']])
        });
        assert.deepStrictEqual(recentActivityContainsPartitionKey1, null);
        assert.deepStrictEqual(recentActivityContainsPartitionKey2, null);
        assert.deepStrictEqual(recentActivityContainsPartitionKey3, null);
        assert.deepStrictEqual(recentActivityContainsPartitionKey4, null);

        assert.deepStrictEqual(recentActivityContainsPartitionOldKey1, null);
        assert.deepStrictEqual(recentActivityContainsPartitionOldKey2, null);
        assert.deepStrictEqual(recentActivityContainsPartitionOldKey3, null);
        assert.deepStrictEqual(recentActivityContainsPartitionOldKey4, null);

        assert.deepStrictEqual(partitionKeyExists1, 1);
        assert.deepStrictEqual(partitionKeyExists2, 1);
        assert.deepStrictEqual(partitionKeyExists3, 1);
        assert.deepStrictEqual(partitionKeyExists4, 1);

        assert.deepStrictEqual(partitionKeyExistsOld1, 0);
        assert.deepStrictEqual(partitionKeyExistsOld2, 0);
        assert.deepStrictEqual(partitionKeyExistsOld3, 0);
        assert.deepStrictEqual(partitionKeyExistsOld4, 0);

        assert.deepStrictEqual(pendingContainsPartitionKey1 >= 0, true, `Should be greater than zero ${pendingContainsPartitionKey1}`);
        assert.deepStrictEqual(pendingContainsPartitionKey2 >= 0, true, `Should be greater than zero ${pendingContainsPartitionKey1}`);
        assert.deepStrictEqual(pendingContainsPartitionKey3 >= 0, true, `Should be greater than zero ${pendingContainsPartitionKey1}`);
        assert.deepStrictEqual(pendingContainsPartitionKey4 >= 0, true, `Should be greater than zero ${pendingContainsPartitionKey1}`);

        assert.deepStrictEqual(indexContainsPartitionKey1 >= 0, true, `Should be greater than zero ${indexContainsPartitionKey1}`);
        assert.deepStrictEqual(indexContainsPartitionKey2 >= 0, true, `Should be greater than zero ${indexContainsPartitionKey2}`);
        assert.deepStrictEqual(indexContainsPartitionKey3 >= 0, true, `Should be greater than zero ${indexContainsPartitionKey3}`);
        assert.deepStrictEqual(indexContainsPartitionKey4 >= 0, true, `Should be greater than zero ${indexContainsPartitionKey4}`);

        assert.deepStrictEqual(indexContainsPartitionOldKey1>= 0, true);
        assert.deepStrictEqual(indexContainsPartitionOldKey2>= 0, true);
        assert.deepStrictEqual(indexContainsPartitionOldKey3>= 0, true);
        assert.deepStrictEqual(indexContainsPartitionOldKey4>= 0, true);

    }).timeout(2500);

    it('Should not allow to mark partition for purging when not initialized', async function () {

        //VERIFY
        await assert.rejects(() => target.purgeAcquire(), err => assert.strictEqual(err, "Please initialize the instance by calling 'initialize' first before any calls.") == undefined);

    });

    it('Should not allow to mark partition for purging when partitionAgeThresholdInSeconds is not valid', async function () {

        //SETUP
        await target.initialize();

        //VERIFY
        await assert.rejects(() => target.purgeAcquire("ladsa"), err => assert.strictEqual(err, "Parameter 'partitionAgeThresholdInSeconds' is invalid: Cannot convert ladsa to a BigInt") == undefined);

    });

    it('Should not allow to mark partition for purging when maxPartitionsToAcquire is not valid', async function () {

        //SETUP
        await target.initialize();

        //VERIFY
        await assert.rejects(() => target.purgeAcquire(0, "ladlf"), err => assert.strictEqual(err, "Parameter 'maxPartitionsToAcquire' is invalid: Cannot convert ladlf to a BigInt") == undefined);

    });

    it('Should not allow to mark partition for purging when partitionAgeThresholdInSeconds is zero or less', async function () {

        //SETUP
        await target.initialize();

        //VERIFY
        await assert.rejects(() => target.purgeAcquire(0), err => assert.strictEqual(err, "Parameter 'partitionAgeThresholdInSeconds' is invalid & should greater than 1.") == undefined);

    });

    it('Should not allow to mark partition for purging when maxPartitionsToAcquire is zero or less', async function () {

        //SETUP
        await target.initialize();

        //VERIFY
        await assert.rejects(() => target.purgeAcquire(undefined, 0), err => assert.strictEqual(err, "Parameter 'maxPartitionsToAcquire' is invalid & should greater than 1.") == undefined);

    });

    it('Should not allow to mark partition for purging when releaseTimeoutSeconds is not valid', async function () {

        //SETUP
        await target.initialize();

        //VERIFY
        await assert.rejects(() => target.purgeAcquire(undefined, undefined, "ladlf"), err => assert.strictEqual(err, "Parameter 'releaseTimeoutSeconds' is invalid: Cannot convert ladlf to a BigInt") == undefined);

    });

    it('Should not allow to mark partition for purging when releaseTimeoutSeconds is zero or less', async function () {

        //SETUP
        await target.initialize();

        //VERIFY
        await assert.rejects(() => target.purgeAcquire(undefined, undefined, 0), err => assert.strictEqual(err, "Parameter 'releaseTimeoutSeconds' is invalid & should greater than 1.") == undefined);

    });

    it('Should ack partition after purging when correct parameters are presented.', async function () {

        //SETUP
        const partitionWidth = 5;
        let inputData = new Map();
        const recentActivityKey = "RecentActivity";

        inputData.set("GapTag", new Map([[1, "One"], [2, "Two"], [10, "Ten"], [20, "Twenty"]]));
        inputData.set("SerialTag", new Map([[1, "One"], [2, "Two"], [3, "Three"], [4, "Four"]]));

        await target.initialize(partitionWidth);

        //WRITE
        const bytes = await target.write(inputData);

        //Killtime
        await new Promise((acc, rej) => setTimeout(acc, 1500));

        //PURGE
        const acquiredPartitions = await target.purgeAcquire(1, 10, 1000);

        //PURGE-Release
        const returnValue = await target.purgeRelease(acquiredPartitions[0].name, acquiredPartitions[0].key, acquiredPartitions[0].releaseToken);
        const partitionKeyExists = await redisClient.exists(target._assembleKey(acquiredPartitions[0].name));
        const indexContainsPartitionKey = await redisClient.zscore(target._assembleKey(acquiredPartitions[0].key), acquiredPartitions[0].name);

        //Read for acked tag
        const ranges = new Map();
        ranges.set("GapTag", { start: 0, end: 50 });
        ranges.set("SerialTag", { start: 0, end: 50 });
        const readResults = await readData(ranges);

        //VERIFY
        assert.deepStrictEqual(bytes > 1n, true);
        assert.deepStrictEqual(acquiredPartitions.length === 4, true, `A:${acquiredPartitions.length} E:${4}`);
        assert.deepStrictEqual(returnValue.success, 1);
        assert.deepStrictEqual(Number.isFinite(returnValue.rate), true);
        assert.deepStrictEqual(partitionKeyExists, 0);
        assert.deepStrictEqual(indexContainsPartitionKey, null);
        inputData.set("GapTag", new Map([[10, "Ten"], [20, "Twenty"]]))
        assert.deepStrictEqual(readResults, inputData);

    }).timeout(2500);

    it('Should not allow to purge release partition when not initialized', async function () {

        //VERIFY
        await assert.rejects(() => target.purgeRelease(), err => assert.strictEqual(err, "Please initialize the instance by calling 'initialize' first before any calls.") == undefined);

    });

    it('Should not allow to purge release partition when invalid parameter partitionName is passed', async function () {

        //SETUP
        await target.initialize()

        //VERIFY
        await assert.rejects(() => target.purgeRelease(), err => assert.strictEqual(err, `Invalid parameter 'partitionName'.`) == undefined);
        await assert.rejects(() => target.purgeRelease(""), err => assert.strictEqual(err, `Invalid parameter 'partitionName'.`) == undefined);

    });

    it('Should not allow to purge release partition when invalid parameter partitionKey is passed', async function () {

        //SETUP
        await target.initialize()

        //VERIFY
        await assert.rejects(() => target.purgeRelease("mockey"), err => assert.strictEqual(err, `Invalid parameter 'partitionKey'.`) == undefined);
        await assert.rejects(() => target.purgeRelease("mockkey", ""), err => assert.strictEqual(err, `Invalid parameter 'partitionKey'.`) == undefined);

    });

    it('Should not allow to purge release partition when invalid parameter purgeReleaseToken is passed', async function () {

        //SETUP
        await target.initialize()

        //VERIFY
        await assert.rejects(() => target.purgeRelease("mockkey", "mockkey"), err => assert.strictEqual(err, `Invalid parameter 'purgeReleaseToken'.`) == undefined);
        await assert.rejects(() => target.purgeRelease("mockkey", "mockkey", ""), err => assert.strictEqual(err, `Invalid parameter 'purgeReleaseToken'.`) == undefined);

    });

    it('Should purge data only once even if it purge is called multiple times.', async function () {

        //SETUP
        const partitionWidth = 5;
        let inputData = new Map();

        inputData.set("GapTag", new Map([[1, "One"], [2, "Two"], [10, "Ten"], [20, "Twenty"]]));
        inputData.set("SerialTag", new Map([[1, "One"], [2, "Two"], [3, "Three"], [4, "Four"]]));

        await target.initialize(partitionWidth);

        //WRITE
        const firstWriteBytes = await target.write(inputData);

        //Killtime
        await new Promise((acc, rej) => setTimeout(acc, 1500));

        //PURGE
        const acquiredPartitions1 = await target.purgeAcquire(1, 10, 1000);
        const acquiredPartitions2 = await target.purgeAcquire(1, 10, 1000);
        const acquiredPartitions3 = await target.purgeAcquire(1, 10, 1000);
        const acquiredPartitions4 = await target.purgeAcquire(1, 10, 1000);

        //Read for acked tag
        const ranges = new Map();
        ranges.set("GapTag", { start: 0, end: 50 });
        ranges.set("SerialTag", { start: 0, end: 50 });
        const readResults = await readData(ranges);

        //VERIFY
        assert.deepStrictEqual(firstWriteBytes > 1n, true);
        assert.deepStrictEqual(acquiredPartitions1.length === 4, true, `A:${acquiredPartitions1.length} E:${4}`);
        assert.deepStrictEqual(acquiredPartitions2.length === 0, true, `A:${acquiredPartitions2.length} E:${4}`);
        assert.deepStrictEqual(acquiredPartitions3.length === 0, true, `A:${acquiredPartitions3.length} E:${4}`);
        assert.deepStrictEqual(acquiredPartitions4.length === 0, true, `A:${acquiredPartitions4.length} E:${4}`);
        assert.deepStrictEqual(readResults, inputData);
    }).timeout(2500);

    it('Should ack only part of partition after purging when correct parameters are presented.', async function () {

        //SETUP
        const partitionWidth = 5;
        let inputData = new Map();
        const recentActivityKey = "RecentActivity";

        inputData.set("GapTag", new Map([[1, "One"], [2, "Two"], [10, "Ten"], [20, "Twenty"]]));
        inputData.set("SerialTag", new Map([[1, "One"], [2, "Two"], [3, "Three"], [4, "Four"]]));

        await target.initialize(partitionWidth);

        //WRITE
        const firstWriteBytes = await target.write(inputData);

        //Killtime
        await new Promise((acc, rej) => setTimeout(acc, 1500));

        //PURGE
        const acquiredPartition = await target.purgeAcquire(1, 10, 1000);

        //Write new data after marking for purge
        const newData = new Map();
        newData.set("GapTag", new Map([[2, "NewTwo"]]));
        const seconWriteBytes = await target.write(newData);

        //PURGE-ACK
        const returnValue = await target.purgeRelease(acquiredPartition[0].name, acquiredPartition[0].key, acquiredPartition[0].releaseToken)
        const recentActivityContainsPartitionOldKey1 = await redisClient.zrank(target._assembleKey(recentActivityKey), acquiredPartition[0].name.replace("pur", "acc"));
        const indexContainsPartitionOldKey1 = await redisClient.zscore(target._assembleKey(acquiredPartition[0].key), acquiredPartition[0].name.replace("pur", "acc"));
        const partitionKeyExistsOld1 = await redisClient.exists(target._assembleKey(acquiredPartition[0].name.replace("pur", "acc")));

        //Read for acked tag
        const ranges = new Map();
        ranges.set("GapTag", { start: 0, end: 50 });
        ranges.set("SerialTag", { start: 0, end: 50 });
        const readResults = await readData(ranges);

        //VERIFY
        assert.deepStrictEqual(firstWriteBytes > 1n, true);
        assert.deepStrictEqual(seconWriteBytes > 1n, true);
        assert.deepStrictEqual(acquiredPartition.length === 4, true, `A:${acquiredPartition.length} E:${4}`);
        assert.deepStrictEqual(returnValue.success, 1);
        assert.deepStrictEqual(Number.isFinite(returnValue.rate), true);
        assert.deepStrictEqual(recentActivityContainsPartitionOldKey1 > -1, true);
        assert.deepStrictEqual(indexContainsPartitionOldKey1 > -1, true);
        assert.deepStrictEqual(partitionKeyExistsOld1, 1);

        inputData.set("GapTag", new Map([[2, "NewTwo"], [10, "Ten"], [20, "Twenty"]]));
        assert.deepStrictEqual(readResults, inputData);
    }).timeout(2500);

    it('Should read chunk of data when correct data when presented with distribution function', async function () {

        //SETUP
        const partitionWidth = 10;
        await target.initialize(partitionWidth, (name) => "FixedPartition")
        let orderedData = new Map();
        let startDate = Date.now();
        for (let orderCounter = 0; orderCounter < 200; orderCounter++) {
            orderedData.set((startDate + orderCounter), orderCounter.toString());
        }
        let inputData = new Map();
        let ranges = new Map();
        for (let partitionCounter = 0; partitionCounter < 10; partitionCounter++) {
            inputData.set(`TagDCJf38X0DrgIZNCgyp4+RZC0rkoLtvaUokoj7cKTE7MSomethings-${partitionCounter}`, orderedData);
            ranges.set(`TagDCJf38X0DrgIZNCgyp4+RZC0rkoLtvaUokoj7cKTE7MSomethings-${partitionCounter}`, { start: startDate, end: (startDate * 2) });
        }

        //EXECUTE
        const returnValue = await target.write(inputData);

        //READ
        const result = await readData(ranges);

        //VERIFY
        assert.deepStrictEqual(result, inputData);

    });

    it('Should ack partition after purging when correct parameters are presented with distribution function', async function () {

        //SETUP
        const partitionWidth = 5;
        let inputData = new Map();
        const recentActivityKey = "RecentActivity";

        inputData.set("GapTag", new Map([[1, "One"], [2, "Two"], [10, "Ten"], [20, "Twenty"]]));
        inputData.set("SerialTag", new Map([[1, "One"], [2, "Two"], [3, "Three"], [4, "Four"]]));

        await target.initialize(partitionWidth, (name) => "FixedPartition");

        //WRITE
        const bytes = await target.write(inputData);

        //Killtime
        await new Promise((acc, rej) => setTimeout(acc, 1500));

        //PURGE
        const acquiredPartitions = await target.purgeAcquire(1, 10, 1000);

        //PURGE-Release
        // const returnValue = await target.purgeRelease(acquiredPartitions[0].name, acquiredPartitions[0].key, acquiredPartitions[0].releaseToken);
        // const partitionKeyExists = await redisClient.exists(target._assembleKey(acquiredPartitions[0].name));
        // const indexContainsPartitionKey = await redisClient.zscore(target._assembleKey(acquiredPartitions[0].key), acquiredPartitions[0].name);

        //Read for acked tag
        const ranges = new Map();
        ranges.set("GapTag", { start: 0, end: 50 });
        ranges.set("SerialTag", { start: 0, end: 50 });
        const readResults = await readData(ranges);

        //VERIFY
        assert.deepStrictEqual(bytes > 1n, true);
        assert.deepStrictEqual(acquiredPartitions.length, 3);
        // assert.deepStrictEqual(returnValue.success, 1);
        // assert.deepStrictEqual(Number.isFinite(returnValue.rate), true);
        // assert.deepStrictEqual(partitionKeyExists, 0);
        // assert.deepStrictEqual(indexContainsPartitionKey, null);
        // inputData.set("GapTag", new Map([[10, "Ten"], [20, "Twenty"]]))
        // assert.deepStrictEqual(readResults, inputData);

    }).timeout(2500);

});

async function readData(ranges) {
    //READ Indexes
    const pages = await target.readIndex(ranges);
    const tagNames = Array.from(ranges.keys());

    //READ Pages
    let asyncCommands = [];
    pages.forEach((pages, partitionName) => {
        pages.forEach((page) => {
            asyncCommands.push((async () => {
                const sortedMap = await target.readPage(page.page, (sortKey, tagName) => tagNames.indexOf(tagName) > -1 && page.start <= sortKey && sortKey <= page.end);
                return new Map([[partitionName, sortedMap]]);
            })());
        });
    });
    let queryResults = await Promise.allSettled(asyncCommands);
    const result = new Map();
    queryResults.reverse().forEach((e) => {
        if (e.status !== "fulfilled") {
            throw new Error(e.reason);
        }
        e = e.value;
        const partitionName = Array.from(e.keys())[0];
        const existingData = result.get(partitionName) || new Map();
        e.get(partitionName).forEach((v, k) => existingData.set(k, v));
        result.set(partitionName, existingData);
    });
    return result;
}