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
        let expectedEPOCH = await redisClient.get(target.SettingsHash + "-EPOCH");

        //VERIFY
        assert.strictEqual(actualEPOCH.toString(), expectedEPOCH);
    });

    it('Should fail with initialization exception when EPOCH is set to invalid', async function () {

        //SETUP
        let hash = Crypto.createHash("sha256").update(JSON.stringify({ "version": 1.0, "partitionWidth": 86400000n.toString(), "purgeQueName": 'Purge' }), "binary").digest("hex")
        await redisClient.set(hash + "-EPOCH", "Laukik");//This is to simulate key is set but not epoch i.e:Timestamp ;

        //VERIFY
        await assert.rejects(target.initialize, err => assert.strictEqual(err, 'Initialization Failed: EPOCH is misplaced with undefined.') == undefined);
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
        const partitionName = "GapTag" + Seperator + partitionStart.toString();
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
        const partitionName = "GapTag" + Seperator + partitionStart.toString();
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
        const partitionName = "GapTag" + Seperator + partitionStart.toString();
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
            const expected = Array.from(pageSets).map(e => `${key}-${e}`).reverse();
            assert.notStrictEqual(undefined, currentPage, `Page not found for ${key} between ${range.start} to ${range.end}.`);
            assert.deepStrictEqual(currentPage.map(e => e.page), expected, `Not all pages found E:${expected} A:${currentPage} for ${key} between ${range.start} to ${range.end}.`);
            //Verify weight odering.
            currentPage.reduce((pPage, cPage) => {
                assert.strictEqual(pPage.sortWeight < cPage.sortWeight, true, `Current weight:${cPage.sortWeight} Previous weight:${pPage.sortWeight} for ${key} between ${range.start} to ${range.end}.`);
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
        await assert.rejects(() => target.readPage("Laukik-", 0, 0), err => assert.strictEqual(err, `Invalid 'pagename': Seperator misplaced @6`) == undefined);

    });

    it('Should not allow readPage when incorrect range is specified', async function () {
        //SETUP
        await target.initialize();

        //VERIFY when start is null
        await assert.rejects(() => target.readPage("Laukik-9", null, 0), err => assert.strictEqual(err, `Invalid start range for Laukik-9: Cannot convert null to a BigInt`) == undefined);

        //VERIFY when end is null
        await assert.rejects(() => target.readPage("Laukik-8", 0, null), err => assert.strictEqual(err, `Invalid end range for Laukik-8: Cannot convert null to a BigInt`) == undefined);

        //VERIFY when it is text
        await assert.rejects(() => target.readPage("Laukik-9", "laukkik", 0), err => assert.strictEqual(err, `Invalid start range for Laukik-9: Cannot convert laukkik to a BigInt`) == undefined);

        //VERIFY when it is float
        await assert.rejects(() => target.readPage("Laukik-9", 0.3, 0), err => assert.strictEqual(err, `Invalid start range for Laukik-9: The number 0.3 cannot be converted to a BigInt because it is not an integer`) == undefined);

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
        let qName = "Purge";
        const Seperator = '-';

        inputData.set("GapTag", new Map([[1, "One"], [2, "Two"], [10, "Ten"], [20, "Twenty"]]));
        inputData.set("SerialTag", new Map([[1, "One"], [2, "Two"], [3, "Three"], [4, "Four"]]));

        await target.initialize(partitionWidth, qName);

        //WRITE
        const bytes = await target.write(inputData);

        //PURGE
        const markedPartitionsIds = await target.purgeScan(1, 10);

        //VERIFY
        assert.deepStrictEqual(bytes > 1n, true);
        assert.deepStrictEqual(markedPartitionsIds.length === 4, true, `A:${markedPartitionsIds.length} E:${4}`);
        const actual = new Map();
        for (let index = 0; index < markedPartitionsIds.length; index++) {
            let results = await redisClient.xrange(target._assembleKey(qName), markedPartitionsIds[index], markedPartitionsIds[index]);
            const tagName = results[0][1][0].split(Seperator)[0];
            let members = actual.get(tagName) || []
            members.push(...((JSON.parse(results[0][1][1])).map(e => JSON.parse(e).p).filter(e => e != undefined)));
            actual.set(tagName, members);
        }
        assert.deepStrictEqual(actual.get("GapTag"), ['One', 'Two', 'Ten', 'Twenty']);
        assert.deepStrictEqual(actual.get("SerialTag"), ['One', 'Two', 'Three', 'Four']);
    });

    it('Should not allow to mark partition for purging when not initialized', async function () {

        //VERIFY
        await assert.rejects(() => target.purgeScan(), err => assert.strictEqual(err, "Please initialize the instance by calling 'initialize' first before any calls.") == undefined);

    });

    it('Should not allow to mark partition for purging when partitionAgeThreshold is not valid', async function () {

        //SETUP
        await target.initialize();

        //VERIFY
        await assert.rejects(() => target.purgeScan("ladsa"), err => assert.strictEqual(err, "Parameter 'partitionAgeThreshold' is invalid: Cannot convert ladsa to a BigInt") == undefined);

    });

    it('Should not allow to mark partition for purging when maxPartitionsToMark is not valid', async function () {

        //SETUP
        await target.initialize();

        //VERIFY
        await assert.rejects(() => target.purgeScan(0, "ladlf"), err => assert.strictEqual(err, "Parameter 'maxPartitionsToMark' is invalid: Cannot convert ladlf to a BigInt") == undefined);

    });

    it('Should not allow to mark partition for purging when partitionAgeThreshold is zero or less', async function () {

        //SETUP
        await target.initialize();

        //VERIFY
        await assert.rejects(() => target.purgeScan(0), err => assert.strictEqual(err, "Parameter 'partitionAgeThreshold' is invalid & should greater than 1.") == undefined);

    });

    it('Should not allow to mark partition for purging when maxPartitionsToMark is zero or less', async function () {

        //SETUP
        await target.initialize();

        //VERIFY
        await assert.rejects(() => target.purgeScan(undefined, 0), err => assert.strictEqual(err, "Parameter 'maxPartitionsToMark' is invalid & should greater than 1.") == undefined);

    });

    it('Should ack partition after purging when correct parameters are presented.', async function () {

        //SETUP
        const partitionWidth = 5;
        let inputData = new Map();
        let qName = "Purge";
        const Seperator = '-';
        const recentActivityKey = "RecentActivity";

        inputData.set("GapTag", new Map([[1, "One"], [2, "Two"], [10, "Ten"], [20, "Twenty"]]));
        inputData.set("SerialTag", new Map([[1, "One"], [2, "Two"], [3, "Three"], [4, "Four"]]));

        await target.initialize(partitionWidth, qName);

        //WRITE
        const bytes = await target.write(inputData);

        //PURGE
        const markedPartitionsIds = await target.purgeScan(1, 10);

        //GET Purged Details
        const results = await redisClient.xrange(target._assembleKey(qName), markedPartitionsIds[0], markedPartitionsIds[0]);
        const partitionKey = results[0][1][0]
        const tagName = partitionKey.split(Seperator)[0];

        //PURGE-ACK
        const returnValue = await target.purgeAck(markedPartitionsIds[0])
        const partitionKeyExists = await redisClient.exists(target._assembleKey(partitionKey));
        const recentActivityContainsPartitionKey = await redisClient.zrank(target._assembleKey(recentActivityKey), partitionKey);
        const indexKeyExists = await redisClient.exists(target._assembleKey(tagName));

        //Read for acked tag
        const ranges = new Map();
        ranges.set("GapTag", { start: 0, end: 50 });
        ranges.set("SerialTag", { start: 0, end: 50 });
        const readResults = await readData(ranges);

        //VERIFY
        assert.deepStrictEqual(bytes > 1n, true);
        assert.deepStrictEqual(markedPartitionsIds.length === 4, true, `A:${markedPartitionsIds.length} E:${4}`);
        assert.deepStrictEqual(returnValue, 1);
        assert.deepStrictEqual(partitionKeyExists, 0);
        assert.deepStrictEqual(recentActivityContainsPartitionKey, null);
        assert.deepStrictEqual(indexKeyExists, 1);
        inputData.set("GapTag", new Map([[10, "Ten"], [20, "Twenty"]]))
        assert.deepStrictEqual(readResults, inputData);

    });

    it('Should purge data only once even if it purge is called multiple times.', async function () {

        //SETUP
        const partitionWidth = 5;
        let inputData = new Map();
        let qName = "Purge";
        const Seperator = '-';
        const recentActivityKey = "RecentActivity";

        inputData.set("GapTag", new Map([[1, "One"], [2, "Two"], [10, "Ten"], [20, "Twenty"]]));
        inputData.set("SerialTag", new Map([[1, "One"], [2, "Two"], [3, "Three"], [4, "Four"]]));

        await target.initialize(partitionWidth, qName);

        //WRITE
        const firstWriteBytes = await target.write(inputData);

        //PURGE
        const markedPartitionsIds1 = await target.purgeScan(1, 10);
        const markedPartitionsIds2 = await target.purgeScan(1, 10);
        const markedPartitionsIds3 = await target.purgeScan(1, 10);
        const markedPartitionsIds4 = await target.purgeScan(1, 10);

        //Read for acked tag
        const ranges = new Map();
        ranges.set("GapTag", { start: 0, end: 50 });
        ranges.set("SerialTag", { start: 0, end: 50 });
        const readResults = await readData(ranges);

        //VERIFY
        assert.deepStrictEqual(firstWriteBytes > 1n, true);
        assert.deepStrictEqual(markedPartitionsIds1.length === 4, true, `A:${markedPartitionsIds1.length} E:${4}`);
        assert.deepStrictEqual(markedPartitionsIds2.length === 0, true, `A:${markedPartitionsIds2.length} E:${4}`);
        assert.deepStrictEqual(markedPartitionsIds3.length === 0, true, `A:${markedPartitionsIds3.length} E:${4}`);
        assert.deepStrictEqual(markedPartitionsIds4.length === 0, true, `A:${markedPartitionsIds4.length} E:${4}`);
        assert.deepStrictEqual(readResults, inputData);
    });

    it('Should ack only part of partition after purging when correct parameters are presented.', async function () {

        //SETUP
        const partitionWidth = 5;
        let inputData = new Map();
        let qName = "Purge";
        const Seperator = '-';
        const recentActivityKey = "RecentActivity";

        inputData.set("GapTag", new Map([[1, "One"], [2, "Two"], [10, "Ten"], [20, "Twenty"]]));
        inputData.set("SerialTag", new Map([[1, "One"], [2, "Two"], [3, "Three"], [4, "Four"]]));

        await target.initialize(partitionWidth, qName);

        //WRITE
        const firstWriteBytes = await target.write(inputData);

        //PURGE
        const markedPartitionsIds = await target.purgeScan(1, 10);

        //Write new data after marking for purge
        const newData = new Map();
        newData.set("GapTag", new Map([[2, "NewTwo"]]));
        const seconWriteBytes = await target.write(newData);

        //GET Purged Details
        const results = await redisClient.xrange(target._assembleKey(qName), markedPartitionsIds[0], markedPartitionsIds[0]);
        const partitionKey = results[0][1][0]
        const tagName = partitionKey.split(Seperator)[0];

        //PURGE-ACK
        const returnValue = await target.purgeAck(markedPartitionsIds[0])
        const partitionKeyExists = await redisClient.exists(target._assembleKey(partitionKey));
        const recentActivityContainsPartitionKey = await redisClient.zrank(target._assembleKey(recentActivityKey), partitionKey);
        const indexKeyExists = await redisClient.exists(target._assembleKey(tagName));

        //Read for acked tag
        const ranges = new Map();
        ranges.set("GapTag", { start: 0, end: 50 });
        ranges.set("SerialTag", { start: 0, end: 50 });
        const readResults = await readData(ranges);

        //VERIFY
        assert.deepStrictEqual(firstWriteBytes > 1n, true);
        assert.deepStrictEqual(seconWriteBytes > 1n, true);
        assert.deepStrictEqual(markedPartitionsIds.length === 4, true, `A:${markedPartitionsIds.length} E:${4}`);
        assert.deepStrictEqual(returnValue, 1);
        assert.deepStrictEqual(partitionKeyExists, 1);
        assert.deepStrictEqual(recentActivityContainsPartitionKey > -1, true);
        assert.deepStrictEqual(indexKeyExists, 1);
        inputData.set("GapTag", new Map([[2, "NewTwo"], [10, "Ten"], [20, "Twenty"]]));
        assert.deepStrictEqual(readResults, inputData);
    });

    it('Should clear partition index and recentactivity after purging when correct parameters are presented.', async function () {

        //SETUP
        const partitionWidth = 5;
        let inputData = new Map();
        let qName = "Purge";
        const Seperator = '-';
        const recentActivityKey = "RecentActivity";

        inputData.set("GapTag", new Map([[1, "One"], [2, "Two"]]));
        inputData.set("SerialTag", new Map([[1, "One"], [2, "Two"], [3, "Three"], [4, "Four"]]));

        await target.initialize(partitionWidth, qName);

        //WRITE
        const bytes = await target.write(inputData);

        //PURGE
        const markedPartitionsIds = await target.purgeScan(1, 10);

        //GET Purged Details
        const results = await redisClient.xrange(target._assembleKey(qName), markedPartitionsIds[0], markedPartitionsIds[0]);
        const partitionKey = results[0][1][0]
        const tagName = partitionKey.split(Seperator)[0];

        //PURGE-ACK
        const returnValue = await target.purgeAck(markedPartitionsIds[0])
        const partitionKeyExists = await redisClient.exists(target._assembleKey(partitionKey));
        const recentActivityContainsPartitionKey = await redisClient.zrank(target._assembleKey(recentActivityKey), partitionKey);
        const indexKeyExists = await redisClient.exists(target._assembleKey(tagName));

        //Read for acked tag
        const ranges = new Map();
        ranges.set("GapTag", { start: 0, end: 50 });
        ranges.set("SerialTag", { start: 0, end: 50 });
        const readResults = await readData(ranges);

        //VERIFY
        assert.deepStrictEqual(bytes > 1n, true);
        assert.deepStrictEqual(markedPartitionsIds.length === 2, true, `A:${markedPartitionsIds.length} E:${4}`);
        assert.deepStrictEqual(returnValue, 1);
        assert.deepStrictEqual(partitionKeyExists, 0);
        assert.deepStrictEqual(recentActivityContainsPartitionKey, null);
        assert.deepStrictEqual(indexKeyExists, 0);
        inputData.delete("GapTag");
        assert.deepStrictEqual(readResults, inputData);

    });

    it('Should not allow to purge ack partition when not initialized', async function () {

        //VERIFY
        await assert.rejects(() => target.purgeAck(), err => assert.strictEqual(err, "Please initialize the instance by calling 'initialize' first before any calls.") == undefined);

    });

    it('Should not allow to purge ack partition when invalid parameter id is passed', async function () {

        //SETUP
        await target.initialize()

        //VERIFY
        await assert.rejects(() => target.purgeAck(), err => assert.strictEqual(err, `Invalid parameter 'purgeId'.`) == undefined);
        await assert.rejects(() => target.purgeAck(""), err => assert.strictEqual(err, `Invalid parameter 'purgeId'.`) == undefined);

    });

    it('Should parse partition data after purge scan when correct parameters are presented.', async function () {

        //SETUP
        const partitionWidth = 5;
        let inputData = new Map();
        let qName = "Purge";
        const Seperator = '-';
        const recentActivityKey = "RecentActivity";

        inputData.set("GapTag", new Map([[1, "One"], [2, "Two"], [10, "Ten"], [20, "Twenty"]]));
        inputData.set("SerialTag", new Map([[1, "One"], [2, "Two"], [3, "Three"], [4, "Four"]]));

        await target.initialize(partitionWidth, qName);

        //WRITE
        const bytes = await target.write(inputData);

        //PURGE
        const markedPartitionsIds = await target.purgeScan(1, 10);

        //GET Purged Details
        const results = await redisClient.xrange(target._assembleKey(qName), markedPartitionsIds[0], markedPartitionsIds[0]);
        const parsedData = target.parsePurgePayload(results[0])
        const partitionKey = parsedData.partition;
        const tagName = parsedData.key;

        //PURGE-ACK
        const returnValue = await target.purgeAck(markedPartitionsIds[0])
        const partitionKeyExists = await redisClient.exists(target._assembleKey(partitionKey));
        const recentActivityContainsPartitionKey = await redisClient.zrank(target._assembleKey(recentActivityKey), partitionKey);
        const indexKeyExists = await redisClient.exists(target._assembleKey(tagName));

        //Read for acked tag
        const ranges = new Map();
        ranges.set("GapTag", { start: 0, end: 50 });
        ranges.set("SerialTag", { start: 0, end: 50 });
        const readResults = await readData(ranges);

        //VERIFY
        assert.deepStrictEqual(bytes > 1n, true);
        assert.deepStrictEqual(markedPartitionsIds.length === 4, true, `A:${markedPartitionsIds.length} E:${4}`);
        assert.deepStrictEqual(returnValue, 1);
        assert.deepStrictEqual(partitionKeyExists, 0);
        assert.deepStrictEqual(recentActivityContainsPartitionKey, null);
        assert.deepStrictEqual(indexKeyExists, 1);
        inputData.set("GapTag", new Map([[10, "Ten"], [20, "Twenty"]]))
        assert.deepStrictEqual(readResults, inputData);
        assert.deepStrictEqual(parsedData.id, markedPartitionsIds[0]);
        assert.deepStrictEqual(parsedData.data, new Map([[1, "One"], [2, "Two"]]));

    });

});

async function readData(ranges) {
    //READ Indexes
    const pages = await target.readIndex(ranges);

    //READ Pages
    let asyncCommands = [];
    pages.forEach((pages, partitionName) => {
        pages.forEach((page) => {
            asyncCommands.push((async () => {
                const sortedMap = await target.readPage(page.page, page.start, page.end);
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