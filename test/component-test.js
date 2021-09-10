const assert = require('assert');
const localRedisConnectionString = "redis://127.0.0.1:6379/";
let redisClient;
const timeseriesType = require('../timeseries.js');
const scripto = require('redis-scripto2');
let target = null;
const hash = (tagName) => Array.from(tagName).reduce((acc, c) => acc + c.charCodeAt(0), 0);
const tagToPartitionMapping = (tagName) => hash(tagName) % 10;
const partitionToRedisMapping = (partitionName) => redisClient;
const tagnameToTagId = hash;
const settings = {
    "ActivityKey": "Activity",
    "SamplesPerPartitionKey": "Stats",
    "PurgePendingKey": "Pending",
    "Seperator": "=",
    "MaximumTagsInOneWrite": 2000,
    "MaximumTagsInOneRead": 100,
    "MaximumTagNameLength": 200,
    "MaximumPartitionsScansInOneRead": 100,
    "PartitionTimeWidth": 5,
    "PurgeMarker": "P"
}
let scriptManager;

describe('Timeseries consumer tests', function () {

    this.beforeEach(async function () {
        redisClient = require("./get-me-redis-client")(localRedisConnectionString);
        await redisClient.flushall();
        scriptManager = new scripto(redisClient);
    });

    this.afterEach(async function () {
        scriptManager = null;
        await redisClient.quit();
        redisClient = null;
    });

    it('Should write data and read it back', async function () {

        //SETUP
        target = new timeseriesType(tagToPartitionMapping, partitionToRedisMapping, tagnameToTagId, settings);
        const inputData = new Map();
        inputData.set("GapTag", new Map([[1n, "One"], [2n, "Two"], [10n, "Ten"], [20n, "Twenty"]]));
        inputData.set("SerialTag", new Map([[1n, "One"], [2n, "Two"], [3n, "Three"], [4n, "Four"]]));
        const ranges = new Map();
        ranges.set("GapTag", { "start": 0, "end": 100 });
        ranges.set("SerialTag", { "start": 0, "end": 100 });

        //Write
        const writeResult = await target.write(inputData);

        //Read
        const readResults = await target.read(ranges)

        //VERIFY
        assert.strictEqual(writeResult, true);
        assert.deepStrictEqual(readResults, inputData);
    });

    it('Should read only updated data', async function () {

        //SETUP
        target = new timeseriesType(tagToPartitionMapping, partitionToRedisMapping, tagnameToTagId, settings);
        const inputData = new Map();
        inputData.set("GapTag", new Map([[1n, "One"], [2n, "Two"], [10n, "Ten"], [20n, "Twenty"]]));
        inputData.set("SerialTag", new Map([[1n, "One"], [2n, "Two"], [3n, "Three"], [4n, "Four"]]));
        const updatedData = new Map();
        updatedData.set("GapTag", new Map([[1n, "OneU"], [2n, "TwoU"], [10n, "TenU"], [20n, "TwentyU"]]));
        updatedData.set("SerialTag", new Map([[1n, "OneU2"], [2n, "TwoU2"], [3n, "ThreeU2"], [4n, "FourU2"]]));
        const ranges = new Map();
        ranges.set("GapTag", { "start": 0, "end": 100 });
        ranges.set("SerialTag", { "start": 0, "end": 100 });

        //Write
        const writeResult = await target.write(inputData);

        //Update
        const updateResult = await target.write(updatedData);

        //Read
        const readResults = await target.read(ranges)

        //VERIFY
        assert.strictEqual(writeResult, true);
        assert.strictEqual(updateResult, true);
        assert.deepStrictEqual(readResults, updatedData);
    });

    it('Should purge on count and release data sucessfully.', async function () {

        //SETUP
        target = new timeseriesType(tagToPartitionMapping, partitionToRedisMapping, tagnameToTagId, settings);
        const inputData = new Map();
        inputData.set("GapTag", new Map([[1n, "One"], [2n, "Two"], [10n, "Ten"], [20n, "Twenty"]]));
        inputData.set("SerialTag", new Map([[1n, "One"], [2n, "Two"], [3n, "Three"], [4n, "Four"]]));
        const ranges = new Map();
        ranges.set("GapTag", { "start": 0, "end": 100 });
        ranges.set("SerialTag", { "start": 0, "end": 100 });

        //Write
        const writeResult = await target.write(inputData);

        //Purge
        const purgeResults = await target.purgeAcquire(scriptManager, -1, 4, -1);

        //Release
        const releaseResults = await target.purgeRelease(scriptManager, purgeResults[0].releaseToken);

        //VERIFY
        assert.strictEqual(writeResult, true);
        assert.strictEqual(purgeResults.length, 1);
        assert.strictEqual(purgeResults[0].name !== "" && purgeResults[0].name !== null, true);
        assert.strictEqual(purgeResults[0].releaseToken !== "" && purgeResults[0].releaseToken !== null, true);
        const tag = "SerialTag";
        assert.deepStrictEqual(purgeResults[0].data.get(BigInt(hash(tag))), inputData.get(tag));
        assert.deepStrictEqual(releaseResults, true);
    });

    it('Should purge on time and release data sucessfully.', async function () {

        //SETUP
        target = new timeseriesType(tagToPartitionMapping, partitionToRedisMapping, tagnameToTagId, settings);
        const inputData = new Map();
        inputData.set("GapTag", new Map([[1n, "One"], [2n, "Two"], [10n, "Ten"], [20n, "Twenty"]]));
        inputData.set("SerialTag", new Map([[1n, "One"], [2n, "Two"], [3n, "Three"], [4n, "Four"]]));
        const ranges = new Map();
        ranges.set("GapTag", { "start": 0, "end": 100 });
        ranges.set("SerialTag", { "start": 0, "end": 100 });

        //Write
        const writeResult = await target.write(inputData);

        //KillTime
        await new Promise((acc, rej) => setTimeout(acc, 1500));

        //Purge
        const purgeResults = await target.purgeAcquire(scriptManager, 1, -1, -1);

        //Release
        const releaseResults = await target.purgeRelease(scriptManager, purgeResults[0].releaseToken);

        //VERIFY
        assert.strictEqual(writeResult, true);
        assert.strictEqual(purgeResults.length, 4);
        let tag = "SerialTag";
        assert.strictEqual(purgeResults[0].name !== "" && purgeResults[0].name !== null, true);
        assert.strictEqual(purgeResults[0].releaseToken !== "" && purgeResults[0].releaseToken !== null, true);
        assert.deepStrictEqual(purgeResults[0].data.get(BigInt(hash(tag))), inputData.get(tag));
        tag = "GapTag";
        assert.strictEqual(purgeResults[1].name !== "" && purgeResults[1].name !== null, true);
        assert.strictEqual(purgeResults[1].releaseToken !== "" && purgeResults[1].releaseToken !== null, true);
        assert.deepStrictEqual(purgeResults[1].data.get(BigInt(hash(tag))), new Map([[1n, "One"], [2n, "Two"]]));
        assert.strictEqual(purgeResults[2].name !== "" && purgeResults[1].name !== null, true);
        assert.strictEqual(purgeResults[3].releaseToken !== "" && purgeResults[1].releaseToken !== null, true);
        assert.deepStrictEqual(purgeResults[2].data.get(BigInt(hash(tag))), new Map([[10n, "Ten"]]));
        assert.strictEqual(purgeResults[3].name !== "" && purgeResults[1].name !== null, true);
        assert.strictEqual(purgeResults[3].releaseToken !== "" && purgeResults[1].releaseToken !== null, true);
        assert.deepStrictEqual(purgeResults[3].data.get(BigInt(hash(tag))), new Map([[20n, "Twenty"]]));
        assert.deepStrictEqual(releaseResults, true);
    });

    it('Should purge on timeout and release data sucessfully.', async function () {

        //SETUP
        target = new timeseriesType(tagToPartitionMapping, partitionToRedisMapping, tagnameToTagId, settings);
        const inputData = new Map();
        inputData.set("GapTag", new Map([[1n, "One"], [2n, "Two"], [10n, "Ten"], [20n, "Twenty"]]));
        inputData.set("SerialTag", new Map([[1n, "One"], [2n, "Two"], [3n, "Three"], [4n, "Four"]]));
        const ranges = new Map();
        ranges.set("GapTag", { "start": 0, "end": 100 });
        ranges.set("SerialTag", { "start": 0, "end": 100 });

        //Write
        const writeResult = await target.write(inputData);

        //Purge
        const purgeResults1 = await target.purgeAcquire(scriptManager, -1, 4, -1);

        //KillTime
        await new Promise((acc, rej) => setTimeout(acc, 1500));

        //Purge
        const purgeResults2 = await target.purgeAcquire(scriptManager, -1, -4, 1);

        //Release
        const releaseResults = await target.purgeRelease(scriptManager, purgeResults2[0].releaseToken);

        //VERIFY
        assert.strictEqual(writeResult, true);
        assert.strictEqual(purgeResults1.length, 1);
        assert.strictEqual(purgeResults1[0].name !== "" && purgeResults1[0].name !== null, true);
        assert.strictEqual(purgeResults1[0].releaseToken !== "" && purgeResults1[0].releaseToken !== null, true);
        const tag = "SerialTag";
        assert.deepStrictEqual(purgeResults1[0].data.get(BigInt(hash(tag))), inputData.get(tag));
        assert.strictEqual(purgeResults2.length, 1);
        assert.strictEqual(purgeResults2[0].name !== "" && purgeResults2[0].name !== null, true);
        assert.strictEqual(purgeResults2[0].releaseToken !== "" && purgeResults2[0].releaseToken !== null, true);
        assert.deepStrictEqual(purgeResults2[0].data.get(BigInt(hash(tag))), inputData.get(tag));
        assert.deepStrictEqual(releaseResults, true);
    });

    it('Should read data when its purged but not released.', async function () {

        //SETUP
        target = new timeseriesType(tagToPartitionMapping, partitionToRedisMapping, tagnameToTagId, settings);
        const inputData = new Map();
        inputData.set("GapTag", new Map([[1n, "One"], [2n, "Two"], [10n, "Ten"], [20n, "Twenty"]]));
        inputData.set("SerialTag", new Map([[1n, "One"], [2n, "Two"], [3n, "Three"], [4n, "Four"]]));
        const ranges = new Map();
        ranges.set("GapTag", { "start": 0, "end": 100 });
        ranges.set("SerialTag", { "start": 0, "end": 100 });

        //Write
        const writeResult = await target.write(inputData);

        //Purge
        const purgeResults = await target.purgeAcquire(scriptManager, -1, 4, -1);

        //Read
        const readResults = await target.read(ranges)

        //VERIFY
        assert.strictEqual(writeResult, true);
        assert.strictEqual(purgeResults.length, 1);
        assert.strictEqual(purgeResults[0].name !== "" && purgeResults[0].name !== null, true);
        assert.strictEqual(purgeResults[0].releaseToken !== "" && purgeResults[0].releaseToken !== null, true);
        const tag = "SerialTag";
        assert.deepStrictEqual(purgeResults[0].data.get(BigInt(hash(tag))), inputData.get(tag));
        assert.deepStrictEqual(readResults, inputData);
    });

    it('Should read data when its purged but not released.', async function () {

        //SETUP
        target = new timeseriesType(tagToPartitionMapping, partitionToRedisMapping, tagnameToTagId, settings);
        const inputData = new Map();
        inputData.set("GapTag", new Map([[1n, "One"], [2n, "Two"], [10n, "Ten"], [20n, "Twenty"]]));
        inputData.set("SerialTag", new Map([[1n, "One"], [2n, "Two"], [3n, "Three"], [4n, "Four"]]));
        const ranges = new Map();
        ranges.set("GapTag", { "start": 0, "end": 100 });
        ranges.set("SerialTag", { "start": 0, "end": 100 });

        //Write
        const writeResult = await target.write(inputData);

        //Purge
        const purgeResults = await target.purgeAcquire(scriptManager, -1, 4, -1);

        //Read
        const readResults = await target.read(ranges)

        //VERIFY
        assert.strictEqual(writeResult, true);
        assert.strictEqual(purgeResults.length, 1);
        assert.strictEqual(purgeResults[0].name !== "" && purgeResults[0].name !== null, true);
        assert.strictEqual(purgeResults[0].releaseToken !== "" && purgeResults[0].releaseToken !== null, true);
        const tag = "SerialTag";
        assert.deepStrictEqual(purgeResults[0].data.get(BigInt(hash(tag))), inputData.get(tag));
        assert.deepStrictEqual(readResults, inputData);
    });

    it('Should not read data when its purged and released.', async function () {

        //SETUP
        target = new timeseriesType(tagToPartitionMapping, partitionToRedisMapping, tagnameToTagId, settings);
        const inputData = new Map();
        inputData.set("GapTag", new Map([[1n, "One"], [2n, "Two"], [10n, "Ten"], [20n, "Twenty"]]));
        inputData.set("SerialTag", new Map([[1n, "One"], [2n, "Two"], [3n, "Three"], [4n, "Four"]]));
        const ranges = new Map();
        ranges.set("GapTag", { "start": 0, "end": 100 });
        ranges.set("SerialTag", { "start": 0, "end": 100 });

        //Write
        const writeResult = await target.write(inputData);

        //Purge
        const purgeResults = await target.purgeAcquire(scriptManager, -1, 4, -1);

        //Release
        const releaseResults = await target.purgeRelease(scriptManager, purgeResults[0].releaseToken);

        //Read
        const readResults = await target.read(ranges);

        //VERIFY
        assert.strictEqual(writeResult, true);
        assert.strictEqual(purgeResults.length, 1);
        assert.strictEqual(purgeResults[0].name !== "" && purgeResults[0].name !== null, true);
        assert.strictEqual(purgeResults[0].releaseToken !== "" && purgeResults[0].releaseToken !== null, true);
        const tag = "SerialTag";
        assert.deepStrictEqual(purgeResults[0].data.get(BigInt(hash(tag))), inputData.get(tag));
        assert.deepStrictEqual(releaseResults, true);
        inputData.delete(tag);
        assert.deepStrictEqual(readResults, inputData);
    });
});