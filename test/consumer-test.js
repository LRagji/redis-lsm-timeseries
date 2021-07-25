const assert = require('assert');
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
        let expectedEPOCH = await redisClient.get("EPOCH");

        //VERIFY
        assert.strictEqual(actualEPOCH.toString(), expectedEPOCH);
    });

    it('Should fail with initialization exception when EPOCH is set to invalid', async function () {

        //SETUP
        await redisClient.set("EPOCH", "Laukik");//This is to simulate key is set but not epoch i.e:Timestamp ;

        //VERIFY
        await assert.rejects(target.initialize, err => assert.strictEqual(err, 'Initialization Failed: EPOCH is misplaced with NaN.') == undefined);
    });
});