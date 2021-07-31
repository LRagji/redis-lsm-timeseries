module.exports = (redisConnectionString) => {
    let redisClient;
    switch (process.env.REDISCLIENT) {
        case '1':
            console.log("Using redis as redis client.")
            const redis = require("redis");
            const myFavClient = redis.createClient(redisConnectionString);
            const { promisify } = require("util");
            redisClient = myFavClient;
            redisClient.xreadgroup = promisify(myFavClient.xreadgroup).bind(myFavClient);
            redisClient.xack = promisify(myFavClient.xack).bind(myFavClient);
            redisClient.multi = () => {
                let multiObject = myFavClient.multi();
                multiObject.exec = promisify(multiObject.exec);
                return multiObject;
            };
            redisClient.xdel = promisify(myFavClient.xdel).bind(myFavClient);
            redisClient.xpending = promisify(myFavClient.xpending).bind(myFavClient);
            redisClient.xgroup = promisify(myFavClient.xgroup).bind(myFavClient);
            redisClient.memory = promisify(myFavClient.memory).bind(myFavClient);
            redisClient.xadd = promisify(myFavClient.xadd).bind(myFavClient);
            redisClient.quit = promisify(myFavClient.quit).bind(myFavClient);
            redisClient.flushall = promisify(myFavClient.flushall).bind(myFavClient);
            redisClient.get = promisify(myFavClient.get).bind(myFavClient);
            redisClient.set = promisify(myFavClient.set).bind(myFavClient);
            redisClient.zadd = promisify(myFavClient.zadd).bind(myFavClient);
            redisClient.zrangebyscore = promisify(myFavClient.zrangebyscore).bind(myFavClient);
            redisClient.zrank = promisify(myFavClient.zrank).bind(myFavClient);
            redisClient.zrange = promisify(myFavClient.zrange).bind(myFavClient);
            break;
        default:
            console.log("Defaulting to ioredis as redis client Enviroment: " + process.env.REDISCLIENT)
            const redisType = require("ioredis");
            redisClient = new redisType(redisConnectionString);
            break;
    }
    return redisClient;
}