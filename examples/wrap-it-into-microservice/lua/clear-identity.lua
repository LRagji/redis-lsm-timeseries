local Key = KEYS[1]
local HashInput = ARGV[1]

return redis.call("HDEL",Key,HashInput);