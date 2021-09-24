local Key = KEYS[1]
local CounterKey = KEYS[2]
local HashInput = ARGV[1]

local HashValue = redis.call("HGET",Key,HashInput)
if (HashValue == nil or (type(HashValue) == "boolean" and not HashValue)) then
    HashValue = redis.call("INCR",CounterKey)
    HashValue = tonumber(HashValue)+1
    redis.call("HSET",Key,HashInput,HashValue)
end
return HashValue