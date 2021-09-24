local Key = KEYS[1]
local HashInput = ARGV[1]

local HashValue = redis.call("HGET",Key,HashInput)
if (HashValue == nil or (type(HashValue) == "boolean" and not HashValue)) then
    HashValue = redis.call("HLEN",Key)
    redis.call("HSET",Key,HashInput)
end
return HashValue