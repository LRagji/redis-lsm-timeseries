local Key = KEYS[1]
local CounterKey = KEYS[2]
local createNewIdentity = tonumber(ARGV[1]);
local acquiredIdentities = {}

for index = 2, #ARGV do
    local HashInput = ARGV[index]
    local HashValue = redis.call("HGET",Key,HashInput)
    if (HashValue == nil or (type(HashValue) == "boolean" and not HashValue) and createNewIdentity == 1) then
        HashValue = redis.call("INCR",CounterKey)
        HashValue = tonumber(HashValue)+1
        redis.call("HSET",Key,HashInput,HashValue)
    end
    table.insert(acquiredIdentities,{HashInput,HashValue})
end

-- local readall = redis.call("HGETALL",Key)
-- for index = 1, (#readall/2), 2 do
--     table.insert(acquiredIdentities,{readall[index],readall[index+1]})
-- end
return acquiredIdentities