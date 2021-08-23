local pendingPurgeKey = KEYS[1]
local partitionIndexKey = KEYS[2]
local partitionKey = KEYS[3]
local monitoringKey = KEYS[4]

local purgeMember = ARGV[1]
local partitionName = ARGV[2]

local returnValues = {}

local purgeMemberExists = redis.call("ZRANK",pendingPurgeKey,purgeMember)

if ((purgeMemberExists == nil) or (type(purgeMemberExists) == "boolean" and purgeMemberExists == false) )then
    table.insert(returnValues,0)
else 
    redis.call("ZREM",pendingPurgeKey,purgeMember)
    redis.call("ZREM",partitionIndexKey,partitionName)
    redis.call("DEL",partitionKey)
    table.insert(returnValues,1)
end

local currentNumberOfKeys = redis.call("DBSIZE")
local tempTime = redis.call("TIME")
local currentTimestampInSeconds = tonumber(tempTime[1])
local previousTimeInSeconds =  redis.call("HGET",monitoringKey,"PTIME")
local previousNumberOfKeys =  redis.call("HGET",monitoringKey,"PSIZE")

redis.call("HSET",monitoringKey,"PTIME",currentTimestampInSeconds,"PSIZE",currentNumberOfKeys)

if ((previousNumberOfKeys == nil) or (type(previousNumberOfKeys) == "boolean" and previousNumberOfKeys == false) ) then 
    previousNumberOfKeys = currentNumberOfKeys
    previousTimeInSeconds = currentTimestampInSeconds
else
    previousNumberOfKeys = tonumber(previousNumberOfKeys)
    previousTimeInSeconds = tonumber(previousTimeInSeconds)
end 

local rateOfChange = 0
if currentTimestampInSeconds == previousTimeInSeconds then 
    rateOfChange = currentNumberOfKeys - previousNumberOfKeys
else
    rateOfChange = (currentNumberOfKeys - previousNumberOfKeys) / (currentTimestampInSeconds - previousTimeInSeconds)
end 

table.insert(returnValues,tostring(rateOfChange))

return returnValues

-- docker run with -v ${PWD}:\"/var/lib/mysql\"
-- cd /var/lib/mysql/lua-scripts
-- redis-cli --eval purge-release.lua space-pen space-abc-0-pur space-monitor , laukik