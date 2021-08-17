local recentActivityKey = KEYS[1]
local purgePendingKey = KEYS[2]

local epochInSeconds = tonumber(ARGV[1])
local instanceToken = ARGV[2]
local entriesToPurge = tonumber(ARGV[3])
local pendingTimeoutInSeconds = tonumber(ARGV[4])
local purgeThresholdInSeconds = tonumber(ARGV[5])

local acquiredPartitions = {}
local logs = {}

local tempTime = redis.call("TIME")
local currentTimestampInSeconds = tonumber(tempTime[1])
table.insert(logs,'Time In Seconds '.. currentTimestampInSeconds)
table.insert(logs,'Epoc In Seconds '.. epochInSeconds)
table.insert(logs,'Purge Threshold In Seconds '.. purgeThresholdInSeconds)
table.insert(logs,'Pending Timeout In Seconds '.. pendingTimeoutInSeconds)
table.insert(logs,'Entries to purge '.. entriesToPurge)
local calculatedDeadTime = currentTimestampInSeconds - pendingTimeoutInSeconds
table.insert(logs,'Calculated DeadTime In Seconds '.. calculatedDeadTime)
local deadPurges =  redis.call("ZRANGEBYSCORE",purgePendingKey,"-inf",calculatedDeadTime,"LIMIT",0, entriesToPurge)
table.insert(logs,'Dead Items '.. #deadPurges)
for index = 1, #deadPurges do
    local deadMember = deadPurges[index]
    redis.call("ZREM",purgePendingKey, deadMember)
    deadMember = cjson.decode(deadMember)
    table.insert(deadMember[2],instanceToken)
    deadMember = cjson.encode(deadMember)
    redis.call("ZADD",purgePendingKey,currentTimestampInSeconds,deadMember)
    table.insert(acquiredPartitions,deadMember)
end
table.insert(logs,'Acquired Items '.. #acquiredPartitions)
if (#acquiredPartitions < entriesToPurge) then
    local calculatedThreshold = (currentTimestampInSeconds - purgeThresholdInSeconds)-epochInSeconds
    table.insert(logs,'Calculated Threshold In Seconds '.. calculatedThreshold)
    local lastElementWithScore = redis.call('ZRANGEBYSCORE',recentActivityKey,"-inf",calculatedThreshold,"LIMIT",0,(entriesToPurge-#acquiredPartitions))
    table.insert(logs,'New Items '.. #lastElementWithScore)
    for index = 1, #lastElementWithScore do
        local tokenList = {}
        table.insert(tokenList,instanceToken)
        local newAcquiredPartition = {}
        table.insert(newAcquiredPartition,lastElementWithScore[index])
        table.insert(newAcquiredPartition,tokenList)
        newAcquiredPartition = cjson.encode(newAcquiredPartition)
        redis.call("ZADD",purgePendingKey,currentTimestampInSeconds,newAcquiredPartition)
        redis.call("ZREM",recentActivityKey,lastElementWithScore[index])
        table.insert(acquiredPartitions,newAcquiredPartition)
    end
else
    table.insert(logs,'Donot require new items'.. #acquiredPartitions)
end

return {acquiredPartitions,logs};