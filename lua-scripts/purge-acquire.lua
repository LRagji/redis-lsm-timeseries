--This script is not redis cluster compatible as it uses dynamic keys at runtime which are renamed apart from KEYS passed in.
local recentActivityKey = KEYS[1]
local pendingPurgeKey = KEYS[2]

local purgeThresholdInSeconds = tonumber(ARGV[1])
local epochInSeconds = tonumber(ARGV[2])
local numberOfParitionsToPurge = tonumber(ARGV[3])
local pendingTimeoutInSeconds = tonumber(ARGV[4])
local instanceToken = ARGV[5]
local spaceKey = ARGV[6]
local seperator = ARGV[7]
local purgeFlag = ARGV[8]

local seperatorMatchFormat = seperator .."[^".. seperator .."]*$"
local acquiredPartitions = {}

local tempTime = redis.call("TIME")
local currentTimestampInSeconds = tonumber(tempTime[1])
local currentRelativeTimeInSeconds = currentTimestampInSeconds - epochInSeconds
local relativeEndTimeInSeconds = (currentTimestampInSeconds - purgeThresholdInSeconds) - epochInSeconds

local timedOutPartitions = redis.call('ZRANGEBYSCORE',pendingPurgeKey,"-inf",relativeEndTimeInSeconds,"LIMIT",0, numberOfParitionsToPurge)
for index = 1, #timedOutPartitions do
    local timedOutPartition = timedOutPartitions[index]
    redis.call("ZREM",pendingPurgeKey,timedOutPartition)
    timedOutPartition = cjson.decode(timedOutPartition)
    local dataToBePurged = redis.call('ZRANGE',(spaceKey ..  seperator .. timedOutPartition[1]),0,-1,'WITHSCORES')
    table.insert(timedOutPartition[2],instanceToken)
    timedOutPartition = cjson.encode(timedOutPartition)
    redis.call("ZADD",pendingPurgeKey,currentRelativeTimeInSeconds,timedOutPartition)
    table.insert(acquiredPartitions,{timedOutPartition,dataToBePurged})
end

if(#acquiredPartitions < numberOfParitionsToPurge) then
    local newPartitions = redis.call('ZRANGEBYSCORE',recentActivityKey,"-inf",relativeEndTimeInSeconds,"LIMIT",0, (numberOfParitionsToPurge - #acquiredPartitions))
   
    for index = 1, #newPartitions do
        local completePartitionName = newPartitions[index] --ABC-200-acc
        local partitionName = string.sub(completePartitionName,1,(string.find(completePartitionName,seperatorMatchFormat))-1)  --ABC-200
        local partitionIndexedName = string.sub(partitionName,1,(string.find(partitionName,seperatorMatchFormat))-1) --ABC
        local renamedPartition = partitionName..seperator..purgeFlag --ABC-200-pur
        local existingScore = redis.call("ZSCORE",(spaceKey.. seperator .. partitionIndexedName),completePartitionName)

        redis.call("ZREM",(spaceKey.. seperator .. partitionIndexedName),completePartitionName)
        redis.call("ZADD",(spaceKey.. seperator .. partitionIndexedName),existingScore,renamedPartition)

        redis.call("RENAMENX",(spaceKey.. seperator .. completePartitionName),(spaceKey.. seperator .. renamedPartition))

        redis.call("ZREM",recentActivityKey,completePartitionName)
        
        local pendingMember = {}
        local tokensList = {}
        table.insert(tokensList,instanceToken)
        table.insert(pendingMember,renamedPartition)
        table.insert(pendingMember,tokensList)
        pendingMember = cjson.encode(pendingMember)
        redis.call("ZADD",pendingPurgeKey,relativeEndTimeInSeconds,pendingMember)

        local dataToBePurged = redis.call('ZRANGE',(spaceKey ..  seperator .. renamedPartition),0,-1,'WITHSCORES')
        table.insert(acquiredPartitions,{pendingMember,dataToBePurged})
    end 
end

return acquiredPartitions
-- docker run with -v ${PWD}:\"/var/lib/mysql\"
-- cd /var/lib/mysql/lua-scripts
-- redis-cli --eval purge-acquire.lua space-rac space-pen , 1 0 1 10 token1 space - pur