--This script is not redis cluster compatible as it uses dynamic keys at runtime which are renamed apart from KEYS passed in.
local ActivityKey = KEYS[1]
local SamplesPerPartitionKey = KEYS[2]
local PurgePendingKey = KEYS[3]

local partitionsToAcquire = tonumber(ARGV[1])
local timeThreshold = tonumber(ARGV[2])
local countThreshold = tonumber(ARGV[3])
local reAcquireTimeout = tonumber(ARGV[4])
local instanceHash = ARGV[5]
local seperator = ARGV[6]
local spaceKey = ARGV[7]
local purgeMarker = ARGV[8]

local acquiredPartitions = {}

local tempTime = redis.call("TIME")
local currentTimestampInSeconds = tonumber(tempTime[1])
local timeTillToAcquirePartitions = (currentTimestampInSeconds - timeThreshold)
local timeTillReAcquirePartitions = (currentTimestampInSeconds - reAcquireTimeout)

local timedOutPartitions = redis.call('ZRANGEBYSCORE',PurgePendingKey,"-inf",timeTillReAcquirePartitions,"LIMIT",0, partitionsToAcquire)
for index = 1, #timedOutPartitions do
    local timedOutPartitionName = timedOutPartitions[index]
    local dataToBePurged = redis.call('ZRANGE',(spaceKey ..  seperator .. timedOutPartitionName ..purgeMarker ),0,-1,'WITHSCORES')
    redis.call("ZADD",PurgePendingKey,currentRelativeTimeInSeconds,timedOutPartitionName)
    table.insert(acquiredPartitions,{timedOutPartitionName,dataToBePurged})
end

if(#acquiredPartitions < numberOfParitionsToPurge and timeThreshold > 0) then
    local newPartitions = redis.call('ZRANGEBYSCORE',ActivityKey,"-inf",timeTillToAcquirePartitions,"LIMIT",0, (partitionsToAcquire - #acquiredPartitions))
   
    for index = 1, #newPartitions do
        local completePartitionName = newPartitions[index] --ABC-200
        local renamedPartition = completePartitionName..seperator..purgeMarker --ABC-200-pur
       
        redis.call("RENAMENX",(spaceKey.. seperator .. completePartitionName),(spaceKey.. seperator .. renamedPartition))
        redis.call("ZREM",ActivityKey,completePartitionName)
        redis.call("ZADD",PurgePendingKey,currentRelativeTimeInSeconds,completePartitionName)
    
        local dataToBePurged = redis.call('ZRANGE',(spaceKey ..  seperator .. completePartitionName ),0,-1,'WITHSCORES')
        table.insert(acquiredPartitions,{completePartitionName,dataToBePurged})
    end 
end

if(#acquiredPartitions < numberOfParitionsToPurge and countThreshold > 0 ) then
    local newPartitions = redis.call('ZRANGEBYSCORE',SamplesPerPartitionKey,countThreshold,"+inf","LIMIT",0, (partitionsToAcquire - #acquiredPartitions))
   
    for index = 1, #newPartitions do
        local completePartitionName = newPartitions[index] --ABC-200
        local renamedPartition = completePartitionName..seperator..purgeMarker --ABC-200-pur
       
        redis.call("RENAMENX",(spaceKey.. seperator .. completePartitionName),(spaceKey.. seperator .. renamedPartition))
        redis.call("ZREM",ActivityKey,completePartitionName)
        redis.call("ZADD",PurgePendingKey,currentRelativeTimeInSeconds,completePartitionName)
    
        local dataToBePurged = redis.call('ZRANGE',(spaceKey ..  seperator .. completePartitionName ),0,-1,'WITHSCORES')
        table.insert(acquiredPartitions,{completePartitionName,dataToBePurged})
    end 
end

return acquiredPartitions
-- docker run with -v ${PWD}:\"/var/lib/mysql\"
-- cd /var/lib/mysql/lua-scripts
-- redis-cli --eval purge-acquire.lua space-rac space-pen , 1 0 1 10 token1 space - pur
-- Stringyfied Response [["[\"GapTag-0-pur\",[\"2b8pjZTY>G\"]]",["{\"p\":\"One\",\"u\":\"1629634010341-2b8pjZTY>G-0\"}","1","{\"p\":\"Two\",\"u\":\"1629634010341-2b8pjZTY>G-1\"}","2"]],["[\"GapTag-10-pur\",[\"2b8pjZTY>G\"]]",["{\"p\":\"Ten\",\"u\":\"1629634010341-2b8pjZTY>G-2\"}","0"]],["[\"GapTag-20-pur\",[\"2b8pjZTY>G\"]]",["{\"p\":\"Twenty\",\"u\":\"1629634010341-2b8pjZTY>G-3\"}","0"]],["[\"SerialTag-0-pur\",[\"2b8pjZTY>G\"]]",["{\"p\":\"One\",\"u\":\"1629634010341-2b8pjZTY>G-4\"}","1","{\"p\":\"Two\",\"u\":\"1629634010341-2b8pjZTY>G-5\"}","2","{\"p\":\"Three\",\"u\":\"1629634010341-2b8pjZTY>G-6\"}","3","{\"p\":\"Four\",\"u\":\"1629634010341-2b8pjZTY>G-7\"}","4"]]]