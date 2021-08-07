local recentActivityKey = KEYS[1]
local purgeStreamKey = KEYS[2]
local purgeThreshold = tonumber(ARGV[1])
local epoch = tonumber(ARGV[2])
local scanEntries = tonumber(ARGV[3]) *-1
local spaceKey = ARGV[4]
local tempTime = redis.call("TIME")
local currentTimestamp = tonumber(math.floor(tempTime[2])+(tempTime[1])*1000)
local enquedIds = {}

local lastElementWithScore = redis.call('ZRANGE',recentActivityKey,scanEntries,-1,'WITHSCORES')
for i = 1, #lastElementWithScore,2 do
    lastElementWithScore[i+1] = currentTimestamp - (lastElementWithScore[i+1] + epoch)

    if(lastElementWithScore[i+1] > purgeThreshold) then 
        local dataToBePurged = redis.call('ZRANGE',(spaceKey ..lastElementWithScore[i]),0,-1,'WITHSCORES')
        local id = redis.call('XADD',purgeStreamKey,'*',lastElementWithScore[i],cjson.encode(dataToBePurged))
        redis.call('ZREM' ,recentActivityKey,lastElementWithScore[i])
        table.insert(enquedIds,id)
    end
end
return enquedIds
-- docker run with -v ${PWD}:\"/var/lib/mysql\"
-- CD /var/lib/mysql/lua-scripts
-- redis-cli --eval enqueue-purge.lua rac purge , 0 0 10