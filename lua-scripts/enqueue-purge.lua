local recentActivityKey = KEYS[1]
local purgeStreamKey = KEYS[2]
local purgeThreshold = math.floor(tonumber(ARGV[1])/1000)
local epoch = tonumber(ARGV[2])
local scanEntries = tonumber(ARGV[3])-1
local spaceKey = ARGV[4]
local enquedIds = {}

local tempTime = redis.call("TIME")
local currentTimestamp = tonumber(tempTime[1])

local lastElementWithScore = redis.call('ZRANGE',recentActivityKey,0,scanEntries,'WITHSCORES')
for i = 1, #lastElementWithScore,2 do
    local relativeActivityTime = currentTimestamp - math.floor((tonumber(lastElementWithScore[i+1]) + epoch)/1000)

    if(relativeActivityTime >= purgeThreshold) then 
        local dataToBePurged = redis.call('ZRANGE',(spaceKey ..lastElementWithScore[i]),0,-1,'WITHSCORES')
        local id = redis.call('XADD',purgeStreamKey,'*',lastElementWithScore[i],cjson.encode(dataToBePurged))
        redis.call('ZREM' ,recentActivityKey,lastElementWithScore[i])
        table.insert(enquedIds,id)
    end
    -- table.insert(enquedIds,relativeActivityTime)
    -- table.insert(enquedIds,lastElementWithScore[i])
    -- table.insert(enquedIds,epoch)
    -- table.insert(enquedIds,math.floor((tonumber(lastElementWithScore[i+1]) + epoch)/1000))
    -- table.insert(enquedIds,currentTimestamp)
end
return enquedIds
-- docker run with -v ${PWD}:\"/var/lib/mysql\"
-- CD /var/lib/mysql/lua-scripts
-- redis-cli --eval enqueue-purge.lua rac purge , 0 0 10