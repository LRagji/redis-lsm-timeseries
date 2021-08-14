local recentActivityKey = KEYS[1]
local purgeStreamKey = KEYS[2]
local partitionKey = KEYS[3]
local indexKey = KEYS[4]
local purgeAckId = ARGV[1]

local data = redis.call('XRANGE',purgeStreamKey,purgeAckId,purgeAckId)
if (#data > 0) then 
    local partitionKeyWithoutSpaceKey = data[1][2][1]
    local elements = cjson.decode(data[1][2][2])

    local membersToDelete = {}
    for i = 1, #elements,2 do
        table.insert(membersToDelete,elements[i])
    end

    if (#membersToDelete > 0) then
        redis.call('ZREM',partitionKey,unpack(membersToDelete))
    end

    if (redis.call('EXISTS',partitionKey) == 0) then
        redis.call('ZREM', indexKey, partitionKeyWithoutSpaceKey)
    end 

    return 1
else
    return 0
end 
--return 'OK'
-- docker run with -v ${PWD}:\"/var/lib/mysql\"
-- CD /var/lib/mysql/lua-scripts
-- redis-cli --eval ack-purge.lua rac purge idx , 1628006107022-0 grp prefixKey