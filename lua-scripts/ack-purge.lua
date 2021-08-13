local recentActivityKey = KEYS[1]
local purgeStreamKey = KEYS[2]
local indexKey
local purgeAckId = ARGV[1]
local seperator = ARGV[2]
local spaceKey = ARGV[3]

local data = redis.call('XRANGE',purgeStreamKey,purgeAckId,purgeAckId)
if (#data > 0) then 
    local cleanUpKey = data[1][2][1]
    indexKey = string.sub(cleanUpKey,0,(string.find(cleanUpKey, (seperator.."[^"..seperator.."]*$"))-1))
    local elements = cjson.decode(data[1][2][2])

    local membersToDelete = {}
    for i = 1, #elements,2 do
        table.insert(membersToDelete,elements[i])
    end

    if (#membersToDelete > 0) then
        redis.call('ZREM',spaceKey .. seperator .. cleanUpKey,unpack(membersToDelete))
    end

    if (redis.call('EXISTS',spaceKey .. seperator .. cleanUpKey) == 0) then
        redis.call('ZREM', spaceKey .. seperator .. indexKey, cleanUpKey)
    end 

    return 1
else
    return 0
end 
--return 'OK'
-- docker run with -v ${PWD}:\"/var/lib/mysql\"
-- CD /var/lib/mysql/lua-scripts
-- redis-cli --eval ack-purge.lua rac purge idx , 1628006107022-0 grp prefixKey