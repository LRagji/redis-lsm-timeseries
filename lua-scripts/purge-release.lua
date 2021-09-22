local PurgePendingKey = KEYS[1]
local Partition = KEYS[2]
local OutputRateKey = KEYS[3]

local purgeMember = ARGV[1]

redis.call("ZREM",PurgePendingKey,purgeMember)
local sampleCount = redis.call("ZCARD",Partition);
redis.call("DEL",Partition)
redis.call("INCRBY",OutputRateKey,sampleCount);
return 1

-- docker run with -v ${PWD}:\"/var/lib/mysql\"
-- cd /var/lib/mysql/lua-scripts
-- redis-cli --eval purge-release.lua space-pen space-abc-0-pur space-monitor , laukik