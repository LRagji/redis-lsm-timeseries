local PurgePendingKey = KEYS[1]
local Partition = KEYS[2]

local purgeMember = ARGV[1]

redis.call("ZREM",PurgePendingKey,purgeMember)
redis.call("DEL",Partition)

return 1

-- docker run with -v ${PWD}:\"/var/lib/mysql\"
-- cd /var/lib/mysql/lua-scripts
-- redis-cli --eval purge-release.lua space-pen space-abc-0-pur space-monitor , laukik