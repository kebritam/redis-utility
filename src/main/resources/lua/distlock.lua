#!lua name=dislock

local function release_lock(KEYS, ARGV)
    local key = KEYS[1]
    if redis.call('get', key) == ARGV[1] then
        redis.call('del', key)
    end
end

redis.register_function('release_lock', release_lock)