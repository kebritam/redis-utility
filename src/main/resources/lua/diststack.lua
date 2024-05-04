#!lua name=diststack

local function offer_first(KEYS, ARGV)
    local key = KEYS[1]
    local max_size = tonumber(ARGV[1])
    local data = ARGV[2]
    if max_size <= 0 or redis.call('LLEN', key) < max_size then
        redis.call('RPUSH', key, data)
        return true
    else
        return false
    end
end

local function push_first(KEYS, ARGV)
    local key = KEYS[1]
    local max_size = tonumber(ARGV[1])
    local data = ARGV[2]
    if max_size > 0 and redis.call('LLEN', key) >= max_size then
        redis.call('LPOP', key)
    end
    redis.call('RPUSH', key, data)
end

redis.register_function('offer_first', offer_first)
redis.register_function('push_first', push_first)