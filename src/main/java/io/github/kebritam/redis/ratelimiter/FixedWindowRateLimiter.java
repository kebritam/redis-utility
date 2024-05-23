package io.github.kebritam.redis.ratelimiter;

import redis.clients.jedis.*;

import java.time.LocalDateTime;

public class FixedWindowRateLimiter implements RateLimiter {

    private final JedisPool pool;
    private final int maxUsage;

    public FixedWindowRateLimiter(String redisAddress, int maxUsage) {
        this.pool = new JedisPool(HostAndPort.from(redisAddress), DefaultJedisClientConfig.builder().build());
        this.maxUsage = maxUsage;
    }

    @Override
    public boolean use(String token) {
        Response<Long> requestCount;
        try (Jedis jedis = this.pool.getResource();
             Pipeline pipe = jedis.pipelined()) {
            requestCount = pipe.incr(token + ":" + LocalDateTime.now().getMinute());
            pipe.expire(token, 60);
        }
        Long count = requestCount.get();
        return count <= this.maxUsage;
    }

}
