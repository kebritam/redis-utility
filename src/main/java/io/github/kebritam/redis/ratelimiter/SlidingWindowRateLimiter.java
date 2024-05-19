package io.github.kebritam.redis.ratelimiter;

import redis.clients.jedis.*;

public class SlidingWindowRateLimiter implements RateLimiter {

    private final JedisPool pool;
    private final int maxUsage;
    private final long windowLength;

    public SlidingWindowRateLimiter(String redisAddress, int maxUsage, int windowLength) {
        this.pool = new JedisPool(HostAndPort.from(redisAddress), DefaultJedisClientConfig.builder().build());
        this.maxUsage = maxUsage;
        this.windowLength = windowLength * 1_000_000_000L;
    }

    @Override
    public boolean use(String token) {
        long currentTime = System.nanoTime();
        long windowStart = currentTime - this.windowLength;

        Response<Long> response;
        try (Jedis jedis = this.pool.getResource();
             Pipeline pipe = jedis.pipelined()) {
            pipe.zremrangeByScore(token, 0, windowStart);
            response = pipe.zcard(token);
            pipe.zadd(token, currentTime, String.valueOf(currentTime));
            pipe.expire(token, this.windowLength);
            pipe.sync();
        }
        return response.get() < this.maxUsage;
    }
}
