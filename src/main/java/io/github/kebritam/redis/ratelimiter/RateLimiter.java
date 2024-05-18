package io.github.kebritam.redis.ratelimiter;

public interface RateLimiter {
    boolean use(String token);
}
