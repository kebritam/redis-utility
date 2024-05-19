package io.github.kebritam.redis.ratelimiter;

import com.redis.testcontainers.RedisStackContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.utility.DockerImageName;

class SlidingWindowRateLimiterTest {
    private static final RedisStackContainer container
            = new RedisStackContainer(DockerImageName.parse("redis/redis-stack:7.2.0-v9-x86_64"));

    @BeforeEach
    void setUp() {
        container.start();
    }

    @AfterEach
    void tearDown() {
        container.close();
    }

    @Test
    void shouldReturnTrueFor50CallsAndReturnFalseForOtherForTheSameKey() {
        final int acceptedCalls = 50;
        RateLimiter limiter = new SlidingWindowRateLimiter(
                container.getRedisHost() + ":" + container.getRedisPort(),
                acceptedCalls, 2);

        for (int i = 0; i < acceptedCalls; i++) {
            int finalI = i;
            Assertions.assertTrue(limiter.use("userUniqueToken"), () -> "At index of " + finalI);
        }
        for (int i = 0; i < 60; i++) {
            int finalI = i;
            Assertions.assertFalse(limiter.use("userUniqueToken"), () -> "At index of " + finalI);
        }
    }

    @Test
    void shouldRefillAfterWindowFinishes() throws InterruptedException {
        final int acceptedCalls = 50;
        RateLimiter limiter = new SlidingWindowRateLimiter(
                container.getRedisHost() + ":" + container.getRedisPort(),
                acceptedCalls, 2);

        for (int i = 0; i < acceptedCalls; i++) {
            int finalI = i;
            Assertions.assertTrue(limiter.use("userUniqueToken"), () -> "At index of " + finalI);
        }
        for (int i = 0; i < 60; i++) {
            int finalI = i;
            Assertions.assertFalse(limiter.use("userUniqueToken"), () -> "At index of " + finalI);
        }

        Thread.sleep(2_000);

        for (int i = 0; i < acceptedCalls; i++) {
            int finalI = i;
            Assertions.assertTrue(limiter.use("userUniqueToken"), () -> "At index of " + finalI);
        }
        for (int i = 0; i < 60; i++) {
            int finalI = i;
            Assertions.assertFalse(limiter.use("userUniqueToken"), () -> "At index of " + finalI);
        }
    }

    @Test
    void shouldRefillIfRequestsAreNotBursting() throws InterruptedException {
        final int acceptedCalls = 50;
        RateLimiter limiter = new SlidingWindowRateLimiter(
                container.getRedisHost() + ":" + container.getRedisPort(),
                acceptedCalls, 1);

        for (int i = 0; i < 2 * acceptedCalls; i++) {
            int finalI = i;
            Assertions.assertTrue(limiter.use("userUniqueToken"), () -> "At index of " + finalI);
            Thread.sleep(20);
        }

    }
}