package io.github.kebritam.redis.ratelimiter;

import com.redis.testcontainers.RedisStackContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;

class FixedWindowRateLimiterTest {
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
        RateLimiter limiter = new FixedWindowRateLimiter(
                container.getRedisHost() + ":" + container.getRedisPort(),
                acceptedCalls);

        for (int i = 0; i < acceptedCalls; i++) {
            Assertions.assertTrue(limiter.use("userUniqueToken"));
        }
        for (int i = 0; i < 60; i++) {
            Assertions.assertFalse(limiter.use("userUniqueToken"));
        }
    }

    @Test
    void shouldAccountRateLimitTokensSeparately() {
        final int acceptedCalls = 50;
        RateLimiter limiter = new FixedWindowRateLimiter(
                container.getRedisHost() + ":" + container.getRedisPort(),
                acceptedCalls);

        for (int i = 0; i < acceptedCalls; i++) {
            Assertions.assertTrue(limiter.use("userUniqueToken1"));
        }
        for (int i = 0; i < acceptedCalls / 2; i++) {
            Assertions.assertTrue(limiter.use("userUniqueToken2"));
        }
        for (int i = 0; i < 40; i++) {
            Assertions.assertFalse(limiter.use("userUniqueToken1"));
        }
        for (int i = 0; i < acceptedCalls / 2; i++) {
            Assertions.assertTrue(limiter.use("userUniqueToken2"));
        }
        for (int i = 0; i < 40; i++) {
            Assertions.assertFalse(limiter.use("userUniqueToken2"));
        }
    }

    @Test
    void shouldStartNewWindowAfterOneMinute() throws InterruptedException {
        final int acceptedCalls = 50;
        RateLimiter limiter = new FixedWindowRateLimiter(
                container.getRedisHost() + ":" + container.getRedisPort(),
                acceptedCalls);

        for (int i = 0; i < acceptedCalls; i++) {
            Assertions.assertTrue(limiter.use("userUniqueToken1"));
        }
        for (int i = 0; i < acceptedCalls / 2; i++) {
            Assertions.assertTrue(limiter.use("userUniqueToken2"));
        }

        Thread.sleep(Duration.ofMinutes(1));

        for (int i = 0; i < acceptedCalls; i++) {
            Assertions.assertTrue(limiter.use("userUniqueToken1"));
        }
        for (int i = 0; i < acceptedCalls; i++) {
            Assertions.assertTrue(limiter.use("userUniqueToken2"));
        }
        for (int i = 0; i < acceptedCalls; i++) {
            Assertions.assertFalse(limiter.use("userUniqueToken1"));
            Assertions.assertFalse(limiter.use("userUniqueToken2"));
        }
    }
}