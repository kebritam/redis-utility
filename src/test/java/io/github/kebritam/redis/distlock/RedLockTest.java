package io.github.kebritam.redis.distlock;

import com.redis.testcontainers.RedisStackContainer;
import org.junit.jupiter.api.*;
import org.testcontainers.utility.DockerImageName;
import redis.clients.jedis.HostAndPort;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

class RedLockTest {
    private static Set<RedisStackContainer> containers;
    private static Set<HostAndPort> addresses;

    @BeforeAll
    static void setupAll() {
        DockerImageName redisImageName = DockerImageName.parse("redis/redis-stack:7.2.0-v9-x86_64");
        containers = new HashSet<>();
        containers.add(new RedisStackContainer(redisImageName));
        containers.add(new RedisStackContainer(redisImageName));
        containers.add(new RedisStackContainer(redisImageName));
    }

    @BeforeEach
    void setUp() {
        addresses = new HashSet<>();
        for (RedisStackContainer container : containers) {
            container.start();
            addresses.add(new HostAndPort(container.getRedisHost(), container.getRedisPort()));
        }
    }

    @AfterEach
    void tearDown() {
        for (RedisStackContainer container : containers) {
            container.close();
        }
        addresses.clear();
    }

    @Test
    void operationShouldBeThreadSafeWhenUsingLocking() throws InterruptedException {
        List<Integer> dataHolder = new ArrayList<>(300);

        Thread t1 = Thread.startVirtualThread(() -> {
            DistributedLock lock =
                    new RedLock(addresses, "dist-lock", Duration.ofMillis(5));

            for (int i = 0; i < 100;) {
                if (lock.lock(10_000)) {
                    dataHolder.add(i);
                    lock.release();
                    ++i;
                }
            }
        });

        Thread t2 = Thread.startVirtualThread(() -> {
            DistributedLock lock =
                    new RedLock(addresses, "dist-lock", Duration.ofMillis(5));

            for (int i = 0; i < 100;) {
                if (lock.lock(10_000)) {
                    dataHolder.add(i);
                    lock.release();
                    ++i;
                }
            }
        });

        Thread t3 = Thread.startVirtualThread(() -> {
            DistributedLock lock =
                    new RedLock(addresses, "dist-lock", Duration.ofMillis(5));

            for (int i = 0; i < 100;) {
                if (lock.lock(10_000)) {
                    dataHolder.add(i);
                    lock.release();
                    ++i;
                }
            }
        });

        t1.join();
        t2.join();
        t3.join();

        Assertions.assertEquals(300, dataHolder.size());
    }

    @Test
    void lockShouldReturnFalseWhenLockIsNotAcquired() {
        DistributedLock lock1 =
                new RedLock(addresses, "dist-lock", Duration.ofMillis(5));
        lock1.lock(10_000);

        DistributedLock lock2 =
                new RedLock(addresses, "dist-lock", Duration.ofMillis(5));
        boolean result = lock2.lock(10_000);
        Assertions.assertFalse(result);
    }

    @Test
    void lockShouldWorkFineWithTimeouts() throws InterruptedException {
        DistributedLock lock1 =
                new RedLock(addresses, "dist-lock", Duration.ofMillis(5));
        lock1.lock(50);

        Thread.sleep(60);

        DistributedLock lock2 =
                new RedLock(addresses, "dist-lock", Duration.ofMillis(5));
        boolean result = lock2.lock(1_000);
        Assertions.assertTrue(result);
    }
}