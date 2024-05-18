package io.github.kebritam.redis.distlock;

import com.redis.testcontainers.RedisStackContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.utility.DockerImageName;

import java.util.ArrayList;
import java.util.List;

class LockSingleInstanceTest {
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
    void operationShouldBeThreadSafeWhenUsingLocking() throws InterruptedException {
        List<Integer> dataHolder = new ArrayList<>(3_000);

        Thread t1 = Thread.startVirtualThread(() -> {
            DistributedLock lock =
                    new LockSingleInstance(container.getRedisHost(), container.getRedisPort(), "dist-lock");

            for (int i = 0; i < 1_000;) {
                if (lock.lock(20_000)) {
                    dataHolder.add(i);
                    lock.release();
                    ++i;
                }
            }
        });

        Thread t2 = Thread.startVirtualThread(() -> {
            DistributedLock lock =
                    new LockSingleInstance(container.getRedisHost(), container.getRedisPort(), "dist-lock");

            for (int i = 0; i < 1_000;) {
                if (lock.lock(20_000)) {
                    dataHolder.add(i);
                    lock.release();
                    ++i;
                }
            }
        });

        Thread t3 = Thread.startVirtualThread(() -> {
            DistributedLock lock =
                    new LockSingleInstance(container.getRedisHost(), container.getRedisPort(), "dist-lock");

            for (int i = 0; i < 1_000;) {
                if (lock.lock(20_000)) {
                    dataHolder.add(i);
                    lock.release();
                    ++i;
                }
            }
        });

        t1.join();
        t2.join();
        t3.join();

        Assertions.assertEquals(3_000, dataHolder.size());
    }

    @Test
    void lockShouldReturnFalseWhenLockIsNotAcquired() {
        DistributedLock lock1 =
                new LockSingleInstance(container.getRedisHost(), container.getRedisPort(), "dist-lock");
        lock1.lock(20_000);

        DistributedLock lock2 =
                new LockSingleInstance(container.getRedisHost(), container.getRedisPort(), "dist-lock");
        boolean result = lock2.lock(20_000);
        Assertions.assertFalse(result);
    }

    @Test
    void lockShouldWorkFineWithTimeouts() throws InterruptedException {
        DistributedLock lock1 =
                new LockSingleInstance(container.getRedisHost(), container.getRedisPort(), "dist-lock");
        lock1.lock(50);

        Thread.sleep(50);

        DistributedLock lock2 =
                new LockSingleInstance(container.getRedisHost(), container.getRedisPort(), "dist-lock");
        boolean result = lock2.lock(1_000);
        Assertions.assertTrue(result);
    }
}