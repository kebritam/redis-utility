package io.github.kebritam.redis.distcollection;

import com.redis.testcontainers.RedisStackContainer;
import io.github.kebritam.redis.common.ElementTranslator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.utility.DockerImageName;
import redis.clients.jedis.exceptions.JedisConnectionException;

class DistQueueTest {
    private static final RedisStackContainer container
            = new RedisStackContainer(DockerImageName.parse("redis/redis-stack:7.2.0-v9-x86_64"));

    private final ElementTranslator<String> elementTranslator = new ElementTranslator<>() {
        @Override
        public String serialize(String element) { return element; }
        @Override
        public String deserialize(String bytes) { return bytes; }
    };

    private Queue<String> redisQueue;

    @BeforeEach
    void setUp() {
        container.start();
        this.redisQueue = new DistributedQueue<>(
                container.getRedisHost() + ":" + container.getRedisPort(),
                this.elementTranslator);
    }

    @AfterEach
    void tearDown() {
        container.close();
    }

    @Test
    void sizeShouldReturnSixAfterSixPushes() {
        this.redisQueue.pushLast("element 1");
        this.redisQueue.pushLast("element 2");
        this.redisQueue.pushLast("element 3");
        this.redisQueue.pushLast("element 4");
        this.redisQueue.pushLast("element 5");
        this.redisQueue.pushLast("element 6");

        Assertions.assertEquals(6, this.redisQueue.size());
    }

    @Test
    void popFirstShouldReturnAndRemoveTheLatestPushed() {
        String elementVal = "element ";

        this.redisQueue.pushLast(elementVal + "1");
        this.redisQueue.pushLast(elementVal + "2");
        this.redisQueue.pushLast(elementVal + "3");
        this.redisQueue.pushLast(elementVal + "4");
        this.redisQueue.pushLast(elementVal + "5");

        for (long i = 1; i <= this.redisQueue.size() ; ++i) {
            Assertions.assertEquals(elementVal + i, this.redisQueue.popFirst());
        }
    }

    @Test
    void popFirstShouldBlockIfNoElementExists() throws InterruptedException {
        Thread popThread = Thread.ofVirtual().start(() -> {
            try {
                this.redisQueue.popFirst();
                Assertions.fail("The method has not blocked the thread");
            } catch (JedisConnectionException e) {
                // This MAY happen because of interrupt
            }

        });

        Thread.sleep(100);
        popThread.interrupt();
    }

    @Test
    void pollFirstShouldReturnTheLatestElement() {
        this.redisQueue.pushLast("element 1");

        Assertions.assertEquals("element 1", this.redisQueue.pollFirst());
    }

    @Test
    void pollFirstShouldReturnNullWhenEmpty() {
        Assertions.assertNull(this.redisQueue.pollFirst());
        Assertions.assertNull(this.redisQueue.pollFirst());
    }

    @Test
    void peekFirstShouldReturnLatestElementWithoutRemove() {
        this.redisQueue.pushLast("element 1");

        Assertions.assertEquals("element 1", this.redisQueue.peekFirst());
        Assertions.assertEquals(1, this.redisQueue.size());
    }


    @Test
    void concurrentWritesShouldBeThreadSafe() throws InterruptedException {

        Thread t1 = Thread.startVirtualThread(() -> {
            for (int i = 0; i < 5_000; ++i) {
                this.redisQueue.pushLast("1_element " + i);
            }
        });

        Thread t2 = Thread.startVirtualThread(() -> {
            for (int i = 0; i < 5_000; ++i) {
                this.redisQueue.pushLast("2_element " + i);
            }
        });

        Thread t3 = Thread.startVirtualThread(() -> {
            for (int i = 0; i < 5_000; ++i) {
                this.redisQueue.pushLast("3_element " + i);
            }
        });

        t1.join();
        t2.join();
        t3.join();

        Assertions.assertEquals(15_000, this.redisQueue.size());
    }
}