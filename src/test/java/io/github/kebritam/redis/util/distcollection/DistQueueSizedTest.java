package io.github.kebritam.redis.util.distcollection;

import com.redis.testcontainers.RedisStackContainer;
import io.github.kebritam.redis.util.common.ElementTranslator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.utility.DockerImageName;

class DistQueueSizedTest {
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
                this.elementTranslator,
                4);
    }

    @AfterEach
    void tearDown() {
        container.close();
    }

    @Test
    void sizeShouldReturnFourAfterSixPushesAndTwoFirstElementsShouldDrop() {
        this.redisQueue.pushLast("element 1");
        this.redisQueue.pushLast("element 2");
        this.redisQueue.pushLast("element 3");
        this.redisQueue.pushLast("element 4");
        this.redisQueue.pushLast("element 5");
        this.redisQueue.pushLast("element 6");

        Assertions.assertEquals(4, this.redisQueue.size());
    }

    @Test
    void offerLastShouldReturnFalseIfQueueIsFullAndTrueIfInsertIsSuccessful() {
        boolean res1 = this.redisQueue.offerLast("element 1");
        boolean res2 = this.redisQueue.offerLast("element 2");
        boolean res3 = this.redisQueue.offerLast("element 3");
        boolean res4 = this.redisQueue.offerLast("element 4");
        boolean res5 = this.redisQueue.offerLast("element 5");
        boolean res6 = this.redisQueue.offerLast("element 6");

        Assertions.assertTrue(res1);
        Assertions.assertTrue(res2);
        Assertions.assertTrue(res3);
        Assertions.assertTrue(res4);
        Assertions.assertFalse(res5);
        Assertions.assertFalse(res6);
        Assertions.assertEquals(4, this.redisQueue.size());
    }

    @Test
    void popFirstShouldReturnAndRemoveTheLatestPushed() {
        this.redisQueue.pushLast("element 1");
        this.redisQueue.pushLast("element 2");
        this.redisQueue.pushLast("element 3");
        this.redisQueue.pushLast("element 4");
        this.redisQueue.pushLast("element 5");
        this.redisQueue.pushLast("element 6");

        Assertions.assertEquals("element 3", this.redisQueue.popFirst());
        Assertions.assertEquals("element 4", this.redisQueue.popFirst());
        Assertions.assertEquals("element 5", this.redisQueue.popFirst());
        Assertions.assertEquals("element 6", this.redisQueue.popFirst());
        Assertions.assertEquals(0, this.redisQueue.size());
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

        Assertions.assertEquals(4, this.redisQueue.size());
    }
}