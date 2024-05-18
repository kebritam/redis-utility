package io.github.kebritam.redis.distcollection;

import com.redis.testcontainers.RedisStackContainer;
import io.github.kebritam.redis.common.ElementTranslator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.utility.DockerImageName;

class DistStackSizedTest {

    private static final RedisStackContainer container
            = new RedisStackContainer(DockerImageName.parse("redis/redis-stack:7.2.0-v9-x86_64"));

    private final ElementTranslator<String> elementTranslator = new ElementTranslator<>() {
        @Override
        public String serialize(String element) { return element; }
        @Override
        public String deserialize(String bytes) { return bytes; }
    };

    private Stack<String> redisStack;

    @BeforeEach
    void setUp() {
        container.start();
        this.redisStack = new DistributedStack<>(
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
        this.redisStack.pushFirst("element 1");
        this.redisStack.pushFirst("element 2");
        this.redisStack.pushFirst("element 3");
        this.redisStack.pushFirst("element 4");
        this.redisStack.pushFirst("element 5");
        this.redisStack.pushFirst("element 6");

        Assertions.assertEquals(4, this.redisStack.size());
    }

    @Test
    void offerLastShouldReturnFalseIfQueueIsFullAndTrueIfInsertIsSuccessful() {
        boolean res1 = this.redisStack.offerFirst("element 1");
        boolean res2 = this.redisStack.offerFirst("element 2");
        boolean res3 = this.redisStack.offerFirst("element 3");
        boolean res4 = this.redisStack.offerFirst("element 4");
        boolean res5 = this.redisStack.offerFirst("element 5");
        boolean res6 = this.redisStack.offerFirst("element 6");

        Assertions.assertTrue(res1);
        Assertions.assertTrue(res2);
        Assertions.assertTrue(res3);
        Assertions.assertTrue(res4);
        Assertions.assertFalse(res5);
        Assertions.assertFalse(res6);
        Assertions.assertEquals(4, this.redisStack.size());
    }

    @Test
    void popFirstShouldReturnAndRemoveTheLatestPushed() {
        this.redisStack.pushFirst("element 1");
        this.redisStack.pushFirst("element 2");
        this.redisStack.pushFirst("element 3");
        this.redisStack.pushFirst("element 4");
        this.redisStack.pushFirst("element 5");
        this.redisStack.pushFirst("element 6");

        Assertions.assertEquals("element 6", this.redisStack.popFirst());
        Assertions.assertEquals("element 5", this.redisStack.popFirst());
        Assertions.assertEquals("element 4", this.redisStack.popFirst());
        Assertions.assertEquals("element 3", this.redisStack.popFirst());
        Assertions.assertEquals(0, this.redisStack.size());
    }

    @Test
    void concurrentWritesShouldBeThreadSafe() throws InterruptedException {

        Thread t1 = Thread.startVirtualThread(() -> {
            for (int i = 0; i < 5_000; ++i) {
                this.redisStack.pushFirst("1_element " + i);
            }
        });

        Thread t2 = Thread.startVirtualThread(() -> {
            for (int i = 0; i < 5_000; ++i) {
                this.redisStack.pushFirst("2_element " + i);
            }
        });

        Thread t3 = Thread.startVirtualThread(() -> {
            for (int i = 0; i < 5_000; ++i) {
                this.redisStack.pushFirst("3_element " + i);
            }
        });

        t1.join();
        t2.join();
        t3.join();

        Assertions.assertEquals(4, this.redisStack.size());
    }
}