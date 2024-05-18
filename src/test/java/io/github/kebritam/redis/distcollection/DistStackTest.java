package io.github.kebritam.redis.distcollection;

import com.redis.testcontainers.RedisStackContainer;
import io.github.kebritam.redis.common.ElementTranslator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.utility.DockerImageName;
import redis.clients.jedis.exceptions.JedisConnectionException;

class DistStackTest {

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
                this.elementTranslator);
    }

    @AfterEach
    void tearDown() {
        container.close();
    }

    @Test
    void sizeShouldReturnSixAfterSixPushes() {
        this.redisStack.pushFirst("element 1");
        this.redisStack.pushFirst("element 2");
        this.redisStack.pushFirst("element 3");
        this.redisStack.pushFirst("element 4");
        this.redisStack.pushFirst("element 5");
        this.redisStack.pushFirst("element 6");

        Assertions.assertEquals(6, this.redisStack.size());
    }

    @Test
    void popFirstShouldReturnAndRemoveTheLatestPushed() {
        String elementVal = "element ";

        this.redisStack.pushFirst(elementVal + "1");
        this.redisStack.pushFirst(elementVal + "2");
        this.redisStack.pushFirst(elementVal + "3");
        this.redisStack.pushFirst(elementVal + "4");
        this.redisStack.pushFirst(elementVal + "5");

        Assertions.assertEquals(5, this.redisStack.size());

        Assertions.assertEquals(elementVal + "5", this.redisStack.popFirst());
        Assertions.assertEquals(elementVal + "4", this.redisStack.popFirst());
        Assertions.assertEquals(elementVal + "3", this.redisStack.popFirst());
        Assertions.assertEquals(elementVal + "2", this.redisStack.popFirst());
        Assertions.assertEquals(elementVal + "1", this.redisStack.popFirst());
        Assertions.assertEquals(0, this.redisStack.size());
    }

    @Test
    void popFirstShouldBlockIfNoElementExists() throws InterruptedException {
        Thread popThread = Thread.ofVirtual().start(() -> {
            try {
                this.redisStack.popFirst();
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
        this.redisStack.pushFirst("element 1");

        Assertions.assertEquals("element 1", this.redisStack.pollFirst());
    }

    @Test
    void pollFirstShouldReturnNullWhenEmpty() {
        Assertions.assertNull(this.redisStack.pollFirst());
        Assertions.assertNull(this.redisStack.pollFirst());
    }

    @Test
    void peekFirstShouldReturnLatestElementWithoutRemove() {
        this.redisStack.pushFirst("element 1");

        Assertions.assertEquals("element 1", this.redisStack.peekFirst());
        Assertions.assertEquals(1, this.redisStack.size());
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

        Assertions.assertEquals(15_000, this.redisStack.size());
    }
}