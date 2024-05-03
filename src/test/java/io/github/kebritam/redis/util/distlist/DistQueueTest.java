package io.github.kebritam.redis.util.distlist;

import com.redis.testcontainers.RedisStackContainer;
import io.github.kebritam.redis.util.common.ElementTranslator;
import io.github.kebritam.redis.util.distcollection.list.DistributedList;
import io.github.kebritam.redis.util.distcollection.list.DistributedQueue;
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
        public byte[] serialize(String element) {
            return element.getBytes();
        }
        @Override
        public String deserialize(byte[] bytes) {
            return new String(bytes);
        }
    };

    private DistributedList<String> redisList;

    @BeforeEach
    void setUp() {
        container.start();
        this.redisList = new DistributedQueue<>(container.getRedisHost(), container.getRedisPort(), this.elementTranslator);
    }

    @AfterEach
    void tearDown() {
        container.close();
    }

    @Test
    void sizeShouldReturnSixAfterSixPushes() {
        this.redisList.push("element 1");
        this.redisList.push("element 2");
        this.redisList.push("element 3");
        this.redisList.push("element 4");
        this.redisList.push("element 5");
        this.redisList.push("element 6");

        Assertions.assertEquals(6, this.redisList.size());
    }

    @Test
    void popShouldReturnAndRemoveTheLatestPushed() {
        String elementVal = "element ";

        this.redisList.push(elementVal + "1");
        this.redisList.push(elementVal + "2");
        this.redisList.push(elementVal + "3");
        this.redisList.push(elementVal + "4");
        this.redisList.push(elementVal + "5");

        for (long i = 1 ; i <= this.redisList.size() ; ++i) {
            Assertions.assertEquals(elementVal + i, this.redisList.pop());
        }
    }

    @Test
    void popShouldBlockIfNoElementExists() throws InterruptedException {
        Thread popThread = Thread.ofVirtual().start(() -> {
            try {
                this.redisList.pop();
                Assertions.fail("The method has not blocked the thread");
            } catch (JedisConnectionException e) {
                // This MAY happen because of interrupt
            }

        });

        Thread.sleep(100);
        popThread.interrupt();
    }

    @Test
    void pollShouldReturnTheLatestElement() {
        this.redisList.push("element 1");

        Assertions.assertEquals("element 1", this.redisList.poll());
    }

    @Test
    void pollShouldReturnNullWhenEmpty() {
        Assertions.assertNull(this.redisList.poll());
        Assertions.assertNull(this.redisList.poll());
    }

    @Test
    void peekShouldReturnLatestElementWithoutRemove() {
        this.redisList.push("element 1");

        Assertions.assertEquals("element 1", this.redisList.peek());
        Assertions.assertEquals(1, this.redisList.size());
    }


    @Test
    void concurrentWritesShouldBeThreadSafe() throws InterruptedException {

        Thread t1 = Thread.startVirtualThread(() -> {
            for (int i = 0; i < 5_000; ++i) {
                this.redisList.push("1_element " + i);
            }
        });

        Thread t2 = Thread.startVirtualThread(() -> {
            for (int i = 0; i < 5_000; ++i) {
                this.redisList.push("2_element " + i);
            }
        });

        Thread t3 = Thread.startVirtualThread(() -> {
            for (int i = 0; i < 5_000; ++i) {
                this.redisList.push("3_element " + i);
            }
        });

        t1.join();
        t2.join();
        t3.join();

        Assertions.assertEquals(15_000, this.redisList.size());
    }
}