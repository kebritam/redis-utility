package io.github.kebritam.redis.util.distcollection;

import io.github.kebritam.redis.util.common.ElementTranslator;
import io.github.kebritam.redis.util.common.RedisHelpers;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.nio.file.Path;
import java.util.List;
import java.util.UUID;

public class DistributedQueue<E> implements Queue<E> {

    private final String key;

    private final JedisPool pool;
    private final ElementTranslator<E> translator;
    private final int maxSize;

    public DistributedQueue(String redisAddress, ElementTranslator<E> translator) {
        this(redisAddress, translator, -1);
    }

    public DistributedQueue(String redisAddress, ElementTranslator<E> translator, int maxSize) {
        this.key = "distributed.queue." + UUID.randomUUID();
        this.pool = new JedisPool(HostAndPort.from(redisAddress), DefaultJedisClientConfig.builder().build());
        this.translator = translator;
        this.maxSize = maxSize;

        byte[] luaScript = RedisHelpers.getLuaScript(Path.of("src/main/resources/lua/distqueue.lua"));
        try (Jedis jedis = this.pool.getResource()) {
            jedis.functionLoadReplace(luaScript);
        }
    }

    @Override
    public void pushLast(E e) {
        try (Jedis jedis = pool.getResource()) {
            jedis.fcall(
                    "push_last",
                    List.of(this.key),
                    List.of(String.valueOf(this.maxSize), this.translator.serialize(e)));
        }
    }

    @Override
    public boolean offerLast(E e) {
        try (Jedis jedis = pool.getResource()) {
            Object res = jedis.fcall(
                    "offer_last",
                    List.of(this.key),
                    List.of(String.valueOf(this.maxSize), this.translator.serialize(e)));
            return res != null;
        }
    }

    @Override
    public E popFirst() {
        try (Jedis jedis = this.pool.getResource()) {
            List<String> elements = jedis.blpop(0, key);
            return this.translator.deserialize(elements.get(1));
        }
    }

    @Override
    public E pollFirst() {
        try (Jedis jedis = this.pool.getResource()) {
            String element = jedis.lpop(key);
            return element == null ? null : this.translator.deserialize(element);
        }
    }

    @Override
    public E peekFirst() {
        try (Jedis jedis = this.pool.getResource()) {
            List<String> elements = jedis.lrange(key, 0, 0);
            return elements.isEmpty() ? null : this.translator.deserialize(elements.getFirst());
        }
    }

    @Override
    public long size() {
        try (Jedis jedis = this.pool.getResource()) {
            return jedis.llen(key);
        }
    }
}
