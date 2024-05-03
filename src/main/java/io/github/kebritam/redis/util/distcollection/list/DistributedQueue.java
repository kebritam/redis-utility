package io.github.kebritam.redis.util.distcollection.list;

import io.github.kebritam.redis.util.common.ElementTranslator;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.List;
import java.util.UUID;

public class DistributedQueue<E> implements DistributedList<E> {

    private final byte[] key;

    private final JedisPool pool;
    private final ElementTranslator<E> translator;

    public DistributedQueue(String host, int port, ElementTranslator<E> translator) {
        this.key = ("distributed.queue." + UUID.randomUUID()).getBytes();
        this.pool = new JedisPool(host, port);
        this.translator = translator;
    }

    @Override
    public void push(E e) {
        try (Jedis jedis = this.pool.getResource()) {
            jedis.rpush(key, this.translator.serialize(e));
        }
    }

    @Override
    public E pop() {
        try (Jedis jedis = this.pool.getResource()) {
            List<byte[]> elements = jedis.blpop(0, key);
            return this.translator.deserialize(elements.get(1));
        }
    }

    @Override
    public E poll() {
        try (Jedis jedis = this.pool.getResource()) {
            byte[] element = jedis.lpop(key);
            return element == null ? null : this.translator.deserialize(element);
        }
    }

    @Override
    public E peek() {
        try (Jedis jedis = this.pool.getResource()) {
            List<byte[]> elements = jedis.lrange(key, 0, 0);
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