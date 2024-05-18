package io.github.kebritam.redis.distlock;

import io.github.kebritam.redis.common.RedisHelpers;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.params.SetParams;

import java.nio.file.Path;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

public class RedLock implements DistributedLock {

    private final Set<JedisPool> jedisPools;
    private final String lockName;
    private final String clientToken = UUID.randomUUID().toString();

    public RedLock(Set<HostAndPort> clusterNodes, String lockName, Duration nodeTimeout) {
        this.lockName = lockName;
        this.jedisPools = new HashSet<>();

        byte[] luaScript = RedisHelpers.getLuaScript(Path.of("src/main/resources/lua/distlock.lua"));
        for (HostAndPort hostAndPort : clusterNodes) {
            JedisPoolConfig poolConfig = new JedisPoolConfig();
            poolConfig.setMaxWait(nodeTimeout);
            JedisPool pool = new JedisPool(hostAndPort.getHost(), hostAndPort.getPort());

            try (Jedis jedis = pool.getResource()) {
                jedis.functionLoadReplace(luaScript);
            }
            this.jedisPools.add(pool);
        }
    }

    @Override
    public boolean lock(long lockExpireTimeMillis) {
        long startTime = System.currentTimeMillis();
        int successCount = 0;
        for (JedisPool jedisPool : jedisPools) {
            try(Jedis jedis = jedisPool.getResource()) {
                String result = jedis.set(lockName, clientToken,
                        SetParams.setParams().nx().px(lockExpireTimeMillis));

                if ("OK".equals(result)) {
                    ++successCount;
                }
            }
        }

        if (successCount > jedisPools.size() / 2 && System.currentTimeMillis() - startTime < lockExpireTimeMillis) {
            return true;
        }
        this.release();
        return false;
    }

    @Override
    public void release() {
        for (JedisPool pool : jedisPools) {
            try (Jedis jedis = pool.getResource()) {
                jedis.fcall("release_lock", List.of(this.lockName), List.of(this.clientToken));
            }
        }
    }
}
