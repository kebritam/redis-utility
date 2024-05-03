package io.github.kebritam.redis.util.distlock;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.params.SetParams;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;

public class LockSingleInstance implements DistributedLock {

    private final JedisPool pool;
    private final String lockName;
    private final String clientToken = UUID.randomUUID().toString();

    public LockSingleInstance(String host, int port, String lockName) {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(2);
        config.setMaxIdle(2);
        config.setMinIdle(1);

        this.pool = new JedisPool(config, host, port);
        this.lockName = lockName;

        try (Jedis jedis = pool.getResource()) {
            byte[] distLockStr = Files.readAllBytes(Path.of("src/main/resources/lua/distlock.lua"));
            jedis.functionLoadReplace(distLockStr);
        } catch (IOException ex) {
            throw new RuntimeException("Exception occurred when reading lua script files.", ex);
        }
    }

    @Override
    public boolean lock(long lockExpireTimeMillis) {
        try (Jedis jedis = this.pool.getResource()) {
            String result = jedis.set(this.lockName, this.clientToken,
                    SetParams.setParams().nx().px(lockExpireTimeMillis));
            return "OK".equals(result);
        }
    }

    @Override
    public void release() {
        try (Jedis jedis = this.pool.getResource()) {
            jedis.fcall("release_lock", List.of(this.lockName), List.of(this.clientToken));

            /*
            I replace the transactional code with a Lua script. It's atomic and faster than transactional.
            The previous code was the following. I won't delete it because it was nice.

            jedis.watch(this.lockName);
            String token = jedis.get(this.lockName);

            if (this.clientToken.equals(token)) {
                // It's our lock
                Transaction transaction = jedis.multi();
                transaction.del(this.lockName);
                transaction.exec();
            } else {
                // It's not our lock!
                jedis.unwatch();
            }
            */
        }
    }
}
