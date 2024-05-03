package io.github.kebritam.redis.util.distlock;

public interface DistributedLock {
    boolean lock(long lockExpireTimeMillis);

    void release();
}
