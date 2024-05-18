package io.github.kebritam.redis.distlock;

public interface DistributedLock {
    boolean lock(long lockExpireTimeMillis);

    void release();
}
