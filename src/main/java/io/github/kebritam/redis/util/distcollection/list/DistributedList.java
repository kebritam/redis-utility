package io.github.kebritam.redis.util.distcollection.list;

public interface DistributedList<E> {

    void push(E e);

    E pop();

    E poll();

    E peek();

    long size();

}
