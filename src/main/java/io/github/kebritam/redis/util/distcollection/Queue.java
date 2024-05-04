package io.github.kebritam.redis.util.distcollection;

public interface Queue<E> {

    void pushLast(E e);

    boolean offerLast(E e);

    E popFirst();

    E pollFirst();

    E peekFirst();

    long size();

}
