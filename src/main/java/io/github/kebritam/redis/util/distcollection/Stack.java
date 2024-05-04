package io.github.kebritam.redis.util.distcollection;

public interface Stack<E> {

    void pushFirst(E e);

    boolean offerFirst(E e);

    E popFirst();

    E pollFirst();

    E peekFirst();

    long size();

}
