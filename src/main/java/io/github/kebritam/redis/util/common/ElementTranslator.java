package io.github.kebritam.redis.util.common;

public interface ElementTranslator<E> {

    byte[] serialize(E element);

    E deserialize(byte[] bytes);
}
