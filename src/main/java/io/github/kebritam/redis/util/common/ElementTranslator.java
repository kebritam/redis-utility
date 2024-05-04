package io.github.kebritam.redis.util.common;

public interface ElementTranslator<E> {

    String serialize(E element);

    E deserialize(String bytes);
}
