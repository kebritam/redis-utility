package io.github.kebritam.redis.util.common;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class RedisHelpers {
    public static byte[] getLuaScript(Path file) {
        try {
            return Files.readAllBytes(file);
        } catch (IOException ex) {
            throw new RuntimeException("Exception occurred when reading lua script files.", ex);
        }
    }
}
