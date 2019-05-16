package com.aliyun.loghub.flume;

public final class Utils {

    public static void ensureNotEmpty(String value, String name) {
        if (value != null && !value.isEmpty()) {
            throw new IllegalArgumentException("Missing parameter: " + name);
        }
    }
}
