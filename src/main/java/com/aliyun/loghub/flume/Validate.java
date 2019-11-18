package com.aliyun.loghub.flume;

import static com.google.common.base.Preconditions.checkArgument;

public final class Validate {

    public static void notEmpty(String value, String name) {
        checkArgument(value != null && !value.isEmpty(), "Missing parameter: " + name);
    }
}
