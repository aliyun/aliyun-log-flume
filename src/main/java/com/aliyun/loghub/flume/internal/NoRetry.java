package com.aliyun.loghub.flume.internal;

import org.apache.flume.Context;

public class NoRetry implements RetryPolicy {
    public static final String ALIAS = "Never";

    @Override
    public void configure(Context context) {
        //No-op
    }

    @Override
    public boolean shouldRetry(RetryContext context) {
        return false;
    }

    @Override
    public long getBackoffMs(RetryContext context) {
        return 0;
    }
}
