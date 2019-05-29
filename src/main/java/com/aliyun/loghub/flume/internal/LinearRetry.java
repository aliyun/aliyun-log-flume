package com.aliyun.loghub.flume.internal;

import org.apache.flume.Context;

public class LinearRetry implements RetryPolicy {

    public static final String ALIAS = "Linear";

    private int maxRetries;
    private long backoffMs;

    @Override
    public void configure(Context context) {
        maxRetries = context.getInteger("maxRetries", 10);
        backoffMs = context.getLong("backoffMs", 500L);
    }

    @Override
    public boolean shouldRetry(RetryContext context) {
        return context.getRetryCount() < maxRetries;
    }

    @Override
    public long getBackoffMs(RetryContext context) {
        return backoffMs;
    }
}
