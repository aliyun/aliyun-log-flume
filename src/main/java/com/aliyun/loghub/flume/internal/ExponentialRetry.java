package com.aliyun.loghub.flume.internal;

import org.apache.flume.Context;

public class ExponentialRetry implements RetryPolicy {

    public static final String ALIAS = "Exponential";

    private int maxRetries;
    private long maxBackoffMs;
    private long initialBackoffMs;
    private double multiplier;

    @Override
    public void configure(Context context) {
        maxRetries = context.getInteger("maxRetries", 10);
        maxBackoffMs = context.getLong("maxBackoffMs", 10 * 1000 * 1000L);
        initialBackoffMs = context.getInteger("initialBackoffMs", 200);
        multiplier = Double.parseDouble(context.getString("multiplier", "1.5"));
    }

    @Override
    public boolean shouldRetry(RetryContext context) {
        return context.getRetryCount() < maxRetries;
    }

    @Override
    public long getBackoffMs(RetryContext context) {
        if (context.getRetryCount() == 0) {
            return initialBackoffMs;
        }
        long lastBackoffMs = context.getLastBackoffMs();
        if (lastBackoffMs >= maxBackoffMs) {
            return maxBackoffMs;
        }
        long waitTimeMs = (long) (Math.floor(lastBackoffMs * multiplier));
        return Math.min(waitTimeMs, maxBackoffMs);
    }
}
