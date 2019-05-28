package com.aliyun.loghub.flume.internal;

public class RetryContext {

    private int retryCount = 0;
    private long lastBackoffMs = 0;

    public int getRetryCount() {
        return retryCount;
    }

    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount;
    }

    public long getLastBackoffMs() {
        return lastBackoffMs;
    }

    public void setLastBackoffMs(long lastBackoffMs) {
        this.lastBackoffMs = lastBackoffMs;
    }

    public void incrementRetryCount() {
        this.retryCount += 1;
    }

    public void reset() {
        this.retryCount = 0;
        this.lastBackoffMs = 0;
    }
}
