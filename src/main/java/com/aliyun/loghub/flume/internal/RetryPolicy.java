package com.aliyun.loghub.flume.internal;


import org.apache.flume.conf.Configurable;

public interface RetryPolicy extends Configurable {

    boolean shouldRetry(RetryContext context);
    
    long getBackoffMs(RetryContext context);
}
