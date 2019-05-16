package com.aliyun.loghub.flume.sink;

import org.apache.flume.conf.Configurable;

public interface Converter<S, T> extends Configurable {

    T convert(S event);
}
