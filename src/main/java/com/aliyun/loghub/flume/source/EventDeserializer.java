package com.aliyun.loghub.flume.source;

import com.aliyun.openservices.log.common.FastLogGroup;
import org.apache.flume.Event;
import org.apache.flume.conf.Configurable;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;

public interface EventDeserializer extends Configurable {

    Charset charset = StandardCharsets.UTF_8;

    /**
     * Serializes a LogGroup to one or more Flume events.
     *
     * @param logGroup
     * @return
     */
    List<Event> deserialize(FastLogGroup logGroup);
}
