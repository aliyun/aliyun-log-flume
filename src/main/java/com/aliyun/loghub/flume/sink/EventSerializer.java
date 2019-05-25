package com.aliyun.loghub.flume.sink;

import com.aliyun.openservices.log.common.LogItem;
import org.apache.flume.Event;
import org.apache.flume.conf.Configurable;

public interface EventSerializer extends Configurable {

    /**
     * Serialize event to log item.
     *
     * @param event
     * @return
     */
    LogItem serialize(Event event);
}
