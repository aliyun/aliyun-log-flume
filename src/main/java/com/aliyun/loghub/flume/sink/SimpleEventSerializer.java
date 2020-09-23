package com.aliyun.loghub.flume.sink;

import com.aliyun.openservices.log.common.LogItem;
import org.apache.flume.Context;
import org.apache.flume.Event;

import java.io.UnsupportedEncodingException;

public class SimpleEventSerializer implements EventSerializer {

    static final String ALIAS = "SIMPLE";
    private String fieldName;
    private String encoding;

    private static final String DEFAULT_FIELD_NAME = "body";

    @Override
    public void configure(Context context) {
        encoding = context.getString("encoding", "UTF-8");
        fieldName = context.getString("fieldName");
        if (fieldName == null || fieldName.isEmpty()) {
            fieldName = DEFAULT_FIELD_NAME;
        }
    }

    @Override
    public LogItem serialize(Event event) {
        LogItem item = new LogItem();
        try {
            item.PushBack(fieldName, new String(event.getBody(), encoding));
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("Failed to decode event with encoding: " + encoding, e);
        }
        return item;
    }
}
