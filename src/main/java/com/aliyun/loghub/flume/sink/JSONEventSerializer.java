package com.aliyun.loghub.flume.sink;

import com.alibaba.fastjson.JSONObject;
import com.aliyun.openservices.log.common.LogContent;
import com.aliyun.openservices.log.common.LogItem;
import org.apache.flume.Context;
import org.apache.flume.Event;

import java.io.UnsupportedEncodingException;

public class JSONEventSerializer implements EventSerializer {

    static final String ALIAS = "JSON";

    private String encoding;

    @Override
    public void configure(Context context) {
        encoding = context.getString("encoding", "UTF-8");
    }

    @Override
    public LogItem serialize(Event event) {
        try {
            String body = new String(event.getBody(), encoding);
            LogItem item = new LogItem();
            JSONObject object = JSONObject.parseObject(body);
            object.forEach((key, value) -> item.PushBack(key, value == null ? "null" : value.toString()));
            return item;
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("Failed to decode event with encoding: " + encoding, e);
        }
    }
}
