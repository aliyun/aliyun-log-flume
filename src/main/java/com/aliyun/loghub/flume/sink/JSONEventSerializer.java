package com.aliyun.loghub.flume.sink;

import com.alibaba.fastjson.JSONObject;
import com.aliyun.openservices.log.common.LogItem;
import org.apache.flume.Context;
import org.apache.flume.Event;

import java.io.UnsupportedEncodingException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

import static com.aliyun.loghub.flume.Constants.TIME_FIELD;
import static com.aliyun.loghub.flume.Constants.TIME_FORMAT;

public class JSONEventSerializer implements EventSerializer {

    static final String ALIAS = "JSON";

    private String encoding;
    private String timeField;
    private DateTimeFormatter formatter;
    private boolean isEpoch = false;

    public LocalDateTime parse(String dateNow) {
        return LocalDateTime.parse(dateNow, formatter);
    }

    @Override
    public void configure(Context context) {
        encoding = context.getString("encoding", "UTF-8");
        timeField = context.getString(TIME_FIELD);
        String timeFormat = context.getString(TIME_FORMAT);
        if (timeFormat != null && !timeFormat.isEmpty()) {
            if (timeFormat.equalsIgnoreCase("epoch")) {
                isEpoch = true;
            } else {
                formatter = DateTimeFormatter.ofPattern(timeFormat);
            }
        }
    }

    @Override
    public LogItem serialize(Event event) {
        try {
            String body = new String(event.getBody(), encoding);
            LogItem item = new LogItem();
            JSONObject object = JSONObject.parseObject(body);
            object.forEach((key, value) -> {
                item.PushBack(key, value == null ? "null" : value.toString());
                if (timeField != null && !timeField.isEmpty() && timeField.equals(key)
                        && value != null) {
                    try {
                        String timestr = value.toString();
                        if (isEpoch) {
                            item.SetTime(Integer.parseInt(timestr));
                        } else {
                            item.SetTime((int) (parse(timestr).toEpochSecond(ZoneOffset.UTC)));
                        }
                    } catch (Exception ex) {
                        // ignore
                    }
                }
            });
            return item;
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("Failed to decode event with encoding: " + encoding, e);
        }
    }
}
