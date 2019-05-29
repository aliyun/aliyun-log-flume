package com.aliyun.loghub.flume.source;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import com.aliyun.openservices.log.common.FastLog;
import com.aliyun.openservices.log.common.FastLogContent;
import com.aliyun.openservices.log.common.FastLogGroup;
import com.aliyun.openservices.log.common.FastLogTag;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.aliyun.loghub.flume.Constants.AUTO_DETECT_JSON_FIELDS;
import static com.aliyun.loghub.flume.Constants.RECORD_SOURCE_KEY;
import static com.aliyun.loghub.flume.Constants.RECORD_TAG_PREFIX;
import static com.aliyun.loghub.flume.Constants.RECORD_TIME_KEY;
import static com.aliyun.loghub.flume.Constants.SOURCE_AS_FIELD;
import static com.aliyun.loghub.flume.Constants.TAG_AS_FIELD;
import static com.aliyun.loghub.flume.Constants.TIMESTAMP;
import static com.aliyun.loghub.flume.Constants.TIME_AS_FIELD;
import static com.aliyun.loghub.flume.Constants.USE_RECORD_TIME;


public class JSONEventDeserializer implements EventDeserializer {
    static final String ALIAS = "JSON";

    private boolean useRecordTime;
    private boolean sourceAsField;
    private boolean tagAsField;
    private boolean timeAsField;
    private boolean autoDetectJSONFields;


    private static boolean mayBeJSON(String string) {
//        if (string == null) {
//            return false;
//        } else if ("null".equals(string)) {
//            return true;
//        }
//        int n = string.length();
//        int left = 0;
//        while (left < n && Character.isWhitespace(string.charAt(left)))
//            left++;
//        if (left >= n)
//            return false;
//        char lch = string.charAt(left);
//        if (lch != '{' && lch != '[')
//            return false;
//        int right = n - 1;
//        while (right >= 0 && Character.isWhitespace(string.charAt(right)))
//            right--;
//        if (right < 0)
//            return false;
//        char rch = string.charAt(right);
//        return (lch == '[' && rch == ']') || (lch == '{' && rch == '}');
        return string != null
                && ("null".equals(string)
                || (string.startsWith("[") && string.endsWith("]")) || (string.startsWith("{") && string.endsWith("}")));
    }

    @Override
    public List<Event> deserialize(FastLogGroup logGroup) {
        int count = logGroup.getLogsCount();
        List<Event> events = new ArrayList<>(count);
        for (int idx = 0; idx < count; ++idx) {
            FastLog log = logGroup.getLogs(idx);
            int fieldCount = log.getContentsCount();
            JSONObject record = new JSONObject(fieldCount);
            for (int i = 0; i < fieldCount; i++) {
                FastLogContent content = log.getContents(i);
                final String value = content.getValue();
                final String key = content.getKey();
                if (autoDetectJSONFields && mayBeJSON(value)) {
                    try {
                        JSONObject jsonObject = JSON.parseObject(value);
                        record.put(key, jsonObject);
                    } catch (JSONException jex) {
                        record.put(key, value);
                    }
                } else {
                    record.put(key, value);
                }
            }
            if (timeAsField) {
                record.put(RECORD_TIME_KEY, String.valueOf(log.getTime()));
            }
            if (tagAsField) {
                int tagCount = logGroup.getLogTagsCount();
                for (int i = 0; i < tagCount; i++) {
                    FastLogTag tag = logGroup.getLogTags(i);
                    record.put(RECORD_TAG_PREFIX + tag.getKey(), tag.getValue());
                }
            }
            if (sourceAsField) {
                record.put(RECORD_SOURCE_KEY, logGroup.getSource());
            }
            int recordTime = log.getTime();
            long timestamp;
            if (useRecordTime) {
                timestamp = ((long) recordTime) * 1000;
            } else {
                timestamp = System.currentTimeMillis();
            }
            Event event = EventBuilder.withBody(record.toJSONString(), charset,
                    Collections.singletonMap(TIMESTAMP, String.valueOf(timestamp)));
            events.add(event);
        }
        return events;
    }

    @Override
    public void configure(Context context) {
        useRecordTime = context.getBoolean(USE_RECORD_TIME, false);
        sourceAsField = context.getBoolean(SOURCE_AS_FIELD, false);
        tagAsField = context.getBoolean(TAG_AS_FIELD, false);
        timeAsField = context.getBoolean(TIME_AS_FIELD, false);
        autoDetectJSONFields = context.getBoolean(AUTO_DETECT_JSON_FIELDS, true);
    }
}
