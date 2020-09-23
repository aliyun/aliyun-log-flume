package com.aliyun.loghub.flume.sink;

import com.alibaba.fastjson.JSONObject;
import com.aliyun.openservices.log.common.LogItem;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.aliyun.loghub.flume.Constants.TIME_FIELD;
import static com.aliyun.loghub.flume.Constants.TIME_FORMAT;

public class RegexEventSerializer implements EventSerializer {
    private static final Logger LOG = LoggerFactory.getLogger(RegexEventSerializer.class);

    static final String ALIAS = "REGEX";
    private List<String> fieldNames;
    private String encoding;
    private Pattern pattern;
    private List<String> expandJsonFields;
    private String timeField;
    private DateTimeFormatter formatter;
    private boolean isEpoch = false;

    private List<String> trimAll(String[] keys) {
        List<String> fields = new ArrayList<>(keys.length);
        for (String k : keys) {
            fields.add(k.trim());
        }
        return fields;
    }

    @Override
    public void configure(Context context) {
        LOG.info("Start sink with config: {}", context.toString());
        encoding = context.getString("encoding", "UTF-8");
        String regex = context.getString("regex");
        if (regex == null || regex.isEmpty()) {
            throw new IllegalArgumentException("regex is missing");
        }
        String fieldNames = context.getString("fieldNames");
        if (fieldNames == null || fieldNames.isEmpty()) {
            throw new IllegalArgumentException("fieldNames is missing");
        }
        this.fieldNames = trimAll(fieldNames.split(",", -1));
        pattern = Pattern.compile(regex);
        String expandJsonKeys = context.getString("expandJsonKeys");
        if (expandJsonKeys != null && !expandJsonKeys.isEmpty()) {
            this.expandJsonFields = trimAll(expandJsonKeys.split(",", -1));
        } else {
            this.expandJsonFields = Collections.emptyList();
        }
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

    public LocalDateTime parse(String dateNow) {
        return LocalDateTime.parse(dateNow, formatter);
    }

    private boolean isTimeField(String key) {
        return timeField != null && !timeField.isEmpty() && timeField.equals(key);
    }

    private int parseLogTime(String timestr) {
        try {
            if (isEpoch) {
                return Integer.parseInt(timestr);
            } else {
                return (int) (parse(timestr).toEpochSecond(ZoneOffset.of("+8")));
            }
        } catch (Exception ex) {
            // ignore
        }
        return -1;
    }

    private void pushField(LogItem item, String k, String v) {
        item.PushBack(k, v);
        if (isTimeField(k)) {
            int ts = parseLogTime(v);
            if (ts > 0) {
                item.SetTime(ts);
            }
        }
    }

    @Override
    public LogItem serialize(Event event) {
        LogItem record = new LogItem();
        try {
            String text = new String(event.getBody(), encoding);
            Matcher matcher = pattern.matcher(text);
            if (!matcher.matches()) {
                LOG.warn("Regex not match - {}", text);
                record.PushBack("content", text);
                return record;
            }
            for (int i = 1; i <= matcher.groupCount(); i++) {
                if (i >= fieldNames.size()) {
                    break;
                }
                String value = matcher.group(i);
                String key = fieldNames.get(i - 1);
                if (value == null) {
                    record.PushBack(key, "null");
                    continue;
                }
                if (expandJsonFields.contains(key)) {
                    // expand first
                    String jsonStr = value.trim();
                    try {
                        JSONObject object = JSONObject.parseObject(jsonStr);
                        object.forEach((k, v) -> {
                            if (v == null) {
                                record.PushBack(k, "null");
                            } else {
                                pushField(record, k, v.toString());
                            }
                        });
                        continue;
                    } catch (Exception ex) {
                        LOG.error("Cannot parse JSON: " + value);
                    }
                }
                pushField(record, key, value);
            }
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("Failed to decode event with encoding: " + encoding, e);
        }
        return record;
    }
}
