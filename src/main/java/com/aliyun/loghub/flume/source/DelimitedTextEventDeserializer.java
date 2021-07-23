package com.aliyun.loghub.flume.source;

import com.aliyun.openservices.log.common.FastLog;
import com.aliyun.openservices.log.common.FastLogContent;
import com.aliyun.openservices.log.common.FastLogGroup;
import com.opencsv.CSVWriter;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.event.EventBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.aliyun.loghub.flume.Constants.APPEND_LOCAL_TIME;
import static com.aliyun.loghub.flume.Constants.APPEND_TIMESTAMP;
import static com.aliyun.loghub.flume.Constants.APPLY_QUOTES_TO_ALL;
import static com.aliyun.loghub.flume.Constants.COLUMNS;
import static com.aliyun.loghub.flume.Constants.ESCAPE_CHAR;
import static com.aliyun.loghub.flume.Constants.LINE_END;
import static com.aliyun.loghub.flume.Constants.LOCAL_TIME_FIELD_NAME;
import static com.aliyun.loghub.flume.Constants.QUOTE_CHAR;
import static com.aliyun.loghub.flume.Constants.SEPARATOR_CHAR;
import static com.aliyun.loghub.flume.Constants.TIMESTAMP;
import static com.aliyun.loghub.flume.Constants.USE_RECORD_TIME;


public class DelimitedTextEventDeserializer implements EventDeserializer {
    private static final Logger LOG = LoggerFactory.getLogger(DelimitedTextEventDeserializer.class);

    static final String ALIAS = "DELIMITED";

    private Map<String, Integer> fieldIndexMapping;
    private boolean useRecordTime;
    private boolean appendTimestamp;
    private boolean applyQuotesToAll;
    private char separatorChar;
    private char quoteChar;
    private char escapeChar;
    private String lineEnd;

    private static final String DEFAULT_LINE_END = "";

    private boolean appendLocalTime;
    private int localTimeIndex = 0;

    @Override
    public List<Event> deserialize(FastLogGroup logGroup) {
        int count = logGroup.getLogsCount();
        int width = fieldIndexMapping.size();
        if (appendTimestamp) {
            width++;
        }
        String[] record = new String[width];
        List<Event> events = new ArrayList<>(count);
        final StringWriter writer = new StringWriter();
        CSVWriter csvWriter = new CSVWriter(writer, separatorChar, quoteChar, escapeChar, lineEnd);

        for (int idx = 0; idx < count; ++idx) {
            FastLog log = logGroup.getLogs(idx);
            for (int i = 0; i < log.getContentsCount(); i++) {
                FastLogContent content = log.getContents(i);
                final String key = content.getKey();
                Integer index = fieldIndexMapping.get(key);
                if (index != null) {
                    // otherwise ignore this field
                    String value = content.getValue();
                    if (value != null && value.contains("\n")) {
                        value = value.replace('\n', ' ');
                    }
                    record[index] = value;
                }
            }
            int recordTime = log.getTime();
            String localTime = String.valueOf(System.currentTimeMillis());
            String timestamp;
            if (useRecordTime) {
                timestamp = String.valueOf(((long) recordTime) * 1000);
            } else {
                timestamp = localTime;
            }
            if (appendTimestamp) {
                record[width - 1] = timestamp;
            }
            if (appendLocalTime) {
                record[localTimeIndex] = localTime;
            }
            csvWriter.writeNext(record, applyQuotesToAll);
            try {
                csvWriter.flush();
            } catch (IOException ex) {
                throw new FlumeException("Failed to flush writer", ex);
            }
            Event event = EventBuilder.withBody(writer.toString(), charset,
                    Collections.singletonMap(TIMESTAMP, timestamp));
            events.add(event);
            for (int i = 0; i < width; i++) {
                record[i] = null;
            }
            writer.getBuffer().setLength(0);
        }
        return events;
    }

    private static char getChar(Context context, String key, char defaultValue) {
        String value = context.getString(key);
        if (value == null) {
            return defaultValue;
        }
        value = value.trim();
        if (value.length() != 1) {
            throw new IllegalArgumentException(key + " is invalid for CSV serializer: " + value);
        }
        return value.charAt(0);
    }

    @Override
    public void configure(Context context) {
        useRecordTime = context.getBoolean(USE_RECORD_TIME, false);
        String columns = context.getString(COLUMNS);
        if (StringUtils.isBlank(columns)) {
            throw new IllegalArgumentException("Missing parameters: " + COLUMNS);
        }
        applyQuotesToAll = context.getBoolean(APPLY_QUOTES_TO_ALL, false);
        separatorChar = getChar(context, SEPARATOR_CHAR, CSVWriter.DEFAULT_SEPARATOR);
        quoteChar = getChar(context, QUOTE_CHAR, CSVWriter.DEFAULT_QUOTE_CHARACTER);
        escapeChar = getChar(context, ESCAPE_CHAR, CSVWriter.DEFAULT_ESCAPE_CHARACTER);
        if (LOG.isDebugEnabled()) {
            LOG.debug("separatorChar=[" + separatorChar + "] quoteChar=[" + quoteChar + "] escapeChar=[" + escapeChar + "]");
        }
        lineEnd = context.getString(LINE_END, DEFAULT_LINE_END);
        appendTimestamp = context.getBoolean(APPEND_TIMESTAMP, false);
        String[] fields = columns.split(",", -1);
        int width = fields.length;
        fieldIndexMapping = new HashMap<>(width);
        for (int i = 0; i < width; i++) {
            fieldIndexMapping.put(fields[i], i);
        }
        appendLocalTime = context.getBoolean(APPEND_LOCAL_TIME, false);
        if (appendLocalTime) {
            String localTimeFieldName = context.getString(LOCAL_TIME_FIELD_NAME);
            if (StringUtils.isBlank(localTimeFieldName)) {
                throw new IllegalArgumentException("Missing parameter: " + LOCAL_TIME_FIELD_NAME);
            }
            if (!fieldIndexMapping.containsKey(localTimeFieldName)) {
                throw new IllegalArgumentException("Field '" + localTimeFieldName + "' not exist in columns");
            }
            localTimeIndex = fieldIndexMapping.get(localTimeFieldName);
        }
    }
}
