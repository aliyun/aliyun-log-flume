package com.aliyun.loghub.flume.source;

import com.aliyun.openservices.log.common.FastLog;
import com.aliyun.openservices.log.common.FastLogContent;
import com.aliyun.openservices.log.common.FastLogGroup;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.aliyun.loghub.flume.Constants.COLUMNS_KEY;
import static com.aliyun.loghub.flume.Constants.NULL_AS_KEY;
import static com.aliyun.loghub.flume.Constants.QUOTE_KEY;
import static com.aliyun.loghub.flume.Constants.SEPARATOR_KEY;
import static com.aliyun.loghub.flume.Constants.TIMESTAMP_HEADER;


public class CSVEventSerializer implements EventSerializer {
    private static final Logger LOG = LoggerFactory.getLogger(CSVEventSerializer.class);

    private static final String DEFAULT_SEPARATOR = ",";
    private static final char DEFAULT_QUOTE = '"';

    private String separator;
    private char quote;
    private Map<String, Integer> fieldIndexMapping;
    private int width;
    private final boolean useRecordTime;
    private String valueForNull = null;

    CSVEventSerializer(boolean useRecordTime) {
        this.useRecordTime = useRecordTime;
    }

    @Override
    public List<Event> serialize(FastLogGroup logGroup) {
        int count = logGroup.getLogsCount();
        String[] record = new String[width];
        List<Event> events = new ArrayList<>(count);
        StringBuilder builder = new StringBuilder();

        for (int idx = 0; idx < count; ++idx) {
            FastLog log = logGroup.getLogs(idx);
            for (int i = 0; i < log.getContentsCount(); i++) {
                FastLogContent content = log.getContents(i);
                final String key = content.getKey();
                Integer index = fieldIndexMapping.get(key);
                if (index != null) {
                    // otherwise ignore this field
                    record[index] = content.getValue();
                }
            }
            builder.setLength(0);
            for (int i = 0; i < width; i++) {
                String text = record[i];
                if (text == null) {
                    if (valueForNull != null) {
                        builder.append(valueForNull);
                    }
                } else {
                    boolean needQuote = text.contains(separator);
                    if (needQuote) {
                        builder.append(quote);
                    }
                    builder.append(text);
                    if (needQuote) {
                        builder.append(quote);
                    }
                    record[i] = null;
                }
                if (i < width - 1) {
                    builder.append(separator);
                }
            }
            Event event = EventBuilder.withBody(builder.toString().getBytes(charset));
            int recordTime = log.getTime();
            long timestamp;
            if (useRecordTime) {
                timestamp = ((long) recordTime) * 1000;
            } else {
                timestamp = System.currentTimeMillis();
            }
            event.setHeaders(Collections.singletonMap(TIMESTAMP_HEADER, String.valueOf(timestamp)));
            events.add(event);
        }
        return events;
    }

    @Override
    public void configure(Context context) {
        String columns = context.getString(COLUMNS_KEY);
        if (StringUtils.isBlank(columns)) {
            throw new IllegalArgumentException("Missing parameters: " + COLUMNS_KEY);
        }
        separator = context.getString(SEPARATOR_KEY);
        if (StringUtils.isBlank(separator)) {
            separator = DEFAULT_SEPARATOR;
        } else {
            separator = separator.trim();
        }
        String quoteText = context.getString(QUOTE_KEY);
        if (StringUtils.isBlank(quoteText)) {
            quote = DEFAULT_QUOTE;
        } else {
            if (quoteText.length() > 1) {
                LOG.warn("Quote expect a char but was: {}. " +
                        "The first character {} will be used as quote.", quoteText, quoteText.charAt(0));
            }
            quote = quoteText.charAt(0);
        }
        String[] fields = columns.split(separator);
        width = fields.length;
        fieldIndexMapping = new HashMap<>(width);
        for (int i = 0; i < width; i++) {
            fieldIndexMapping.put(fields[i], i);
        }
        valueForNull = context.getString(NULL_AS_KEY);
    }
}
