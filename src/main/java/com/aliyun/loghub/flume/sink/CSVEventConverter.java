package com.aliyun.loghub.flume.sink;

import com.aliyun.openservices.log.common.LogItem;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static com.aliyun.loghub.flume.LoghubConstants.COLUMNS_KEY;
import static com.aliyun.loghub.flume.LoghubConstants.QUOTE_KEY;
import static com.aliyun.loghub.flume.LoghubConstants.SEPARATOR_KEY;


public class CSVEventConverter implements Converter<Event, LogItem> {
    private static final Logger LOG = LoggerFactory.getLogger(CSVEventConverter.class);

    private List<String> fieldNames;
    private String separator;
    private char quote;
    private static final String DEFAULT_SEPARATOR = ",";
    private static final char DEFAULT_QUOTE = '"';

    @Override
    public LogItem convert(Event event) {
        String eventBody = new String(event.getBody(), StandardCharsets.UTF_8);
        char[] characters = eventBody.toCharArray();
        int index = 0;
        boolean inQuote = false;
        int n = characters.length;
        int offset = 0, sepSize = separator.length();

        LogItem item = new LogItem();
        for (int i = 0; i < n; ) {
            if (characters[i] == quote) {
                if (inQuote) {
                    String value = eventBody.substring(offset, i);
                    item.PushBack(fieldNames.get(index++), value);
                    inQuote = false;
                } else {
                    inQuote = true;
                }
                offset = i + 1;
                i++;
                continue;
            }
            if (!inQuote &&
                    eventBody.substring(i, i + sepSize).equals(separator)) {
                // separator found
                String value = eventBody.substring(offset, i);
                item.PushBack(fieldNames.get(index++), value);
                offset += sepSize;
                i += sepSize;
                continue;
            }
            i++;
        }
        if (offset < n) {
            String value = eventBody.substring(offset, n);
            item.PushBack(fieldNames.get(index), value);
        }
        return item;
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
        fieldNames = new ArrayList<>(fields.length);
        for (String field : fields) {
            fieldNames.add(field.trim());
        }
    }
}
