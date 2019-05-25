package com.aliyun.loghub.flume.sink;

import com.aliyun.openservices.log.common.LogItem;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.CSVWriter;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

import static com.aliyun.loghub.flume.Constants.COLUMNS;
import static com.aliyun.loghub.flume.Constants.ESCAPE_CHAR;
import static com.aliyun.loghub.flume.Constants.QUOTE_CHAR;
import static com.aliyun.loghub.flume.Constants.SEPARATOR_CHAR;
import static com.aliyun.loghub.flume.Constants.TIMESTAMP;
import static com.aliyun.loghub.flume.Constants.USE_RECORD_TIME;


public class DelimitedTextEventSerializer implements EventSerializer {
    private static final Logger LOG = LoggerFactory.getLogger(DelimitedTextEventSerializer.class);

    static final String ALIAS = "DELIMITED";

    private String[] fieldNames;
    private CSVParser csvParser;
    private boolean useRecordTime;

    @Override
    public LogItem serialize(Event event) {
        try (InputStreamReader in = new InputStreamReader(
                new ByteArrayInputStream(event.getBody()),
                StandardCharsets.UTF_8)) {
            CSVReader reader = new CSVReaderBuilder(in).withCSVParser(csvParser).build();
            String[] record = reader.readNext();
            LogItem item = new LogItem();
            int numberOfCol = Math.min(record.length, fieldNames.length);
            for (int i = 0; i < numberOfCol; i++) {
                if (useRecordTime && TIMESTAMP.equals(fieldNames[i])) {
                    try {
                        item.SetTime(Integer.parseInt(record[i]));
                    } catch (NumberFormatException nfe) {
                        LOG.warn("Failed to parse record time", nfe);
                    }
                }
                item.PushBack(fieldNames[i], record[i]);
            }
            return item;
        } catch (IOException ex) {
            throw new FlumeException("Failed to parsing delimited text", ex);
        }
    }

    private static char getChar(Context context, String key, char defaultValue) {
        String value = context.getString(key);
        if (value == null) {
            return defaultValue;
        }
        value = value.trim();
        if (value.length() != 1) {
            throw new IllegalArgumentException(key + " is invalid for DELIMITED serializer: " + value);
        }
        return value.charAt(0);
    }

    @Override
    public void configure(Context context) {
        String columns = context.getString(COLUMNS);
        if (StringUtils.isBlank(columns)) {
            throw new IllegalArgumentException("Missing parameter: " + COLUMNS);
        }
        char separatorChar = getChar(context, SEPARATOR_CHAR, CSVWriter.DEFAULT_SEPARATOR);
        char quoteChar = getChar(context, QUOTE_CHAR, CSVWriter.DEFAULT_QUOTE_CHARACTER);
        char escapeChar = getChar(context, ESCAPE_CHAR, CSVWriter.DEFAULT_ESCAPE_CHARACTER);
        LOG.info("separatorChar=[" + separatorChar + "] quoteChar=[" + quoteChar + "] escapeChar=[" + escapeChar + "]");
        fieldNames = columns.split(",", -1);
        csvParser = new CSVParserBuilder().withEscapeChar(escapeChar)
                .withSeparator(separatorChar)
                .withQuoteChar(quoteChar)
                .build();
        useRecordTime = context.getBoolean(USE_RECORD_TIME, false);
    }
}
