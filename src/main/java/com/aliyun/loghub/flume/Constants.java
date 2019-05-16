package com.aliyun.loghub.flume;


public class Constants {

    public static final String CONSUMER_GROUP_KEY = "consumerGroup";
    public static final String ENDPOINT_KEY = "endpoint";
    public static final String PROJECT_KEY = "project";
    public static final String LOGSTORE_KEY = "logstore";
    public static final String ACCESS_KEY_ID_KEY = "accessKeyId";
    public static final String ACCESS_KEY_SECRET_KEY = "accessKey";
    public static final String CONSUME_POSITION_KEY = "consumerPosition";
    public static final String CONSUME_POSITION_BEGIN = "begin";
    public static final String CONSUME_POSITION_END = "end";
    public static final String CONSUME_POSITION_TIMESTAMP = "timestamp";
    public static final String CONSUME_POSITION_START_TIME_KEY = "startTime";
    /**
     * Consumer group heartbeat interval in millisecond.
     */
    public static final String HEARTBEAT_INTERVAL_MS = "heartbeatIntervalMs";
    /**
     * Fetch data interval in millisecond.
     */
    public static final String FETCH_INTERVAL_MS = "fetchIntervalMs";

    public static final String USER_RECORD_TIME_KEY = "useRecordTime";
    public static final boolean DEFAULT_USER_RECORD_TIME = false;
    public static final long DEFAULT_HEARTBEAT_INTERVAL_MS = 30000L;
    public static final long DEFAULT_FETCH_INTERVAL_MS = 100L;
    public static final String FETCH_IN_ORDER_KEY = "fetchInOrder";
    public static final boolean DEFAULT_FETCH_IN_ORDER = false;
    public static final String BATCH_SIZE_KEY = "batchSize";
    public static final int DEFAULT_BATCH_SIZE = 1000;

    public static final String MAX_BUFFER_SIZE = "maxBufferSize";

    public static final String FORMAT_KEY = "format";
    public static final String CSV_FORMAT = "csv";
    public static final String JSON_FORMAT = "json";
    public static final String STRING_FORMAT = "string";

    public static final String DEFAULT_SOURCE_FORMAT = "csv";
    public static final String DEFAULT_SINK_FORMAT = "string";

    public static final String COLUMNS_KEY = "columns";
    public static final String SEPARATOR_KEY = "separator";
    public static final String QUOTE_KEY = "quote";
    public static final String NULL_AS_KEY = "nullAs";

    public static final String TIMESTAMP_HEADER = "timestamp";
    public static final String RECORD_TIME_KEY = "__time__";
    public static final String RECORD_TAG_PREFIX = "__tag__:";
}
