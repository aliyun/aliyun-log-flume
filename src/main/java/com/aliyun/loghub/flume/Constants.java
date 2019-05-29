package com.aliyun.loghub.flume;


public class Constants {

    public static final String CONSUMER_GROUP_KEY = "consumerGroup";
    public static final String ENDPOINT_KEY = "endpoint";
    public static final String PROJECT_KEY = "project";
    public static final String LOGSTORE_KEY = "logstore";
    public static final String ACCESS_KEY_ID_KEY = "accessKeyId";
    public static final String ACCESS_KEY_SECRET_KEY = "accessKey";
    public static final String CONSUME_INITIAL_POSITION = "initialPosition";
    public static final String CONSUME_POSITION_BEGIN = "begin";
    public static final String CONSUME_POSITION_END = "end";
    public static final String CONSUME_POSITION_TIMESTAMP = "timestamp";
    /**
     * Consumer group heartbeat interval in millisecond.
     */
    public static final String HEARTBEAT_INTERVAL_MS = "heartbeatIntervalMs";
    /**
     * Fetch data interval in millisecond.
     */
    public static final String FETCH_INTERVAL_MS = "fetchIntervalMs";

    public static final String USE_RECORD_TIME = "useRecordTime";
    public static final long DEFAULT_HEARTBEAT_INTERVAL_MS = 30000L;
    public static final long DEFAULT_FETCH_INTERVAL_MS = 100L;
    public static final String FETCH_IN_ORDER = "fetchInOrder";
    public static final boolean DEFAULT_FETCH_IN_ORDER = false;
    public static final String BATCH_SIZE = "batchSize";
    public static final int DEFAULT_BATCH_SIZE = 1000;

    public static final String MAX_BUFFER_SIZE = "maxBufferSize";

    public static final String SERIALIZER = "serializer";
    public static final String DESERIALIZER = "deserializer";
    public static final String COLUMNS = "columns";
    public static final String SEPARATOR_CHAR = "separatorChar";
    public static final String QUOTE_CHAR = "quoteChar";
    public static final String ESCAPE_CHAR = "escapeChar";
    public static final String LINE_END = "lineEnd";
    public static final String APPEND_TIMESTAMP = "appendTimestamp";
    public static final String TIME_AS_FIELD = "timeAsField";
    public static final String TAG_AS_FIELD = "tagAsField";
    public static final String SOURCE_AS_FIELD = "sourceAsField";

    public static final String AUTO_DETECT_JSON_FIELDS = "autoDetectJSONFields";
    public static final String TIMESTAMP = "timestamp";
    public static final String RECORD_TIME_KEY = "__time__";
    public static final String RECORD_TAG_PREFIX = "__tag__:";
    public static final String RECORD_SOURCE_KEY = "__source__";
}
