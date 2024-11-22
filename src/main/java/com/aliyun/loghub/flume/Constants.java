package com.aliyun.loghub.flume;


import com.aliyun.loghub.flume.utils.VersionInfoUtils;

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
    public static final String QUERY = "query";
    public static final String CONSUME_POSITION_TIMESTAMP = "timestamp";
    public static final String LOG_USER_AGENT = "userAgent";
    public static final String LOG_CONNECTOR_USER_AGENT = VersionInfoUtils.getDefaultUserAgent();

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
    public static final String BUFFER_BYTES = "bufferBytes";
    public static final long DEFAULT_BUFFER_BYTES = 2 * 1024 * 1024; // 2MB
    public static final long MAX_LOG_GROUP_BYTES = 10 * 1024 * 1024; // 10MB
    public static final String MAX_BUFFER_TIME = "maxBufferTime";
    public static final String MAX_RETRY = "maxRetry";
    public static final int DEFAULT_MAX_RETRY = 16;

    public static final String SERIALIZER = "serializer";
    public static final String DESERIALIZER = "deserializer";
    public static final String COLUMNS = "columns";
    public static final String SEPARATOR_CHAR = "separatorChar";
    public static final String APPLY_QUOTES_TO_ALL = "applyQuotesToAll";
    public static final String QUOTE_CHAR = "quoteChar";
    public static final String ESCAPE_CHAR = "escapeChar";
    public static final String LINE_END = "lineEnd";
    public static final String APPEND_TIMESTAMP = "appendTimestamp";
    public static final String TIME_AS_FIELD = "timeAsField";
    public static final String TOPIC_AS_FIELD = "topicAsField";
    public static final String TAG_AS_FIELD = "tagAsField";
    public static final String SOURCE_AS_FIELD = "sourceAsField";

    public static final String TIME_FIELD = "timeField";
    public static final String TIME_FORMAT = "timeFormat";

    public static final String AUTO_DETECT_JSON_FIELDS = "autoDetectJSONFields";
    public static final String TIMESTAMP = "timestamp";
    public static final String RECORD_TIME_KEY = "__time__";
    public static final String RECORD_TAG_PREFIX = "__tag__:";
    public static final String RECORD_SOURCE_KEY = "__source__";
    public static final String RECORD_TOPIC_KEY = "__topic__";

    public static final String APPEND_LOCAL_TIME = "appendLocalTime";
    public static final String LOCAL_TIME_FIELD_NAME = "localTimeFieldName";
}
