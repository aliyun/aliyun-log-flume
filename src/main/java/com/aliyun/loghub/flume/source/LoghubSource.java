package com.aliyun.loghub.flume.source;

import com.aliyun.loghub.flume.Validate;
import com.aliyun.openservices.loghub.client.ClientWorker;
import com.aliyun.openservices.loghub.client.config.LogHubConfig;
import com.aliyun.openservices.loghub.client.config.LogHubConfig.ConsumePosition;
import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.FlumeException;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;

import static com.aliyun.loghub.flume.Constants.ACCESS_KEY_ID_KEY;
import static com.aliyun.loghub.flume.Constants.ACCESS_KEY_SECRET_KEY;
import static com.aliyun.loghub.flume.Constants.BATCH_SIZE;
import static com.aliyun.loghub.flume.Constants.CONSUMER_GROUP_KEY;
import static com.aliyun.loghub.flume.Constants.CONSUME_INITIAL_POSITION;
import static com.aliyun.loghub.flume.Constants.CONSUME_POSITION_BEGIN;
import static com.aliyun.loghub.flume.Constants.CONSUME_POSITION_END;
import static com.aliyun.loghub.flume.Constants.CONSUME_POSITION_TIMESTAMP;
import static com.aliyun.loghub.flume.Constants.DEFAULT_BATCH_SIZE;
import static com.aliyun.loghub.flume.Constants.DEFAULT_FETCH_INTERVAL_MS;
import static com.aliyun.loghub.flume.Constants.DEFAULT_FETCH_IN_ORDER;
import static com.aliyun.loghub.flume.Constants.DEFAULT_HEARTBEAT_INTERVAL_MS;
import static com.aliyun.loghub.flume.Constants.DEFAULT_MAX_RETRY;
import static com.aliyun.loghub.flume.Constants.DESERIALIZER;
import static com.aliyun.loghub.flume.Constants.ENDPOINT_KEY;
import static com.aliyun.loghub.flume.Constants.FETCH_INTERVAL_MS;
import static com.aliyun.loghub.flume.Constants.FETCH_IN_ORDER;
import static com.aliyun.loghub.flume.Constants.HEARTBEAT_INTERVAL_MS;
import static com.aliyun.loghub.flume.Constants.LOGSTORE_KEY;
import static com.aliyun.loghub.flume.Constants.LOG_CONNECTOR_USER_AGENT;
import static com.aliyun.loghub.flume.Constants.LOG_USER_AGENT;
import static com.aliyun.loghub.flume.Constants.MAX_RETRY;
import static com.aliyun.loghub.flume.Constants.PROJECT_KEY;
import static com.aliyun.loghub.flume.Constants.QUERY;
import static com.google.common.base.Preconditions.checkArgument;


public class LoghubSource extends AbstractSource implements
        EventDrivenSource, Configurable {
    private static final Logger LOG = LoggerFactory.getLogger(LoghubSource.class);

    private LogHubConfig config;
    private ClientWorker worker;
    private SourceCounter counter;
    private EventDeserializer deserializer;
    private int maxRetry;

    @Override
    public void configure(Context context) {
        config = parseConsumerConfig(context);
        deserializer = createDeserializer(context);
        maxRetry = context.getInteger(MAX_RETRY, DEFAULT_MAX_RETRY);
    }

    private static LogHubConfig parseConsumerConfig(Context context) {
        String endpoint = context.getString(ENDPOINT_KEY);
        Validate.notEmpty(endpoint, ENDPOINT_KEY);
        String project = context.getString(PROJECT_KEY);
        Validate.notEmpty(project, PROJECT_KEY);
        String logstore = context.getString(LOGSTORE_KEY);
        Validate.notEmpty(logstore, LOGSTORE_KEY);
        String accessKeyId = context.getString(ACCESS_KEY_ID_KEY);
        Validate.notEmpty(accessKeyId, ACCESS_KEY_ID_KEY);
        String accessKey = context.getString(ACCESS_KEY_SECRET_KEY);
        Validate.notEmpty(accessKey, ACCESS_KEY_SECRET_KEY);
        String consumerGroup = context.getString(CONSUMER_GROUP_KEY);
        if (StringUtils.isBlank(consumerGroup)) {
            LOG.info("Loghub Consumer Group is not specified, will generate a random Consumer Group name.");
            consumerGroup = createConsumerGroupName();
        }
        String consumerId = UUID.randomUUID().toString();
        LOG.info("Using consumer group {}, consumer  {}", consumerGroup, consumerId);

        long heartbeatIntervalMs = context.getLong(HEARTBEAT_INTERVAL_MS, DEFAULT_HEARTBEAT_INTERVAL_MS);
        long fetchIntervalMs = context.getLong(FETCH_INTERVAL_MS, DEFAULT_FETCH_INTERVAL_MS);
        boolean fetchInOrder = context.getBoolean(FETCH_IN_ORDER, DEFAULT_FETCH_IN_ORDER);
        int batchSize = context.getInteger(BATCH_SIZE, DEFAULT_BATCH_SIZE);

        String position = context.getString(CONSUME_INITIAL_POSITION, CONSUME_POSITION_BEGIN);
        LogHubConfig config;
        switch (position) {
            case CONSUME_POSITION_TIMESTAMP:
                Integer startTime = context.getInteger(CONSUME_POSITION_TIMESTAMP);
                checkArgument(startTime != null, "Missing parameter: " + CONSUME_POSITION_TIMESTAMP);
                checkArgument(startTime > 0, "timestamp must be > 0");
                config = new LogHubConfig(consumerGroup, consumerId, endpoint, project, logstore, accessKeyId, accessKey,
                        startTime, batchSize);
                break;
            case CONSUME_POSITION_END:
                config = new LogHubConfig(consumerGroup, consumerId, endpoint, project, logstore, accessKeyId, accessKey,
                        ConsumePosition.END_CURSOR, batchSize);
                break;
            default:
                // Start from the earliest position by default
                config = new LogHubConfig(consumerGroup, consumerId, endpoint, project, logstore, accessKeyId, accessKey,
                        ConsumePosition.BEGIN_CURSOR, batchSize);
                break;
        }
        String query = context.getString(QUERY);
        if (!StringUtils.isBlank(query)) {
            config.setQuery(query);
        }
        config.setHeartBeatIntervalMillis(heartbeatIntervalMs);
        config.setConsumeInOrder(fetchInOrder);
        config.setFetchIntervalMillis(fetchIntervalMs);
        String userAgent = context.getString(LOG_USER_AGENT);
        if (StringUtils.isBlank(userAgent)) {
            userAgent = LOG_CONNECTOR_USER_AGENT;
        }
        config.setUserAgent(userAgent);
        return config;
    }

    @VisibleForTesting
    static String createConsumerGroupName() {
        try {
            return InetAddress.getLocalHost().getHostName().replace('.', '-').toLowerCase();
        } catch (UnknownHostException e) {
            return UUID.randomUUID().toString();
        }
    }

    private EventDeserializer createDeserializer(Context context) {
        String deserializerName = context.getString(DESERIALIZER);
        EventDeserializer deserializer;
        if (deserializerName == null || deserializerName.isEmpty()) {
            deserializer = new DelimitedTextEventDeserializer();
        } else if (deserializerName.equals(DelimitedTextEventDeserializer.ALIAS)
                || deserializerName.equalsIgnoreCase(DelimitedTextEventDeserializer.class.getName())) {
            deserializer = new DelimitedTextEventDeserializer();
        } else if (deserializerName.equals(JSONEventDeserializer.ALIAS)
                || deserializerName.equalsIgnoreCase(JSONEventDeserializer.class.getName())) {
            deserializer = new JSONEventDeserializer();
        } else {
            try {
                deserializer = (EventDeserializer) Class.forName(deserializerName).newInstance();
            } catch (Exception e) {
                throw new IllegalArgumentException("Unable to instantiate deserializer: " + deserializerName
                        + " on source: " + getName(), e);
            }
        }
        deserializer.configure(context);
        return deserializer;
    }

    @Override
    public void start() throws FlumeException {
        LOG.info("Starting Loghub source {}...", getName());
        try {
            worker = new ClientWorker(() ->
                    new LogReceiver(getChannelProcessor(), deserializer, counter, getName(), maxRetry), config);
        } catch (Exception e) {
            throw new FlumeException("Fail to start log service client worker.", e);
        }
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Shutting down source...");
            worker.shutdown();
        }));
        Thread consumerThread = new Thread(worker);
        consumerThread.start();
        LOG.info("Loghub consumer group {} started.", getName());
        if (counter == null) {
            counter = new SourceCounter(getName());
        }
        counter.start();
        super.start();
        LOG.info("Loghub source {} started.", getName());
    }

    @Override
    public void stop() throws FlumeException {
        if (worker != null) {
            worker.shutdown();
            LOG.info("Loghub consumer stopped.");
        }
        if (counter != null) {
            counter.stop();
        }
        super.stop();
        LOG.info("Loghub source {} stopped. Metrics: {}", getName(), counter);
    }

}
