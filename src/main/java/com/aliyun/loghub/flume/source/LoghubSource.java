package com.aliyun.loghub.flume.source;

import com.aliyun.openservices.loghub.client.ClientWorker;
import com.aliyun.openservices.loghub.client.config.LogHubConfig;
import com.aliyun.openservices.loghub.client.config.LogHubConfig.ConsumePosition;
import com.google.common.base.Preconditions;
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
import static com.aliyun.loghub.flume.Constants.BATCH_SIZE_KEY;
import static com.aliyun.loghub.flume.Constants.CONSUMER_GROUP_KEY;
import static com.aliyun.loghub.flume.Constants.CONSUME_POSITION_END;
import static com.aliyun.loghub.flume.Constants.CONSUME_POSITION_KEY;
import static com.aliyun.loghub.flume.Constants.CONSUME_POSITION_START_TIME_KEY;
import static com.aliyun.loghub.flume.Constants.CONSUME_POSITION_TIMESTAMP;
import static com.aliyun.loghub.flume.Constants.CSV_FORMAT;
import static com.aliyun.loghub.flume.Constants.DEFAULT_BATCH_SIZE;
import static com.aliyun.loghub.flume.Constants.DEFAULT_FETCH_INTERVAL_MS;
import static com.aliyun.loghub.flume.Constants.DEFAULT_FETCH_IN_ORDER;
import static com.aliyun.loghub.flume.Constants.DEFAULT_HEARTBEAT_INTERVAL_MS;
import static com.aliyun.loghub.flume.Constants.DEFAULT_SOURCE_FORMAT;
import static com.aliyun.loghub.flume.Constants.ENDPOINT_KEY;
import static com.aliyun.loghub.flume.Constants.FETCH_INTERVAL_MS;
import static com.aliyun.loghub.flume.Constants.FETCH_IN_ORDER_KEY;
import static com.aliyun.loghub.flume.Constants.FORMAT_KEY;
import static com.aliyun.loghub.flume.Constants.HEARTBEAT_INTERVAL_MS;
import static com.aliyun.loghub.flume.Constants.JSON_FORMAT;
import static com.aliyun.loghub.flume.Constants.LOGSTORE_KEY;
import static com.aliyun.loghub.flume.Constants.PROJECT_KEY;
import static com.google.common.base.Preconditions.checkArgument;


public class LoghubSource extends AbstractSource implements
        EventDrivenSource, Configurable {
    private static final Logger LOG = LoggerFactory.getLogger(LoghubSource.class);

    private LogHubConfig config;
    private ClientWorker worker;
    private SourceCounter counter;
    private EventSerializer serializer;

    @Override
    public void configure(Context context) {
        String consumerGroup = context.getString(CONSUMER_GROUP_KEY);
        String endpoint = context.getString(ENDPOINT_KEY);
        ensureNotEmpty(endpoint, ENDPOINT_KEY);
        String project = context.getString(PROJECT_KEY);
        ensureNotEmpty(project, PROJECT_KEY);
        String logstore = context.getString(LOGSTORE_KEY);
        ensureNotEmpty(logstore, LOGSTORE_KEY);
        String accessKeyId = context.getString(ACCESS_KEY_ID_KEY);
        ensureNotEmpty(accessKeyId, ACCESS_KEY_ID_KEY);
        String accessKey = context.getString(ACCESS_KEY_SECRET_KEY);
        ensureNotEmpty(accessKey, ACCESS_KEY_SECRET_KEY);
        long heartbeatIntervalMs = context.getLong(HEARTBEAT_INTERVAL_MS, DEFAULT_HEARTBEAT_INTERVAL_MS);
        long fetchIntervalMs = context.getLong(FETCH_INTERVAL_MS, DEFAULT_FETCH_INTERVAL_MS);
        boolean fetchInOrder = context.getBoolean(FETCH_IN_ORDER_KEY, DEFAULT_FETCH_IN_ORDER);
        int batchSize = context.getInteger(BATCH_SIZE_KEY, DEFAULT_BATCH_SIZE);

        if (StringUtils.isBlank(consumerGroup)) {
            LOG.info("Loghub Consumer Group is not specified, will generate a random Consumer Group name.");
            consumerGroup = createConsumerGroupName();
        }
        String consumerId = UUID.randomUUID().toString();
        LOG.info("Using consumer group {}, consumer  {}", consumerGroup, consumerId);

        String position = context.getString(CONSUME_POSITION_KEY, CONSUME_POSITION_END);
        switch (position) {
            case CONSUME_POSITION_TIMESTAMP:
                Integer startTime = context.getInteger(CONSUME_POSITION_START_TIME_KEY);
                checkArgument(startTime != null, String.format("Missing parameter: %s when set config %s to %s.",
                        CONSUME_POSITION_START_TIME_KEY, CONSUME_POSITION_KEY, CONSUME_POSITION_TIMESTAMP));
                config = new LogHubConfig(consumerGroup, consumerId, endpoint, project, logstore, accessKeyId, accessKey,
                        startTime, batchSize);
                break;
            case CONSUME_POSITION_END:
                config = new LogHubConfig(consumerGroup, consumerId, endpoint, project, logstore, accessKeyId, accessKey,
                        ConsumePosition.END_CURSOR);
            default:
                // Start from earliest by default
                config = new LogHubConfig(consumerGroup, consumerId, endpoint, project, logstore, accessKeyId, accessKey,
                        ConsumePosition.BEGIN_CURSOR);
                break;
        }
        config.setHeartBeatIntervalMillis(heartbeatIntervalMs);
        config.setConsumeInOrder(fetchInOrder);
        config.setDataFetchIntervalMillis(fetchIntervalMs);
        String format = context.getString(FORMAT_KEY, DEFAULT_SOURCE_FORMAT);
        serializer = createSerializer(format);
        serializer.configure(context);
    }

    private static String createConsumerGroupName() {
        try {
            return InetAddress.getLocalHost().getCanonicalHostName() + ":" + UUID.randomUUID();
        } catch (UnknownHostException e) {
            return UUID.randomUUID().toString();
        }
    }

    private static void ensureNotEmpty(String value, String name) {
        Preconditions.checkArgument(value != null && !value.isEmpty(), "Missing parameter: " + name);
    }

    private static EventSerializer createSerializer(String format) {
        if (StringUtils.isBlank(format)) {
            LOG.info("Event format is not specified, will use default format {}", format);
            format = DEFAULT_SOURCE_FORMAT;
        }
        if (format.equals(CSV_FORMAT)) {
            return new CSVEventSerializer();
        } else if (format.equals(JSON_FORMAT)) {
            return new JSONEventSerializer();
        } else {
            throw new IllegalArgumentException("Unimplemented format for Loghub source: " + format);
        }
    }

    @Override
    public void start() throws FlumeException {
        LOG.info("Starting Loghub source {}...", getName());
        try {
            worker = new ClientWorker(
                    () -> new LogReceiver(getChannelProcessor(), serializer, counter, getName()), config);
        } catch (Exception e) {
            throw new FlumeException("Fail to start log service client worker.", e);
        }
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Shutting down consumer group thread...");
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
