package com.aliyun.loghub.flume.source;

import com.aliyun.openservices.loghub.client.ClientWorker;
import com.aliyun.openservices.loghub.client.config.LogHubConfig;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.source.AbstractPollableSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static com.aliyun.loghub.flume.LoghubConstants.ACCESS_KEY_ID_KEY;
import static com.aliyun.loghub.flume.LoghubConstants.ACCESS_KEY_SECRET_KEY;
import static com.aliyun.loghub.flume.LoghubConstants.BACKOFF_SLEEP_INCREMENT_KEY;
import static com.aliyun.loghub.flume.LoghubConstants.BATCH_DURATION_KEY;
import static com.aliyun.loghub.flume.LoghubConstants.BATCH_SIZE_KEY;
import static com.aliyun.loghub.flume.LoghubConstants.CONSUMER_GROUP_KEY;
import static com.aliyun.loghub.flume.LoghubConstants.CONSUME_POSITION_END;
import static com.aliyun.loghub.flume.LoghubConstants.CONSUME_POSITION_KEY;
import static com.aliyun.loghub.flume.LoghubConstants.CONSUME_POSITION_START_TIME_KEY;
import static com.aliyun.loghub.flume.LoghubConstants.CONSUME_POSITION_TIMESTAMP;
import static com.aliyun.loghub.flume.LoghubConstants.CSV_FORMAT;
import static com.aliyun.loghub.flume.LoghubConstants.DEFAULT_BACKOFF_SLEEP_INCREMENT;
import static com.aliyun.loghub.flume.LoghubConstants.DEFAULT_BATCH_DURATION_MS;
import static com.aliyun.loghub.flume.LoghubConstants.DEFAULT_BATCH_SIZE;
import static com.aliyun.loghub.flume.LoghubConstants.DEFAULT_FETCH_IN_ORDER;
import static com.aliyun.loghub.flume.LoghubConstants.DEFAULT_HEARTBEAT_INTERVAL_MS;
import static com.aliyun.loghub.flume.LoghubConstants.DEFAULT_MAX_BACKOFF_SLEEP;
import static com.aliyun.loghub.flume.LoghubConstants.DEFAULT_MAX_BUFFER_SIZE;
import static com.aliyun.loghub.flume.LoghubConstants.DEFAULT_SOURCE_FORMAT;
import static com.aliyun.loghub.flume.LoghubConstants.DEFAULT_USER_RECORD_TIME;
import static com.aliyun.loghub.flume.LoghubConstants.ENDPOINT_KEY;
import static com.aliyun.loghub.flume.LoghubConstants.FETCH_IN_ORDER_KEY;
import static com.aliyun.loghub.flume.LoghubConstants.FORMAT_KEY;
import static com.aliyun.loghub.flume.LoghubConstants.HEARTBEAT_INTERVAL_KEY;
import static com.aliyun.loghub.flume.LoghubConstants.JSON_FORMAT;
import static com.aliyun.loghub.flume.LoghubConstants.LOGSTORE_KEY;
import static com.aliyun.loghub.flume.LoghubConstants.MAX_BACKOFF_SLEEP_KEY;
import static com.aliyun.loghub.flume.LoghubConstants.MAX_BUFFER_SIZE;
import static com.aliyun.loghub.flume.LoghubConstants.PROJECT_KEY;
import static com.aliyun.loghub.flume.LoghubConstants.USER_RECORD_TIME_KEY;
import static com.aliyun.loghub.flume.Utils.ensureNotEmpty;
import static com.aliyun.openservices.loghub.client.config.LogHubCursorPosition.BEGIN_CURSOR;
import static com.aliyun.openservices.loghub.client.config.LogHubCursorPosition.END_CURSOR;
import static com.google.common.base.Preconditions.checkArgument;


public class LoghubSource extends AbstractPollableSource implements Configurable {
    private static final Logger LOG = LoggerFactory.getLogger(LoghubSource.class);

    private LogHubConfig config;
    private ClientWorker worker;
    private long batchDurationMillis;
    private int batchSize;
    private SourceCounter counter;
    private long backoffSleepIncrement;
    private long maxBackOffSleepInterval;
    private int maxBufferSize;
    private BlockingQueue<List<Event>> bufferQueue;
    private EventSerializer serializer;

    @Override
    protected void doConfigure(Context context) throws FlumeException {
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
        long heartbeatIntervalMs = context.getLong(HEARTBEAT_INTERVAL_KEY, DEFAULT_HEARTBEAT_INTERVAL_MS);
        boolean fetchInOrder = context.getBoolean(FETCH_IN_ORDER_KEY, DEFAULT_FETCH_IN_ORDER);
        boolean useRecordTime = context.getBoolean(USER_RECORD_TIME_KEY, DEFAULT_USER_RECORD_TIME);
        batchSize = context.getInteger(BATCH_SIZE_KEY, DEFAULT_BATCH_SIZE);
        batchDurationMillis = context.getLong(BATCH_DURATION_KEY, DEFAULT_BATCH_DURATION_MS);
        backoffSleepIncrement = context.getLong(BACKOFF_SLEEP_INCREMENT_KEY, DEFAULT_BACKOFF_SLEEP_INCREMENT);
        maxBackOffSleepInterval = context.getLong(MAX_BACKOFF_SLEEP_KEY, DEFAULT_MAX_BACKOFF_SLEEP);
        maxBufferSize = context.getInteger(MAX_BUFFER_SIZE, DEFAULT_MAX_BUFFER_SIZE);

        if (StringUtils.isBlank(consumerGroup)) {
            LOG.info("Loghub Consumer Group is not specified, will generate a random Consumer Group name.");
            consumerGroup = UUID.randomUUID().toString();
        }
        String consumerId = UUID.randomUUID().toString();

        LOG.info("Consumer Group {}, Consumer {}", consumerGroup, consumerId);

        String position = context.getString(CONSUME_POSITION_KEY, CONSUME_POSITION_END);
        switch (position) {
            case CONSUME_POSITION_TIMESTAMP:
                Integer startTime = context.getInteger(CONSUME_POSITION_START_TIME_KEY);
                checkArgument(startTime != null, String.format("Missing parameter: %s when set config %s to %s.",
                        CONSUME_POSITION_START_TIME_KEY, CONSUME_POSITION_KEY, CONSUME_POSITION_TIMESTAMP));
                config = new LogHubConfig(consumerGroup, consumerId, endpoint, project, logstore, accessKeyId, accessKey,
                        startTime, heartbeatIntervalMs, fetchInOrder);
                break;
            case CONSUME_POSITION_END:
                config = new LogHubConfig(consumerGroup, consumerId, endpoint, project, logstore, accessKeyId, accessKey,
                        END_CURSOR, heartbeatIntervalMs, fetchInOrder);
            default:
                // Start from earliest by default
                config = new LogHubConfig(consumerGroup, consumerId, endpoint, project, logstore, accessKeyId, accessKey,
                        BEGIN_CURSOR, heartbeatIntervalMs, fetchInOrder);
                break;
        }
        String format = context.getString(FORMAT_KEY, DEFAULT_SOURCE_FORMAT);
        serializer = createSerializer(format, useRecordTime);
        serializer.configure(context);
    }

    private static EventSerializer createSerializer(String format, boolean useRecordTime) {
        if (StringUtils.isBlank(format)) {
            LOG.info("Event format is not specified, will use default format {}", format);
            format = DEFAULT_SOURCE_FORMAT;
        }
        if (format.equals(CSV_FORMAT)) {
            return new CSVEventSerializer(useRecordTime);
        } else if (format.equals(JSON_FORMAT)) {
            return new JSONEventSerializer(useRecordTime);
        } else {
            throw new IllegalArgumentException("Unimplemented format for Loghub source: " + format);
        }
    }

    @Override
    protected Status doProcess() throws EventDeliveryException {
        long logCount = 0L;
        long elapsedTime = 0L;
        long beginTime = System.currentTimeMillis();
        List<List<Event>> events = new ArrayList<>();

        Status result = Status.READY;
        try {
            while (elapsedTime < batchDurationMillis && logCount < batchSize) {
                bufferQueue.drainTo(events);
                if (events.isEmpty()) {
                    if (logCount == 0) {
                        LOG.info("No events received from Loghub source.");
                        result = Status.BACKOFF;
                    }
                    break;
                }
                for (List<Event> item : events) {
                    int count = item.size();
                    logCount += count;
                    for (int i = 0; i < count; i += batchSize) {
                        int endIndex = Math.min(count, i + batchSize);
                        List<Event> batch = item.subList(i, endIndex);
                        getChannelProcessor().processEventBatch(batch);
                        counter.incrementAppendBatchReceivedCount();
                        counter.addToEventAcceptedCount(batch.size());
                        counter.incrementAppendBatchAcceptedCount();
                    }
                }
                events.clear();
                elapsedTime = System.currentTimeMillis() - beginTime;
            }
            LOG.info("Processed {} events, cost {}", logCount, elapsedTime);
        } catch (Exception ex) {
            counter.incrementEventReadOrChannelFail(ex);
            LOG.error("{} - Exception thrown while processing events", getName(), ex);
            result = Status.BACKOFF;
        }
        return result;
    }

    @Override
    public long getBackOffSleepIncrement() {
        return backoffSleepIncrement;
    }

    @Override
    public long getMaxBackOffSleepInterval() {
        return maxBackOffSleepInterval;
    }

    @Override
    protected void doStart() throws FlumeException {
        LOG.info("Starting Loghub source {}...", getName());
        bufferQueue = new LinkedBlockingQueue<>(maxBufferSize);
        try {
            worker = new ClientWorker(() -> new LoghubProcessor(serializer, bufferQueue), config);
        } catch (Exception e) {
            throw new FlumeException("Fail to start log service client worker.", e);
        }
        Thread consumerThread = new Thread(worker);
        consumerThread.start();
        LOG.info("Loghub Consumer Group {} started.", getName());
        if (counter == null) {
            counter = new SourceCounter(getName());
        }
        counter.start();
        LOG.info("Loghub source {} started.", getName());
    }

    @Override
    protected void doStop() throws FlumeException {
        if (worker != null) {
            worker.shutdown();
            LOG.info("Loghub consumer stopped.");
        }
        if (counter != null) {
            counter.stop();
        }
        LOG.info("Loghub Source {} stopped. Metrics: {}", getName(), counter);
    }

}
