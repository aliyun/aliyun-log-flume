package com.aliyun.loghub.flume.sink;

import com.aliyun.loghub.flume.Validate;
import com.aliyun.loghub.flume.source.DelimitedTextEventDeserializer;
import com.aliyun.openservices.log.Client;
import com.aliyun.openservices.log.common.LogContent;
import com.aliyun.openservices.log.common.LogItem;
import com.aliyun.openservices.log.util.NetworkUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import static com.aliyun.loghub.flume.Constants.*;

public class LoghubSink extends AbstractSink implements Configurable {
    private static final Logger LOG = LoggerFactory.getLogger(LoghubSink.class);

    private int batchSize;
    private long bufferBytes;
    private int maxRetry;
    private int concurrency;
    private long maxBufferTime;
    private String project;
    private String logstore;
    private String source;
    private EventSerializer serializer;
    private final List<Future<Boolean>> producerFutures = new ArrayList<>();
    private ThreadPoolExecutor executor;
    private Client client;
    private SinkCounter counter;

    @Override
    public synchronized void start() {
        executor = new ThreadPoolExecutor(0, concurrency,
                60L, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(100),
                Executors.defaultThreadFactory(),
                new ThreadPoolExecutor.CallerRunsPolicy());
        executor.allowCoreThreadTimeOut(true);
        counter.start();
        source = NetworkUtils.getLocalMachineIP();
        super.start();
        LOG.info("Loghub Sink {} started.", getName());
    }

    private static long getLogItemSize(LogItem item) {
        long size = 0;
        for (LogContent logContent : item.mContents) {
            if (logContent.mKey != null) {
                size += logContent.mKey.length();
            }
            if (logContent.mValue != null) {
                size += logContent.mValue.length();
            }
        }
        return size;
    }

    @Override
    public Status process() throws EventDeliveryException {
        Channel channel = getChannel();
        Transaction transaction = null;
        long earliestEventTime = -1;
        Status result = Status.READY;
        List<LogItem> buffer = new ArrayList<>();
        long totalBytes = 0;
        try {
            long processedEvents = 0;
            transaction = channel.getTransaction();
            transaction.begin();
            for (; processedEvents < batchSize; processedEvents++) {
                Event event = channel.take();
                if (event == null) {
                    // no events available in channel
                    if (processedEvents == 0) {
                        result = Status.BACKOFF;
                        counter.incrementBatchEmptyCount();
                    } else {
                        counter.incrementBatchUnderflowCount();
                    }
                    break;
                }
                counter.incrementEventDrainAttemptCount();
                LogItem logItem;
                try {
                    logItem = serializer.serialize(event);
                } catch (Exception ex) {
                    LOG.error("Failed to serialize event to log", ex);
                    continue;
                }
                long logItemSize = getLogItemSize(logItem);
                if (logItemSize >= MAX_LOG_GROUP_BYTES) {
                    LOG.error("Event size {} is too large", logItemSize);
                    continue;
                }
                if (earliestEventTime < 0) {
                    earliestEventTime = System.currentTimeMillis();
                }
                if (totalBytes + logItemSize > bufferBytes || System.currentTimeMillis() - earliestEventTime >= maxBufferTime) {
                    LOG.debug("Flushing events to Log service, event count {}", buffer.size());
                    List<LogItem> events = buffer;
                    producerFutures.add(sendEvents(events));
                    buffer = new ArrayList<>();
                    earliestEventTime = -1;
                    totalBytes = 0;
                }
                buffer.add(logItem);
                totalBytes += logItemSize;
            }
            if (!buffer.isEmpty()) {
                producerFutures.add(sendEvents(buffer));
            }
            if (processedEvents > 0) {
                for (Future<Boolean> future : producerFutures) {
                    future.get();
                }
                producerFutures.clear();
                counter.addToEventDrainSuccessCount(processedEvents);
            }
            transaction.commit();
        } catch (Exception ex) {
            LOG.error("Failed to publish events", ex);
            if (transaction != null) {
                try {
                    transaction.rollback();
                } catch (Exception e) {
                    LOG.error("Transaction rollback failed", e);
                }
            }
            throw new EventDeliveryException("Failed to publish events", ex);
        } finally {
            if (transaction != null) {
                transaction.close();
            }
        }
        return result;
    }

    private Future<Boolean> sendEvents(List<LogItem> events) {
        return executor.submit(new EventHandler(client, project, logstore, source, events, maxRetry));
    }

    @Override
    public void configure(Context context) {
        String endpoint = context.getString(ENDPOINT_KEY);
        Validate.notEmpty(endpoint, ENDPOINT_KEY);
        project = context.getString(PROJECT_KEY);
        Validate.notEmpty(project, PROJECT_KEY);
        logstore = context.getString(LOGSTORE_KEY);
        Validate.notEmpty(logstore, LOGSTORE_KEY);
        String accessKeyId = context.getString(ACCESS_KEY_ID_KEY);
        Validate.notEmpty(accessKeyId, ACCESS_KEY_ID_KEY);
        String accessKey = context.getString(ACCESS_KEY_SECRET_KEY);
        Validate.notEmpty(accessKey, ACCESS_KEY_SECRET_KEY);
        client = new Client(endpoint, accessKeyId, accessKey);
        String userAgent = context.getString(LOG_USER_AGENT);
        if (StringUtils.isEmpty(userAgent)) {
            userAgent = LOG_CONNECTOR_USER_AGENT;
        }
        client.setUserAgent(userAgent);
        logstore = context.getString(LOGSTORE_KEY);
        if (counter == null) {
            counter = new SinkCounter(getName());
        }
        batchSize = context.getInteger(BATCH_SIZE, DEFAULT_BATCH_SIZE);
        bufferBytes = context.getLong(BUFFER_BYTES, DEFAULT_BUFFER_BYTES);
        maxBufferTime = context.getInteger(MAX_BUFFER_TIME, 3000);
        maxRetry = context.getInteger(MAX_RETRY, DEFAULT_MAX_RETRY);
        int cores = Runtime.getRuntime().availableProcessors();
        concurrency = context.getInteger("concurrency", cores);
        serializer = createSerializer(context);
    }

    private EventSerializer createSerializer(Context context) {
        String serializerName = context.getString(SERIALIZER);
        EventSerializer serializer;
        if (serializerName == null || serializerName.isEmpty()) {
            serializer = new DelimitedTextEventSerializer();
        } else if (serializerName.equals(DelimitedTextEventSerializer.ALIAS)
                || serializerName.equalsIgnoreCase(DelimitedTextEventDeserializer.class.getName())) {
            serializer = new DelimitedTextEventSerializer();
        } else if (serializerName.equals(SimpleEventSerializer.ALIAS)
                || serializerName.equalsIgnoreCase(SimpleEventSerializer.class.getName())) {
            serializer = new SimpleEventSerializer();
        } else if (serializerName.endsWith(RegexEventSerializer.ALIAS)
                || serializerName.equalsIgnoreCase(RegexEventSerializer.class.getName())) {
            serializer = new RegexEventSerializer();
        } else if (serializerName.endsWith(JSONEventSerializer.ALIAS)
                || serializerName.equalsIgnoreCase(JSONEventSerializer.class.getName())) {
            serializer = new JSONEventSerializer();
        } else {
            try {
                serializer = (EventSerializer) Class.forName(serializerName).newInstance();
            } catch (Exception e) {
                throw new IllegalArgumentException("Unable to instantiate serializer: " + serializerName
                        + " on sink: " + getName(), e);
            }
        }
        serializer.configure(context);
        return serializer;
    }

    @Override
    public synchronized void stop() {
        super.stop();
        LOG.info("Stopping Loghub Sink {}", getName());
        if (executor != null) {
            try {
                executor.shutdown();
                executor.awaitTermination(30, TimeUnit.SECONDS);
            } catch (final Exception ex) {
                LOG.error("Error while closing Loghub sink {}.", getName(), ex);
            }
        }
    }
}
