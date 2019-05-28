package com.aliyun.loghub.flume.source;

import com.aliyun.loghub.flume.internal.RetryContext;
import com.aliyun.loghub.flume.internal.RetryPolicy;
import com.aliyun.openservices.log.common.FastLogGroup;
import com.aliyun.openservices.log.common.LogGroupData;
import com.aliyun.openservices.loghub.client.ILogHubCheckPointTracker;
import com.aliyun.openservices.loghub.client.exceptions.LogHubCheckPointException;
import com.aliyun.openservices.loghub.client.interfaces.ILogHubProcessor;
import org.apache.flume.Event;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.instrumentation.SourceCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Receives logs from Loghub and send to Flume channel.
 */
class LogReceiver implements ILogHubProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(LogReceiver.class);

    private final ChannelProcessor processor;
    private final EventDeserializer deserializer;
    private final RetryPolicy retryPolicy;
    private final SourceCounter sourceCounter;
    private final String sourceName;

    private int shardId = 0;
    private long checkpointSavedAt = 0;

    LogReceiver(ChannelProcessor processor,
                EventDeserializer deserializer,
                RetryPolicy retryPolicy,
                SourceCounter sourceCounter,
                String sourceName) {
        this.processor = processor;
        this.deserializer = deserializer;
        this.retryPolicy = retryPolicy;
        this.sourceCounter = sourceCounter;
        this.sourceName = sourceName;
    }

    @Override
    public void initialize(int shardId) {
        LOG.debug("LogReceiver for shard {} has been initialized", shardId);
        this.shardId = shardId;
    }

    @Override
    public String process(List<LogGroupData> logGroups, ILogHubCheckPointTracker tracker) {
        RetryContext context = new RetryContext();
        for (LogGroupData data : logGroups) {
            FastLogGroup logGroup = data.GetFastLogGroup();
            List<Event> events = deserializer.deserialize(logGroup);

            int numberOfEvents = events.size();
            LOG.debug("{} events serialized for shard {}", numberOfEvents, shardId);
            if (numberOfEvents == 0) {
                continue;
            }
            context.reset();
            while (true) {
                Exception exception;
                try {
                    long beginTime = System.currentTimeMillis();
                    processor.processEventBatch(events);
                    sourceCounter.addToEventAcceptedCount(events.size());
                    long elapsedTime = System.currentTimeMillis() - beginTime;
                    LOG.debug("Processed {} events, elapsedTime {}", numberOfEvents, elapsedTime);
                    break;
                } catch (final Exception ex) {
                    exception = ex;
                }
                if (!retryPolicy.shouldRetry(context)) {
                    LOG.error("{} - Exception thrown while processing events", sourceName, exception);
                    break;
                }
                long backoffMs = retryPolicy.getBackoffMs(context);
                LOG.warn("Failed to emit events to channel: {}, wait {} ms before next attempt",
                        exception.getMessage(), backoffMs);
                context.incrementRetryCount();
                if (backoffMs <= 0) {
                    continue;
                }
                try {
                    Thread.sleep(backoffMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    // Do not save checkpoint!
                    return null;
                }
            }
        }
        long nowMs = System.currentTimeMillis();
        if (nowMs - checkpointSavedAt > 30 * 1000) {
            try {
                tracker.saveCheckPoint(true);
                checkpointSavedAt = nowMs;
            } catch (LogHubCheckPointException ex) {
                LOG.error("Failed to save checkpoint to remote sever", ex);
            }
        }
        return null;
    }

    @Override
    public void shutdown(ILogHubCheckPointTracker checkPointTracker) {
        LOG.info("Shutting down receiver.");
        try {
            checkPointTracker.saveCheckPoint(true);
        } catch (Exception ex) {
            LOG.error("Failed to save checkpoint to remote sever", ex);
        }
    }
}