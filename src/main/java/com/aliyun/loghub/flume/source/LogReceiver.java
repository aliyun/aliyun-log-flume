package com.aliyun.loghub.flume.source;

import com.aliyun.openservices.log.common.FastLogGroup;
import com.aliyun.openservices.log.common.LogGroupData;
import com.aliyun.openservices.loghub.client.ILogHubCheckPointTracker;
import com.aliyun.openservices.loghub.client.exceptions.LogHubCheckPointException;
import com.aliyun.openservices.loghub.client.interfaces.ILogHubProcessor;
import org.apache.flume.ChannelFullException;
import org.apache.flume.Event;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.instrumentation.SourceCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Receives logs from Loghub and send to Flume channel.
 */
class LogReceiver implements ILogHubProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(LogReceiver.class);

    private final ChannelProcessor processor;
    private final EventDeserializer deserializer;
    private final SourceCounter sourceCounter;
    private final String sourceName;

    private int shardId = 0;
    private long checkpointSavedAt = 0;
    private Random random;
    private volatile boolean running;
    private volatile boolean success;
    private int maxRetry;

    LogReceiver(ChannelProcessor processor,
                EventDeserializer deserializer,
                SourceCounter sourceCounter,
                String sourceName,
                int maxRetry) {
        this.processor = processor;
        this.deserializer = deserializer;
        this.sourceCounter = sourceCounter;
        this.sourceName = sourceName;
        this.random = new Random();
        this.maxRetry = maxRetry;
        this.running = true;
        this.success = true;
    }

    @Override
    public void initialize(int shardId) {
        LOG.debug("LogReceiver for shard {} has been initialized", shardId);
        this.shardId = shardId;
    }

    private boolean emitEvents(List<Event> events) {
        int count = events.size();
        int retry = 0;
        long backoff = 1000;
        long maxBackoff = 30000;
        while (retry < maxRetry && running) {
            if (retry > 0) {
                try {
                    Thread.sleep(random.nextInt(500) + backoff);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    // It's OK as we don't need to exit base on this signal
                }
                backoff = Math.min((long) (backoff * 1.2), maxBackoff);
            }
            try {
                long beginTime = System.currentTimeMillis();
                LOG.debug("Sending {} events to Flume", count);
                processor.processEventBatch(events);
                sourceCounter.addToEventReceivedCount(count);
                long elapsedTime = System.currentTimeMillis() - beginTime;
                LOG.debug("Processed {} events, elapsedTime {}", count, elapsedTime);
                return true;
            } catch (ChannelFullException ex) {
                // For Queue Full, retry until success.
                LOG.warn("Queue full, wait and retry");
            } catch (final Exception ex) {
                if (retry < maxRetry - 1) {
                    LOG.warn("{} - failed to send data, retrying: {}", sourceName, ex.getMessage());
                    retry++;
                } else {
                    LOG.error("{} - failed to send data, data maybe loss", sourceName, ex);
                    success = false;
                    break;
                }
            }
        }
        return false;
    }

    @Override
    public String process(List<LogGroupData> logGroups, ILogHubCheckPointTracker tracker) {
        LOG.debug("Processing {} log groups", logGroups.size());
        int totalCount = 0;
        List<Event> batchEvents = new ArrayList<>();
        for (LogGroupData data : logGroups) {
            FastLogGroup logGroup = data.GetFastLogGroup();
            List<Event> events = deserializer.deserialize(logGroup);
            int numberOfEvents = events.size();
            totalCount += numberOfEvents;
            LOG.debug("{} events serialized for shard {}", numberOfEvents, shardId);
            if (numberOfEvents == 0) {
                continue;
            }
            batchEvents.addAll(events);
            if (batchEvents.size() < 1024) {
                continue;
            }
            success = emitEvents(batchEvents);
            if (success) {
                batchEvents = new ArrayList<>();
            }
        }
        if (!batchEvents.isEmpty()) {
            success = emitEvents(batchEvents);
        }
        LOG.debug("{} events have been serialized from {} log groups", totalCount, logGroups.size());
        long nowMs = System.currentTimeMillis();
        if (success && nowMs - checkpointSavedAt > 30 * 1000) {
            try {
                tracker.saveCheckPoint(true);
                checkpointSavedAt = nowMs;
                LOG.info("SLS source processed {} logs", sourceCounter.getEventReceivedCount());
            } catch (LogHubCheckPointException ex) {
                LOG.error("Failed to save checkpoint to remote sever", ex);
            }
        }
        return null;
    }

    @Override
    public void shutdown(ILogHubCheckPointTracker checkPointTracker) {
        LOG.info("Shutting down receiver.");
        running = false;
        if (success) {
            try {
                checkPointTracker.saveCheckPoint(true);
            } catch (Exception ex) {
                LOG.error("Failed to save checkpoint to remote sever", ex);
            }
        }
    }
}