package com.aliyun.loghub.flume.source;

import com.aliyun.openservices.log.common.FastLogGroup;
import com.aliyun.openservices.log.common.LogGroupData;
import com.aliyun.openservices.loghub.client.ILogHubCheckPointTracker;
import com.aliyun.openservices.loghub.client.exceptions.LogHubCheckPointException;
import com.aliyun.openservices.loghub.client.interfaces.ILogHubProcessor;
import org.apache.flume.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.BlockingQueue;


public class LoghubProcessor implements ILogHubProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(LoghubProcessor.class);

    private int shardId = 0;
    private long checkpointSavedAt = 0;
    private final EventSerializer serializer;
    private final BlockingQueue<List<Event>> bufferQueue;

    LoghubProcessor(EventSerializer serializer, BlockingQueue<List<Event>> bufferQueue) {
        this.serializer = serializer;
        this.bufferQueue = bufferQueue;
    }

    public void initialize(int shardId) {
        this.shardId = shardId;
        LOG.debug("Initializing processor for shard {}", shardId);
    }

    public String process(List<LogGroupData> logGroups,
                          ILogHubCheckPointTracker checkPointTracker) {
        for (LogGroupData data : logGroups) {
            FastLogGroup logGroup = data.GetFastLogGroup();
            List<Event> events = serializer.serialize(logGroup);
            LOG.debug("{} events serialized from log group for shard {}", events.size(), shardId);
            try {
                bufferQueue.put(events);
            } catch (InterruptedException iex) {
                Thread.currentThread().interrupt();
                // TODO what now ?
                break;
            }
        }
        long nowMs = System.currentTimeMillis();
        if (nowMs - checkpointSavedAt > 30 * 1000) {
            try {
                checkPointTracker.saveCheckPoint(true);
                checkpointSavedAt = nowMs;
            } catch (LogHubCheckPointException ex) {
                LOG.error("Failed to save checkpoint to remote sever", ex);
            }
        }
        // TODO How to fix checkpoint saved but events not emitted.
        return null;
    }

    public void shutdown(ILogHubCheckPointTracker checkPointTracker) {
        LOG.info("Shutting down Loghub processor.");
        try {
            checkPointTracker.saveCheckPoint(true);
        } catch (Exception ex) {
            LOG.error("Failed to save checkpoint to remote sever", ex);
        }
    }
}
