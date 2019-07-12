package com.aliyun.loghub.flume.sink;

import com.aliyun.openservices.log.Client;
import com.aliyun.openservices.log.common.LogItem;
import com.aliyun.openservices.log.exception.LogException;
import org.apache.flume.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

public class EventHandler implements Callable<Boolean> {
    private static final Logger LOG = LoggerFactory.getLogger(EventHandler.class);

    private final Client client;
    private final String project;
    private final String logstore;
    private final String source;
    private final List<Event> eventList;
    private final EventSerializer serializer;
    private final int maxRetry;

    EventHandler(Client client,
                 String project,
                 String logstore,
                 String source,
                 List<Event> eventList,
                 EventSerializer serializer,
                 int maxRetry) {
        this.client = client;
        this.project = project;
        this.logstore = logstore;
        this.source = source;
        this.eventList = eventList;
        this.serializer = serializer;
        this.maxRetry = maxRetry;
    }

    @Override
    public Boolean call() throws Exception {
        List<LogItem> records = new ArrayList<>(eventList.size());
        for (Event event : eventList) {
            LogItem record;
            try {
                record = serializer.serialize(event);
            } catch (Exception ex) {
                LOG.error("Serialize event to log record failed", ex);
                continue;
            }
            records.add(record);
        }
        if (records.isEmpty()) {
            return true;
        }
        for (int i = 0; i < maxRetry; i++) {
            if (i > 0) {
                try {
                    Thread.sleep(50);
                } catch (InterruptedException ex) {
                    // It's okay
                    Thread.currentThread().interrupt();
                }
            }
            try {
                client.PutLogs(project, logstore, "", records, source);
                LOG.info("{} events has been sent to Log Service", records.size());
                return true;
            } catch (LogException ex) {
                if ((ex.GetHttpCode() >= 500 || ex.GetHttpCode() == 403) && i < maxRetry - 1) {
                    LOG.warn("Retry on error: {}", ex.GetErrorMessage());
                } else {
                    LOG.error("Send events to Log Service failed", ex);
                    throw ex;
                }
            }
        }
        return false;
    }
}
