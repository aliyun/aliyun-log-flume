package com.aliyun.loghub.flume.sink;

import com.aliyun.openservices.log.Client;
import com.aliyun.openservices.log.common.LogItem;
import com.aliyun.openservices.log.exception.LogException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Callable;

public class EventHandler implements Callable<Boolean> {
    private static final Logger LOG = LoggerFactory.getLogger(EventHandler.class);

    private final Client client;
    private final String project;
    private final String logstore;
    private final String source;
    private final List<LogItem> eventList;
    private final int maxRetry;

    private static final long MAX_BACKOFF = 3000;

    EventHandler(Client client,
                 String project,
                 String logstore,
                 String source,
                 List<LogItem> eventList,
                 int maxRetry) {
        this.client = client;
        this.project = project;
        this.logstore = logstore;
        this.source = source;
        this.eventList = eventList;
        this.maxRetry = maxRetry;
    }

    @Override
    public Boolean call() throws Exception {
        if (eventList.isEmpty()) {
            return true;
        }
        long backoff = 100;
        for (int i = 0; ; i++) {
            if (i > 0) {
                try {
                    Thread.sleep(backoff);
                } catch (InterruptedException ex) {
                    // It's okay
                    Thread.currentThread().interrupt();
                }
                backoff = Math.min(backoff * 2, MAX_BACKOFF);
            }
            try {
                client.PutLogs(project, logstore, "", eventList, source);
                LOG.info("{} events has been sent to Log Service", eventList.size());
                return true;
            } catch (LogException ex) {
                int code = ex.GetHttpCode();
                boolean alwaysRetry = code >= 500 || code == 403 || code <= 0;
                if (alwaysRetry || i < maxRetry - 1) {
                    LOG.warn("Retry on error={}, status={}", ex.GetErrorMessage(), code);
                } else {
                    LOG.error("Send events to Log Service failed", ex);
                    throw ex;
                }
            }
        }
    }
}
