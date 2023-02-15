package com.oceanbase.jdbc.internal.failover.BlackList.append;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.oceanbase.jdbc.HostAddress;
import com.oceanbase.jdbc.internal.failover.utils.Consts;
import com.oceanbase.jdbc.internal.logging.Logger;
import com.oceanbase.jdbc.internal.logging.LoggerFactory;

public class RetryDuration implements AppendStrategy {

    private static final Logger logger = LoggerFactory.getLogger(RetryDuration.class);

    private long        durationMs;
    private int         retryTimes;
    private static long maxDurationMs;
    private static ConcurrentMap<HostAddress, HashSet<Long>> failureRecords = new ConcurrentHashMap<>();

    public RetryDuration(long durationMs, int retryTimes) {
        this.durationMs = durationMs;
        this.retryTimes = retryTimes;
    }

    @Override
    public String toString() {
        return "RetryDuration{}";
    }

    public String toJson() {
        return "\"APPEND_STRATEGY\":{" + "\"NAME\":\"RETRYDURATION\"" + ",\"RETRYTIMES\":" + retryTimes + ",\"DURATION\":" + durationMs + "}";
    }

    public long getDuration() {
        return durationMs;
    }

    public int getRetryTimes() {
        return retryTimes;
    }

    @Override
    public boolean needToAppend(HostAddress host, Properties properties) {
        boolean need = false;

        synchronized (host) {
            Set history = failureRecords.putIfAbsent(host, new HashSet<>());
            if (history == null) {
                logger.warn("failureRecords add {}", host.host);
            }

            int count = 0;

            Set<Long> recentFailedTimeList = failureRecords.get(host);
            synchronized (recentFailedTimeList) {
                long failedTimeMs = Long.parseLong((String) properties.get(Consts.FAILED_TIME_MS));
                logger.debug("{} --> {}", host.host, recentFailedTimeList);
                logger.debug("{} add {}", host.host, failedTimeMs);
                recentFailedTimeList.add(failedTimeMs);
                logger.debug("{} --> {}", host.host, failureRecords.get(host));

                Iterator<Long> longIterator = recentFailedTimeList.iterator();
                while(longIterator.hasNext()) {
                    long ts = longIterator.next();
                    if (failedTimeMs - ts > maxDurationMs) {
                        logger.debug("{} --> {}", host.host, recentFailedTimeList);
                        logger.debug("{} remove {}", host.host, ts);
                        longIterator.remove();
                        logger.debug("{} --> {}", host.host, failureRecords.get(host));
                    } else if (failedTimeMs - ts <= durationMs) {
                        count++;
                    }
                }
            }

            need = count >= retryTimes;
            if (need) {
                logger.warn("failureRecords remove {}", host.host);
                removeFromFailureRecords(host);
            }
        }

        return need;
    }

    public static void removeFromFailureRecords(HostAddress host) {
        if (host != null) {
            failureRecords.remove(host);
        }
    }

    public static void updateMaxDuration(long newDuration) {
        maxDurationMs = Math.max(newDuration, maxDurationMs);
    }

}
