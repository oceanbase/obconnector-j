package com.oceanbase.jdbc.internal.failover.BlackList.append;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.oceanbase.jdbc.HostAddress;
import com.oceanbase.jdbc.internal.failover.utils.Consts;

public class RetryDuration implements AppendStrategy {
    private long duration; //millisecond
    private int retryTimes;
    private static ConcurrentMap<HostAddress, List<Long>> failureRecords = new ConcurrentHashMap<>();

    private static long maxDuration;

    public RetryDuration(long duration, int retryTimes) {
        this.duration = duration;
        this.retryTimes = retryTimes;
    }

    @Override
    public String toString() {
        return "RetryDuration{}";
    }

    @Override
    public boolean needToAppend(HostAddress host, Properties properties) {
        long failedTime = Long.parseLong((String) properties.get(Consts.FAILED_TIME));
        if (!failureRecords.containsKey(host)) {
            failureRecords.put(host, new ArrayList<Long>());
        }
        List<Long> recentFailedTimeList = failureRecords.get(host);
        recentFailedTimeList.add(failedTime);
        Iterator<Long> longIterator = recentFailedTimeList.iterator();
        int count = 0;
        while(longIterator.hasNext()) {
            long ts = longIterator.next();
            if (failedTime - ts > (maxDuration * 1000)) {
                longIterator.remove();
            } else if ((failedTime - ts) <= (duration * 1000)) {
                count++;
            }
        }

        boolean need = count >= retryTimes;
        if (need) {
            removeFromFailureRecords(host);
        }
        return need;
    }

    public static void removeFromFailureRecords(HostAddress host) {
        if (host != null) {
            failureRecords.remove(host);
        }
    }

    public static void updateMaxDuration(long newDuration) {
        maxDuration = Math.max(newDuration, maxDuration);
    }

}
