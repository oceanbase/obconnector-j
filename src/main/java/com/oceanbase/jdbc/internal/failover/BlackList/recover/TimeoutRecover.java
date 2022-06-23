package com.oceanbase.jdbc.internal.failover.BlackList.recover;

import java.util.concurrent.TimeUnit;

import com.oceanbase.jdbc.internal.failover.BlackList.recover.RemoveStrategy;
import com.oceanbase.jdbc.internal.failover.utils.HostStateInfo;

public class TimeoutRecover implements RemoveStrategy {
    long timeout;

    public TimeoutRecover() {
    }

    @Override
    public String toString() {
        return "TimeoutRecover{}";
    }

    public long getTimeout() {
        return timeout;
    }

    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    @Override
    public boolean needToChangeStateInfo(HostStateInfo hostStateInfo) {
        long currentTimeNanos = System.nanoTime();
        long entryNanos = hostStateInfo.getTimestamp();
        long durationSeconds = TimeUnit.NANOSECONDS.toSeconds(currentTimeNanos - entryNanos);
        if (durationSeconds >= timeout) {
            return true;
        } else {
            return false;
        }
    }
}
