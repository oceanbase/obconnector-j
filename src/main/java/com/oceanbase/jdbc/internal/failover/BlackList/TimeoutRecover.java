package com.oceanbase.jdbc.internal.failover.BlackList;

public class TimeoutRecover implements RemoveStrategy {
    long timeout;

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

    public TimeoutRecover() {
    }
}
