package com.oceanbase.jdbc.internal.failover.BlackList;

public class RetryDuration implements AppendStrategy {
    int retryTimes;
    int duration;

    @Override
    public String toString() {
        return "RetryDuration{}";
    }

    public int getRetryTimes() {
        return retryTimes;
    }

    public void setRetryTimes(int retryTimes) {
        this.retryTimes = retryTimes;
    }

    public int getDuration() {
        return duration;
    }

    public void setDuration(int duration) {
        this.duration = duration;
    }

    public RetryDuration() {
    }
}
