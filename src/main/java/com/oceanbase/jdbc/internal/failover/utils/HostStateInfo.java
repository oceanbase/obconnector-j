package com.oceanbase.jdbc.internal.failover.utils;

public class HostStateInfo {
    public enum STATE {
        GREY,
        BLACK
    }

    STATE state;
    Long  timestamp;

    public HostStateInfo() {
        state = STATE.BLACK;
        timestamp = System.nanoTime();
    }

    public HostStateInfo(STATE state, Long timestamp) {
        this.state = state;
        this.timestamp = timestamp;
    }

    public STATE getState() {
        return state;
    }

    public void setState(STATE state) {
        this.state = state;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "HostStateInfo{" + "state=" + state + ", timestamp=" + timestamp + '}';
    }
}
