package com.alipay.oceanbase.jdbc.internal.util.pool;

import java.sql.Connection;

public class GlobalStateInfo {

    private final long    maxAllowedPacket;
    private final int     waitTimeout;
    private final boolean autocommit;
    private final int     autoIncrementIncrement;
    private final String  timeZone;
    private final String  systemTimeZone;
    private final int     defaultTransactionIsolation;

    /** Default value. ! To be used for Connection that will only Kill query/connection ! */
    public GlobalStateInfo() {
    this.maxAllowedPacket = 1_000_000;
    this.waitTimeout = 28_800;
    this.autocommit = true;
    this.autoIncrementIncrement = 1;
    this.timeZone = "+00:00";
    this.systemTimeZone = "+00:00";
    this.defaultTransactionIsolation = Connection.TRANSACTION_REPEATABLE_READ;
  }

    /**
     * Storing global server state to avoid asking server each new connection. Using this Object
     * meaning having set the option "staticGlobal". Application must not change any of the following
     * options.
     *
     * @param maxAllowedPacket max_allowed_packet global variable value
     * @param waitTimeout wait_timeout global variable value
     * @param autocommit auto_commit global variable value
     * @param autoIncrementIncrement auto_increment_increment global variable value
     * @param timeZone time_zone global variable value
     * @param systemTimeZone System global variable value
     * @param defaultTransactionIsolation tx_isolation variable value
     */
    public GlobalStateInfo(long maxAllowedPacket, int waitTimeout, boolean autocommit,
                           int autoIncrementIncrement, String timeZone, String systemTimeZone,
                           int defaultTransactionIsolation) {
        this.maxAllowedPacket = maxAllowedPacket;
        this.waitTimeout = waitTimeout;
        this.autocommit = autocommit;
        this.autoIncrementIncrement = autoIncrementIncrement;
        this.timeZone = timeZone;
        this.systemTimeZone = systemTimeZone;
        this.defaultTransactionIsolation = defaultTransactionIsolation;
    }

    public long getMaxAllowedPacket() {
        return maxAllowedPacket;
    }

    public int getWaitTimeout() {
        return waitTimeout;
    }

    public boolean isAutocommit() {
        return autocommit;
    }

    public int getAutoIncrementIncrement() {
        return autoIncrementIncrement;
    }

    public String getTimeZone() {
        return timeZone;
    }

    public String getSystemTimeZone() {
        return systemTimeZone;
    }

    public int getDefaultTransactionIsolation() {
        return defaultTransactionIsolation;
    }
}
