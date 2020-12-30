package com.alipay.oceanbase.jdbc.internal.util.exceptions;

import java.sql.SQLException;

public class OceanBaseSqlException extends SQLException {

    private String sql;
    private String threadName   = null;
    private String deadLockInfo = null;

    public OceanBaseSqlException(String reason, String sql, Throwable cause) {
        super(reason, cause);
        this.sql = sql;
    }

    public OceanBaseSqlException(String reason, String sql, String sqlState, Throwable cause) {
        super(reason, sqlState, cause);
        this.sql = sql;
    }

    public OceanBaseSqlException(String reason, String sql, String sqlState, int vendorCode,
                                 Throwable cause) {
        super(reason, sqlState, vendorCode, cause);
        this.sql = sql;
    }

    public static OceanBaseSqlException of(SQLException cause, String sql) {
        return new OceanBaseSqlException(cause.getMessage().contains("\n") ? cause.getMessage()
            .substring(0, cause.getMessage().indexOf("\n")) : cause.getMessage(), sql,
            cause.getSQLState(), cause.getErrorCode(), cause);
    }

    public OceanBaseSqlException withThreadName(String threadName) {
        this.threadName = threadName;
        return this;
    }

    public OceanBaseSqlException withDeadLockInfo(String deadLockInfo) {
        this.deadLockInfo = deadLockInfo;
        return this;
    }

    public String getSql() {
        return sql;
    }

    public String getThreadName() {
        return threadName;
    }

    public String getDeadLockInfo() {
        return deadLockInfo;
    }
}
