package com.alipay.oceanbase.jdbc.internal.util.dao;

import java.sql.SQLException;

public class ReconnectDuringTransactionException extends SQLException {

    public ReconnectDuringTransactionException(String message, int errorCode, String sqlState) {
        super(message, sqlState, errorCode);
    }
}
