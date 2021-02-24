package com.alipay.oceanbase.jdbc.internal.com.read.resultset.rowprotocol;

import com.alipay.oceanbase.jdbc.internal.util.SqlStates;
import com.alipay.oceanbase.jdbc.internal.util.constant.Messages;

import java.sql.SQLException;

class OperationNotSupportedException extends SQLException {

    static final long serialVersionUID = 474918612056813430L;

    OperationNotSupportedException() {
        super(Messages.getString("RowDataDynamic.10"), SqlStates.SQL_STATE_ILLEGAL_ARGUMENT
            .getSqlState());
    }
}