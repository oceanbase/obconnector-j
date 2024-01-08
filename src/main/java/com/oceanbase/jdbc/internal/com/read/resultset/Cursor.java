package com.oceanbase.jdbc.internal.com.read.resultset;

import java.io.IOException;
import java.sql.SQLException;

import com.oceanbase.jdbc.internal.com.read.dao.Results;
import com.oceanbase.jdbc.internal.protocol.Protocol;

public abstract class Cursor extends SelectResultSet {
    public Cursor(ColumnDefinition[] columnDefinition, Results results, Protocol protocol,
                  boolean callableResult, boolean eofDeprecated, boolean isPsOutParameter)
                                                                                          throws IOException,
                                                                                          SQLException {
        super(columnDefinition, results, protocol, callableResult, eofDeprecated, isPsOutParameter);
    }

    protected abstract boolean cursorFetch() throws SQLException;

}
