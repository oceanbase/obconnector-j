package com.alipay.oceanbase.jdbc.internal.com.read.resultset;

import com.alipay.oceanbase.jdbc.ServerSidePreparedStatement;
import com.alipay.oceanbase.jdbc.internal.com.read.dao.Results;
import com.alipay.oceanbase.jdbc.internal.protocol.Protocol;

import java.io.IOException;
import java.sql.SQLException;

public class CursorResultSet extends SelectResultSet {
    int statementId;

    public CursorResultSet(ColumnDefinition[] columnsInformation, Results results,
                           Protocol protocol, boolean callableResult, boolean eofDeprecated,
                           boolean isPsOutParamter) throws IOException, SQLException {
        super(columnsInformation, results, protocol, callableResult, eofDeprecated, isPsOutParamter);
        this.statementId = results.getStatementId();
    }

    @Override
    public boolean next() throws SQLException {
        if (super.next()) {
            return true;
        } else {
            super.resetState();
            cursorFetch();
            return super.next();
        }
    }

    private void cursorFetch() throws SQLException {
        int numToFetch = this.getFetchSize();
        ((ServerSidePreparedStatement) this.getStatement()).cursorFetch2(this.statementId,
            numToFetch);
        try {
            getCursorFetchData();
        } catch (IOException e) {
            handleIoException(e);
        }
    }

    private void getCursorFetchData() throws IOException, SQLException {
        // read only fetchSize values
        int fetchSizeTmp = this.getFetchSize();
        while (fetchSizeTmp >= 0 && super.readNextValue()) {
            fetchSizeTmp--;
        }
    }

    @Override
    public boolean last() throws SQLException {
        throw new SQLException("Operation not supported for streaming result set");
    }

    @Override
    public void beforeFirst() throws SQLException {
        throw new SQLException("Operation not supported for streaming result set");
    }

}
