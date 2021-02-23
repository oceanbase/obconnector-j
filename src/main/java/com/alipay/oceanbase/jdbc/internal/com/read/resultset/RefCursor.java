package com.alipay.oceanbase.jdbc.internal.com.read.resultset;

import com.alipay.oceanbase.jdbc.ServerSidePreparedStatement;
import com.alipay.oceanbase.jdbc.extend.datatype.RowObCursorData;
import com.alipay.oceanbase.jdbc.internal.com.read.dao.Results;
import com.alipay.oceanbase.jdbc.internal.com.read.resultset.rowprotocol.BinaryRowProtocol;
import com.alipay.oceanbase.jdbc.internal.protocol.Protocol;

import java.io.IOException;
import java.sql.SQLException;

public class RefCursor extends SelectResultSet {
    private RowObCursorData rowObCursorData;
    private boolean         isLastRow = false;

    public RefCursor(ColumnDefinition[] columnsInformation, Results results, Protocol protocol,
                     boolean callableResult, boolean eofDeprecated, boolean isPsOutParamter,
                     RowObCursorData rowObCursorData) throws IOException, SQLException {
        super(columnsInformation, results, protocol, callableResult, eofDeprecated, isPsOutParamter);
        this.rowObCursorData = rowObCursorData;
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
        if (isLastRow) {
            return;
        }
        int numToFetch = this.getFetchSize();
        ColumnDefinition[] ci = new ColumnDefinition[0];
        try {
            ci = ((ServerSidePreparedStatement) this.getStatement()).cursorFetch(
                this.rowObCursorData.getCursorId(), numToFetch);
        } catch (SQLException e) {
            if (e.getMessage().equals("ORA-01002: fetch out of sequence")) {
                isLastRow = true;
                return;
            } else {
                throw e;
            }
        }
        if (ci != null) {
            this.columnsInformation = ci;
            this.columnInformationLength = ci.length;
            this.row = new BinaryRowProtocol(columnsInformation, columnInformationLength, this
                .getStatement().getMaxFieldSize(), options);
        }
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

    @Override
    public void close() throws SQLException {
        rowObCursorData.setOpen(false);
    }
}
