package com.alipay.oceanbase.jdbc.internal.com.read.resultset.rowprotocol;

import com.alipay.oceanbase.jdbc.OceanBaseConnection;
import com.alipay.oceanbase.jdbc.OceanBaseResultSetMetaData;
import com.alipay.oceanbase.jdbc.extend.datatype.RowObCursorData;
import com.alipay.oceanbase.jdbc.internal.com.read.dao.Results;
import com.alipay.oceanbase.jdbc.internal.com.read.resultset.ColumnDefinition;
import com.alipay.oceanbase.jdbc.internal.com.read.resultset.SelectResultSet;
import com.alipay.oceanbase.jdbc.internal.protocol.Protocol;

import java.sql.*;
import java.util.ArrayList;

public class RowDataObCursor extends SelectResultSet {
    /**
     * Creates a new cursor-backed row provider.
     *
     * @param ioChannel         connection to the server.
     * @param creatingStatement statement that opened the cursor.
     * @param metadata
     */
    private OceanBaseConnection connection;            // todo
    RowDataCursor               dataCursor;
    boolean                     isOpen         = false;
    int                         cursorId;
    boolean                     initialed      = false;
    Protocol                    cursorProtocol;
    Statement                   cursorStatement;
    RowObCursorData             obCursorData;
    int                         currentPointer = 0;
    int                         total          = 0;

    //    private RowObCursorData rowObCursorData;

    //    public void setRowObCursorData(RowObCursorData rowObCursorData) {
    //        this.rowObCursorData = rowObCursorData;
    //    }
    // todo

    /**
     * The result set that we 'belong' to.
     */

    private void notSupported() throws SQLException {
        throw new OperationNotSupportedException();
    }

    /**
     * Field-level metadata from the server. We need this, because it is not
     * sent for each batch of rows, but we need the metadata to unpack the
     * results for each field.
     */

    public RowDataObCursor(ColumnDefinition[] columnDefinition,Protocol protocol, RowObCursorData obCursorData ,Statement creatorStmt)
            throws SQLException {
        super(columnDefinition, new ArrayList<>(), protocol, TYPE_SCROLL_SENSITIVE);
        dataCursor = new RowDataCursor(protocol, creatorStmt,columnDefinition);
        this.cursorProtocol = protocol;
        this.cursorStatement = creatorStmt;
        isOpen = true;
        this.cursorId = obCursorData.getCursorId();
        this.obCursorData = obCursorData;
    }

    public void setOwner(SelectResultSet rs) {
        dataCursor.setOwner(rs);
    }

    public ResultSetMetaData getMetaData() {
        return new OceanBaseResultSetMetaData(columnsInformation, options, forceAlias,
            this.cursorProtocol.isOracleMode());
    }

    @Override
    public void setFetchSize(int fetchSize) {
        dataCursor.setNumRowsToFetch(fetchSize);
    }

    @Override
    public void close() throws SQLException {
        obCursorData.setOpen(false);
        isOpen = false;
        super.close();
    }

    private boolean addData() throws SQLException {
        byte[] buffer = dataCursor.next();
        columnsInformation = dataCursor.getMetadata();
        columnInformationLength = columnsInformation.length;
        this.changeRowProtocol(new BinaryRowProtocol(columnsInformation, columnsInformation.length,
            cursorStatement.getMaxFieldSize(), cursorProtocol.getOptions()));
        if (buffer == null) {
            return false;
        }
        this.addRowData(buffer);
        total++;
        return true;
    }

    @Override
    public boolean next() throws SQLException {
        if (!isOpen) {
            return false;
        }
        boolean ret = addData();
        if (!ret && currentPointer == total - 1) {
            return false;
        } else {
            ret = true;
        }
        currentPointer++;
        rowPointer = currentPointer;
        return ret;
    }

    public boolean init() throws SQLException {
        if (!isOpen) {
            return false;
        }
        currentPointer = -1;
        return addData();
    }

    public void setRowObCursorData(RowObCursorData obCursorData) throws SQLException {
        cursorId = obCursorData.getCursorId();
        isOpen = obCursorData.isOpen();
        dataCursor.setCursorId(cursorId);
        if (!initialed) {
            //todo
            init();
            initialed = true;

        }
    }

    protected RowDataObCursor getComplexCursor(int columnIndex) throws SQLException {
        if (!isOpen) {
            throw new SQLException("cursor is not open");
        }
        return super.getComplexCursor(columnIndex);
    }
}
