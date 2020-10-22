package com.alipay.oceanbase.jdbc.internal.com.read.resultset.rowprotocol;

import com.alipay.oceanbase.jdbc.ServerSidePreparedStatement;
import com.alipay.oceanbase.jdbc.internal.com.read.Buffer;
import com.alipay.oceanbase.jdbc.internal.com.read.resultset.ColumnDefinition;
import com.alipay.oceanbase.jdbc.internal.com.read.resultset.SelectResultSet;
import com.alipay.oceanbase.jdbc.internal.protocol.Protocol;
import com.sun.org.apache.bcel.internal.generic.Select;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Model for result set data backed by a cursor. Only works for forward-only result sets (but still works with updatable concurrency).
 */
public class RowDataCursor {

    private final static int   BEFORE_START_OF_ROWS          = -1;

    private int                currentPositionInEntireResult = BEFORE_START_OF_ROWS;

    /**
     * Position in cache of rows, used to determine if we need to fetch more
     * rows from the server to satisfy a request for the next row.
     */
    private int                currentPositionInFetchedRows  = BEFORE_START_OF_ROWS;

    /**
     * The result set that we 'belong' to.
     */
    private SelectResultSet    owner;

    private SelectResultSet    curentResultSet               = null;

    /**
     * Have we been told from the server that we have seen the last row?
     */
    private boolean            lastRowFetched                = false;

    /**
     * Field-level metadata from the server. We need this, because it is not
     * sent for each batch of rows, but we need the metadata to unpack the
     * results for each field.
     */
    private ColumnDefinition[] metadata;

    /**
     * Communications channel to the server
     */
    private Protocol           protocol;

    public long getStatementIdOnServer() {
        return statementIdOnServer;
    }

    /**
     * Identifier for the statement that created this cursor.
     */
    private long             statementIdOnServer;

    /**
     * The prepared statement that created this cursor.
     */
    private Statement        prepStmt;

    /**
     * The server status for 'last-row-sent'...This might belong in mysqldefs,
     * but it it only ever referenced from here.
     */
    private static final int SERVER_STATUS_LAST_ROW_SENT = 128;

    /**firstFetchCompleted
     * Have we attempted to fetch any rows yet?
     */
    private boolean          firstFetchCompleted         = false;

    private boolean          wasEmpty                    = false;

    private boolean          is_close                    = false;

    public void setNumRowsToFetch(int numRowsToFetch) {
        this.numRowsToFetch = numRowsToFetch;
    }

    private int numRowsToFetch = 5;
    private int cursorId       = 0;

    /**
     * Creates a new cursor-backed row provider.
     *
     * @param protocol
     *            connection to the server.
     * @param creatingStatement
     *            statement that opened the cursor.
     * @param metadata
     *            field-level metadata for the results that this cursor covers.
     */
    public RowDataCursor(Protocol protocol, Statement creatingStatement, ColumnDefinition[] metadata) {
        this.currentPositionInEntireResult = BEFORE_START_OF_ROWS;
        this.metadata = metadata;
        this.protocol = protocol;
        //        this.statementIdOnServer = creatingStatement();
        this.prepStmt = creatingStatement;

    }

    /**
     *
     * @param protocol
     * @param cursorId
     */
    public RowDataCursor(Protocol protocol, long cursorId) {
        this.currentPositionInEntireResult = BEFORE_START_OF_ROWS;
        this.protocol = protocol;
        this.statementIdOnServer = cursorId;

    }

    public void setCursorId(int cursorId) {
        this.cursorId = cursorId;
    }

    /** todo
     * Returns true if we got the last element.
     */
    public boolean isAfterLast() {
        //        return this.lastRowFetched && this.currentPositionInFetchedRows > this.fetchedRows.size();
        return false;
    }

    /**
     * Only works on non dynamic result sets.
     *
     * @param index
     *            row number to get at
     * @return row data at index
     * @throws SQLException
     *             if a database error occurs
     */
    public byte[] getAt(int index) throws SQLException {
        notSupported();

        return null;
    }

    /**
     * Returns if iteration has not occured yet.
     *
     * @return true if before first row
     * @throws SQLException
     *             if a database error occurs
     */
    public boolean isBeforeFirst() throws SQLException {
        return this.currentPositionInEntireResult < 0;
    }

    /**
     * Moves the current position in the result set to the given row number.
     *
     * @param rowNumber
     *            row to move to
     * @throws SQLException
     *             if a database error occurs
     */
    public void setCurrentRow(int rowNumber) throws SQLException {
        notSupported();
    }

    /**
     * Returns the current position in the result set as a row number.
     *
     * @return the current row number
     * @throws SQLException
     *             if a database error occurs
     */
    public int getCurrentRowNumber() throws SQLException {
        return this.currentPositionInEntireResult + 1;
    }

    /**
     * Returns true if the result set is dynamic.
     *
     * This means that move back and move forward won't work because we do not
     * hold on to the records.
     *
     * @return true if this result set is streaming from the server
     */
    public boolean isDynamic() {
        return true;
    }

    /**
     * Has no records.
     *
     * @return true if no records
     * @throws SQLException
     *             if a database error occurs
     */
    public boolean isEmpty() throws SQLException {
        return this.isBeforeFirst() && this.isAfterLast();
    }

    /**
     * Are we on the first row of the result set?
     *
     * @return true if on first row
     * @throws SQLException
     *             if a database error occurs
     */
    public boolean isFirst() throws SQLException {
        return this.currentPositionInEntireResult == 0;
    }

    /**
     * Are we on the last row of the result set?
     *
     * @return true if on last row
     * @throws SQLException
     *             if a database error occurs
     */
    // todo
    public boolean isLast() throws SQLException {
        //        return this.lastRowFetched
        //                && this.currentPositionInFetchedRows == (this.fetchedRows.size() - 1);
        return false;
    }

    /**
     * Moves to after last.
     *
     * @throws SQLException
     *             if a database error occurs
     */
    public void afterLast() throws SQLException {
        notSupported();
    }

    /**
     * Moves to before first.
     *
     * @throws SQLException
     *             if a database error occurs
     */
    public void beforeFirst() throws SQLException {
        notSupported();
    }

    /**
     * Moves to before last so next el is the last el.
     *
     * @throws SQLException
     *             if a database error occurs
     */
    public void beforeLast() throws SQLException {
        notSupported();
    }

    /**
     * We're done.
     *
     * @throws SQLException
     *             if a database error occurs
     */
    public void close() throws SQLException {

        this.metadata = null;
        this.owner = null;
    }

    /**
     * Moves the current position relative 'rows' from the current position.
     *
     * @param rows
     *            the relative number of rows to move
     * @throws SQLException
     *             if a database error occurs
     */
    public void moveRowRelative(int rows) throws SQLException {
        notSupported();
    }

    /**
     * Returns the next row.
     *
     * @return the next row value
     * @throws SQLException
     *             if a database error occurs
     */
    public byte[] next() throws SQLException {
        if (!this.firstFetchCompleted) {
            fetchMoreRows();
        }
        if (!curentResultSet.next()) {
            fetchMoreRows();
            if (!curentResultSet.next())
                return null;
        }
        return curentResultSet.getCurrentRowData();
    }

    /**
     */
    private void fetchMoreRows() throws SQLException {
        if (!this.firstFetchCompleted) {
            this.firstFetchCompleted = true;
            numRowsToFetch = this.numRowsToFetch;

        } else {
            int numRowsToFetch = this.owner.getFetchSize();

            if (numRowsToFetch == 0 && this.prepStmt != null) {
                numRowsToFetch = this.prepStmt.getFetchSize();
            }

            if (numRowsToFetch == Integer.MIN_VALUE) {
                // Handle the case where the user used 'old' streaming result sets

                numRowsToFetch = 1;
            }
            if (numRowsToFetch == 0) {
                numRowsToFetch = this.numRowsToFetch;
            }
        }
        Buffer packet;
        curentResultSet = ((ServerSidePreparedStatement) prepStmt).cursorFetch(cursorId,
            numRowsToFetch);
        this.metadata = curentResultSet.getColumnsInformation();
    }

    /**
     * Removes the row at the given index.
     *
     * @param
     *            ind row to move to
     * @throws SQLException
     *             if a database error occurs
     */
    public void removeRow(int ind) throws SQLException {
        notSupported();
    }

    /**
     * Only works on non dynamic result sets.
     *
     * @return the size of this row data
     */
    public int size() {
        //        return RESULT_SET_SIZE_UNKNOWN; todo
        return 0;
    }

    protected void nextRecord() throws SQLException {

    }

    private void notSupported() throws SQLException {
        throw new OperationNotSupportedException();
    }

    /*
     * (non-Javadoc)
     *
     * @see com.alipay.oceanbase.jdbc.RowProvider#setOwner(com.alipay.oceanbase.jdbc.ResultSet)
     */
    public void setOwner(SelectResultSet rs) {
        this.owner = rs;
    }

    /*
     * (non-Javadoc)
     *
     * @see com.alipay.oceanbase.jdbc.RowProvider#getOwner()
     */
    public SelectResultSet getOwner() {
        return this.owner;
    }

    public boolean wasEmpty() {
        return this.wasEmpty;
    }

    public void setMetadata(ColumnDefinition[] metadata) {
        this.metadata = metadata;
    }

    public ColumnDefinition[] getMetadata() {
        return this.metadata;
    }
}
