/*
 *
 * OceanBase Client for Java
 *
 * Copyright (c) 2021 OceanBase.
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 *
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * Copyright (c) 2009-2011, Marcus Eriksson
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 * Redistributions of source code must retain the above copyright notice, this list
 * of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice, this
 * list of conditions and the following disclaimer in the documentation and/or
 * other materials provided with the distribution.
 *
 * Neither the name of the driver nor the names of its contributors may not be
 * used to endorse or promote products derived from this software without specific
 * prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS  AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 * NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
 * OF SUCH DAMAGE.
 *
 */
package com.oceanbase.jdbc.internal.com.read.resultset;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.oceanbase.jdbc.ServerSidePreparedStatement;
import com.oceanbase.jdbc.internal.com.Packet;
import com.oceanbase.jdbc.internal.com.read.Buffer;
import com.oceanbase.jdbc.internal.com.read.dao.Results;
import com.oceanbase.jdbc.internal.protocol.Protocol;
import com.oceanbase.jdbc.internal.util.exceptions.ExceptionFactory;

public class CursorResultSet extends Cursor {

    private int       statementId;
    /***
     * When COM_STMT_PREPARE_EXECUTE protocol is supported
     * and URL option "useCursorOffset" which controls OB_Oracle_Fetch protocol are activated,
     * a scrollable cursor is used on server side.
     */
    protected boolean isServerSide     = false;
    /***
     * Mark the absolute position in entire result for the first row in cache.
     * The absolute position of the first row in entire result is 1.
     */
    private int       clientStartIndex = 0;
    /***
     * Mark the absolute position in entire Result for the last row in cache.
     */
    private int       clientEndIndex   = 0;
    /***
     * Mark the absolute position in entire result for the current row in cache.
     */
    private int       currentIndex     = 0;
    /***
     * Mark the absolute position in entire result for the target row,
     * which is going to be fetched next time.
     */
    private int       fetchIndex       = 0;
    /***
     * Mark the absolute position of the last row in entire result.
     */
    private int       lastRowIndex     = 0;

    public CursorResultSet(ColumnDefinition[] columnsInformation, Results results,
                           Protocol protocol, boolean callableResult, boolean eofDeprecated,
                           boolean isPsOutParamter) throws IOException, SQLException {
        super(columnsInformation, results, protocol, callableResult, eofDeprecated, isPsOutParamter);
        this.statementId = results.getStatementId();
        if (protocol.supportFetchWithOffset()) {
            if (resultSetScrollType == ResultSet.TYPE_FORWARD_ONLY) {
                isServerSide = true;
            } else if (protocol.supportStmtPrepareExecute()) {
                // serverside scrollable cursor rely on new PS protocol, in which EXECUTE_MODE is set as OCI_STMT_SCROLLABLE_READONLY
                isServerSide = true;
            }
            if (isServerSide && dataSize > 0) {
                clientStartIndex = 1;
                clientEndIndex = dataSize;
                setLastRowIndex();
            }
        }
    }

    private void setLastRowIndex() {
        if (isLastRowSent && lastRowIndex != clientEndIndex) {
            lastRowIndex = clientEndIndex;
        }
    }

    protected void cursorFetch() throws SQLException {
        if (isLastRowSent) {
            return;
        }
        try {
            ((ServerSidePreparedStatement) this.getStatement()).cursorFetch(this.statementId,
                this.getFetchSize());
        } catch (SQLException e) {
            if ("ORA-01002: fetch out of sequence".equals(e.getMessage())) {
                isLastRowSent = true;
            }
            throw e;
        }
        try {
            getCursorFetchData(fetchSize);
        } catch (IOException e) {
            handleIoException(e);
        }
    }

    private void cursorFetchForOracle(byte offsetType, int offset) throws SQLException {
        try {
            ((ServerSidePreparedStatement) this.getStatement()).cursorFetchForOracle(
                this.statementId, this.getFetchSize(), offsetType, offset);
        } catch (SQLException e) {
            if ("ORA-01002: fetch out of sequence".equals(e.getMessage())) {
                isLastRowSent = true;
                return;
            } else {
                throw e;
            }
        }
        try {
            getCursorFetchData(fetchSize);
        } catch (IOException e) {
            handleIoException(e);
        }
    }

    protected void getCursorFetchData(int tmpFetchSize) throws IOException, SQLException {
        if (resultSetScrollType == ResultSet.TYPE_FORWARD_ONLY) {
            lastRowPointer = -1;

            // this resultSet must be CURSOR , and if it has no need to get previous value
            if (dataSize > 0) {
                discardedRows += dataSize;
                dataSize = 0;
            }
            super.resetState();
        }
        // client side CURSOR ResultSet caches all rows step by step
        if (isServerSide) {
            dataSize = 0;
            lastRowPointer = -1;
        } else if (dataSize == -1) {
            dataSize = 0;
        }

        // read only fetchSize rows and an EOF packet
        while (tmpFetchSize >= 0 && super.readNextValue()) {
            tmpFetchSize--;
        }

        if (isServerSide) {
            // there is an OK packet at the end of OB Oracle Fetch Response
            Buffer buffer = reader.getPacket(true);
            if (buffer.getByteAt(0) != Packet.OK) {
                throw ExceptionFactory.INSTANCE
                    .create("expected OK packet at the end of FETCH Response not found");
            }
            buffer.skipByte(); // header
            final long rowCount = buffer.getLengthEncodedNumeric();
            //            final long lastInsertId = buffer.getLengthEncodedNumeric();
            //            short serverStatus = buffer.readShort();
            //            short warnings = buffer.readShort();

            clientEndIndex = (int) rowCount;
            clientStartIndex = (int) (rowCount - dataSize + 1);

            setLastRowIndex();
        }
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        checkClose();
        if (protocol.isOracleMode() && dataSize <= 0) {
            return false;
        }

        if (!isServerSide) {
            return rowPointer == -1;
        }
        return currentIndex == 0;
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        checkClose();
        if (protocol.isOracleMode() && dataSize <= 0) {
            return false;
        }

        if (!isServerSide) {
            return isLastRowSent && rowPointer >= dataSize;
        }
        return currentIndex == -1;
    }

    @Override
    public boolean isFirst() throws SQLException {
        checkClose();
        if (dataSize <= 0) {
            return false;
        }

        if (!isServerSide) {
            return rowPointer == 0;
        }
        return currentIndex == 1;
    }

    @Override
    public boolean isLast() throws SQLException {
        checkClose();
        if (resultSetScrollType == TYPE_FORWARD_ONLY && getProtocol().isOracleMode()) {
            throw new SQLException(
                "Invalid operation on TYPE_FORWARD_ONLY CURSOR ResultSet for oracle mode");
        }
        if (dataSize <= 0) {
            return false;
        }

        if (!isServerSide) {
            return isLastRowSent && rowPointer == dataSize - 1;
        }
        return isLastRowSent && currentIndex == lastRowIndex;
    }

    @Override
    public void beforeFirst() throws SQLException {
        checkClose();
        if (resultSetScrollType == TYPE_FORWARD_ONLY) {
            throw new SQLException("Invalid operation on TYPE_FORWARD_ONLY CURSOR ResultSet");
        }
        cancelRowInserts();

        if (!isServerSide) {
            rowPointer = -1;
        } else {
            rowPointer = -1;
            currentIndex = 0;
        }
    }

    @Override
    public void afterLast() throws SQLException {
        checkClose();
        if (resultSetScrollType == TYPE_FORWARD_ONLY) {
            throw new SQLException("Invalid operation on TYPE_FORWARD_ONLY CURSOR ResultSet");
        }
        cancelRowInserts();

        if (dataSize == 0) {
            return;
        }

        if (!isServerSide) {
            while (!isLastRowSent) {
                cursorFetch();
            }
            rowPointer = dataSize;
        } else {
            rowPointer = dataSize;
            currentIndex = -1; // -1 marks the position after last
        }
    }

    @Override
    public boolean first() throws SQLException {
        checkClose();
        if (resultSetScrollType == TYPE_FORWARD_ONLY) {
            throw new SQLException("Invalid operation on TYPE_FORWARD_ONLY CURSOR ResultSet");
        }
        cancelRowInserts();

        if (dataSize == 0) {
            return false;
        }

        beforeFirst();
        return next();
    }

    @Override
    public boolean last() throws SQLException {
        checkClose();
        if (resultSetScrollType == TYPE_FORWARD_ONLY) {
            throw new SQLException("Invalid operation on TYPE_FORWARD_ONLY CURSOR ResultSet");
        }
        cancelRowInserts();

        if (dataSize == 0) {
            return false;
        }

        if (!isServerSide) {
            while (!isLastRowSent) {
                cursorFetch();
            }
            rowPointer = dataSize - 1;
            return dataSize > 0;
        }

        if (!isLastRowSent) {
            cursorFetchForOracle(Packet.OCI_FETCH_LAST, 0);
            if (clientStartIndex != clientEndIndex) {
                throw new SQLException(
                    "clientStartIndex is supposed to equal to clientEndIndex, but actually clientStartIndex is "
                            + clientStartIndex + " and clientEndIndex is " + clientEndIndex);
            }
        }
        rowPointer = dataSize - 1;
        currentIndex = lastRowIndex;
        return true;
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        checkClose();
        if (resultSetScrollType == TYPE_FORWARD_ONLY) {
            throw new SQLException(
                "Invalid operation on TYPE_FORWARD_ONLY CURSOR ResultSet: absolute");
        }
        if (row == 0 && protocol.isOracleMode()) {
            // Compatible with Oracle
            throw new SQLException("Invalid parameter: absolute(0)");
        }
        cancelRowInserts();

        if (dataSize == 0) {
            return false;
        }

        if (!isServerSide) {
            if (row > 0) {
                while (!isLastRowSent && row > dataSize) {
                    cursorFetch();
                }
                if (row <= dataSize) {
                    rowPointer = row - 1;
                    return true;
                }
                afterLast();
                return false;
            }

            last(); // absolute position reverse from tail of resultSet
            if (row >= -dataSize) {
                rowPointer = dataSize + row;
                return true;
            }
            beforeFirst();
            return false;
        }

        if (row > 0) {
            fetchIndex = row;
        } else {
            if (lastRowIndex == 0 && !last()) {
                return false;
            }
            fetchIndex = lastRowIndex + row + 1;
        }
        return fetchAbsoluteRow(fetchIndex);
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        checkClose();
        if (resultSetScrollType == TYPE_FORWARD_ONLY) {
            throw new SQLException("Invalid operation on TYPE_FORWARD_ONLY CURSOR ResultSet");
        }
        cancelRowInserts();

        if (dataSize == 0) {
            return false;
        }

        if (!isServerSide) {
            int newPos = rowPointer + rows;
            if (newPos < 0) {
                beforeFirst();
                return false;
            }
            return absolute(newPos + 1);
        }

        if (isAfterLast() && rows >= 0 || isBeforeFirst() && rows <= 0) {
            return false;
        }
        if (isLast() && rows > 0) {
            afterLast();
            return false;
        }
        if (isFirst() && rows < 0) {
            beforeFirst();
            return false;
        }
        if (isAfterLast() && rows < 0) {
            if (!last()) {
                return false;
            }
            return absolute(currentIndex + rows + 1);
        }
        return absolute(currentIndex + rows);
    }

    @Override
    public boolean previous() throws SQLException {
        checkClose();
        if (resultSetScrollType == TYPE_FORWARD_ONLY) {
            throw new SQLException("Invalid operation on TYPE_FORWARD_ONLY CURSOR ResultSet");
        }
        cancelRowInserts();

        if (dataSize == 0) {
            return false;
        }

        if (!isServerSide) {
            if (rowPointer > -1) {
                rowPointer--;
                return rowPointer > -1;
            }
            return false;
        }

        if (isAfterLast()) {
            return last();
        }
        fetchIndex = currentIndex - 1;
        return fetchAbsoluteRow(fetchIndex);
    }

    @Override
    public boolean next() throws SQLException {
        checkClose();
        cancelRowInserts();

        if (dataSize == 0) {
            return false;
        }

        if (!isServerSide) {
            if (rowPointer < dataSize - 1) {
                rowPointer++;
                return true;
            }
            if (isLastRowSent) {
                rowPointer = dataSize;
                return false;
            }

            cursorFetch();
            rowPointer++;
            return dataSize > rowPointer;
        }

        if (isAfterLast()) {
            return false;
        }
        fetchIndex = currentIndex + 1;
        return fetchAbsoluteRow(fetchIndex);
    }

    @Override
    public int getRow() throws SQLException {
        checkClose();
        if (!isServerSide) {
            return super.getRow();
        }

        return currentIndex;
    }

    private boolean fetchAbsoluteRow(int fetchIndex) throws SQLException {
        if (fetchIndex < 1) {
            rowPointer = -1;
            currentIndex = 0;
            return false;
        }
        if (lastRowIndex < fetchIndex && lastRowIndex > 0) {
            rowPointer = dataSize;
            currentIndex = -1; // -1 marks the position after last
            return false;
        }
        if (clientStartIndex <= fetchIndex && fetchIndex <= clientEndIndex) {
            rowPointer = fetchIndex - clientStartIndex;
            currentIndex = fetchIndex;
            return true;
        }
        try {
            cursorFetchForOracle(Packet.OCI_FETCH_ABSOLUTE, fetchIndex);
            rowPointer = 0;
            currentIndex = clientStartIndex;
            return dataSize > 0;
        } catch (Exception ex) {
            return false;
        }
    }

}
