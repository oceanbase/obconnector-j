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
import java.sql.SQLException;

import com.oceanbase.jdbc.ServerSidePreparedStatement;
import com.oceanbase.jdbc.internal.com.read.dao.Results;
import com.oceanbase.jdbc.internal.protocol.Protocol;

public class CursorResultSet extends SelectResultSet {

    int statementId;

    public CursorResultSet(ColumnDefinition[] columnsInformation, Results results,
                           Protocol protocol, boolean callableResult, boolean eofDeprecated,
                           boolean isPsOutParamter) throws IOException, SQLException {
        super(columnsInformation, results, protocol, callableResult, eofDeprecated, isPsOutParamter);
        this.statementId = results.getStatementId();
    }

    private void cursorFetch() throws SQLException {
        if (isLastRow) {
            return;
        }
        int numToFetch = this.getFetchSize();
        try {
            ((ServerSidePreparedStatement) this.getStatement()).cursorFetch(this.statementId, numToFetch);
        } catch (SQLException e) {
            if ("ORA-01002: fetch out of sequence".equals(e.getMessage())) {
                isLastRow = true;
                return;
            } else {
                throw e;
            }
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
    public boolean isLast() throws SQLException {
        if (resultSetScrollType == TYPE_FORWARD_ONLY && getProtocol().isOracleMode()) {
            throw new SQLException("Invalid operation on TYPE_FORWARD_ONLY CURSOR ResultSet for oracle mode: isLast");
        }

        if (rowPointer < dataSize - 1) {
            return false;
        } else if (isEof) {
            return isLastRow && rowPointer == dataSize - 1 && dataSize > 0;
        } else {
            cursorFetch();

            if (isEof) {
                return isLastRow && rowPointer == dataSize - 1 && dataSize > 0;
            }
            return false;
        }
    }

    @Override
    public void afterLast() throws SQLException {
        if (resultSetScrollType == TYPE_FORWARD_ONLY) {
            throw new SQLException("Invalid operation on TYPE_FORWARD_ONLY CURSOR or STREAMING ResultSet: afterLast");
        }

        while (!isLastRow) {
            cursorFetch();
        }
        rowPointer = dataSize;
    }

    @Override
    public boolean last() throws SQLException {
        if (resultSetScrollType == TYPE_FORWARD_ONLY) {
            throw new SQLException("Invalid operation on TYPE_FORWARD_ONLY CURSOR or STREAMING ResultSet: last");
        }

        while (!isLastRow) {
            cursorFetch();
        }
        rowPointer = dataSize - 1;
        return dataSize > 0;
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        if (resultSetScrollType == TYPE_FORWARD_ONLY) {
            throw new SQLException("Invalid operation on TYPE_FORWARD_ONLY CURSOR or STREAMING ResultSet: absolute");
        }

        while (!isLastRow && (row > dataSize || row < -dataSize)) {
            cursorFetch();
        }

        if (row >= 0) {
            if (row <= dataSize) {
                rowPointer = row - 1;
                return true;
            }
            rowPointer = dataSize; // go to afterLast() position
            return false;
        } else {
            if (row >= -dataSize) {
                // absolute position reverse from ending resultSet
                rowPointer = dataSize + row;
                return true;
            }
            rowPointer = -1; // go to before first position
            return false;
        }
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        if (resultSetScrollType == TYPE_FORWARD_ONLY) {
            throw new SQLException("Invalid operation on TYPE_FORWARD_ONLY CURSOR or STREAMING ResultSet: relative");
        }

        int newPos = rowPointer + rows;
        if (newPos < 0) {
            rowPointer = -1;
            return false;
        }
        return absolute(newPos + 1);
    }

    @Override
    public boolean next() throws SQLException {
        if (super.next()) {
            return true;
        } else {
            if (!isLastRow) {
                super.resetState();
                cursorFetch();
                return super.next();
            } else {
                return false;
            }
        }
    }

}
