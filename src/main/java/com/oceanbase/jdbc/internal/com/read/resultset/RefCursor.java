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
import com.oceanbase.jdbc.extend.datatype.RowObCursorData;
import com.oceanbase.jdbc.internal.com.read.dao.ColumnLabelIndexer;
import com.oceanbase.jdbc.internal.com.read.dao.Results;
import com.oceanbase.jdbc.internal.com.read.resultset.rowprotocol.BinaryRowProtocol;
import com.oceanbase.jdbc.internal.protocol.Protocol;

public class RefCursor extends CursorResultSet {

    private RowObCursorData rowObCursorData;

    public RefCursor(ColumnDefinition[] columnsInformation, Results results, Protocol protocol,
                     boolean callableResult, boolean eofDeprecated, boolean isPsOutParamter,
                     RowObCursorData rowObCursorData) throws IOException, SQLException {
        super(columnsInformation, results, protocol, callableResult, eofDeprecated, isPsOutParamter);
        resultSetScrollType = ResultSet.TYPE_FORWARD_ONLY;
        resultSetConcurType = ResultSet.CONCUR_READ_ONLY;
        this.rowObCursorData = rowObCursorData;
        try {
            cursorFetchInternal(1); // get matched column information
        } catch (Exception ignored) {
        }
    }

    @Override
    protected boolean cursorFetch() throws SQLException {
        return cursorFetchInternal(fetchSize);
    }

    private boolean cursorFetchInternal(int tmpFetchSize) throws SQLException {
        if (protocol != null) {
            protocol.startCallInterface();
        }
        this.lock.lock();
        try {
            lockLogger.debug("RefCursor.cursorFetchInternal locked");
            if (isLastRowSent) {
                return false;
            }
            if (this.rowObCursorData == null) {
                isLastRowSent = true;
                return false;
            }

            ColumnDefinition[] ci = ((ServerSidePreparedStatement) this.getStatement())
                .cursorFetch(this.rowObCursorData.getCursorId(), tmpFetchSize);
            if (ci != null) {
                this.columnsInformation = ci;
                this.columnInformationLength = ci.length;
                this.row = new BinaryRowProtocol(columnsInformation, columnInformationLength, this
                    .getStatement().getMaxFieldSize(), options);
                this.columnLabelIndexer = new ColumnLabelIndexer(columnsInformation);
                this.row.setProtocol(this.getProtocol());
            }

            getCursorFetchData(tmpFetchSize);
            return true;
        } catch (SQLException e) {
            if ("ORA-01002: fetch out of sequence".equals(e.getMessage())) {
                isLastRowSent = true;
            }
            throw e;
        } finally {
            lock.unlock();
            lockLogger.debug("RefCursor.cursorFetchInternal unlocked");
            if (protocol != null) {
                protocol.endCallInterface("RefCursor.cursorFetch");
            }
        }
    }

    @Override
    public void close() throws SQLException {
        if (rowObCursorData != null) {
            ((ServerSidePreparedStatement) this.getStatement()).closeCursor(this.rowObCursorData
                .getCursorId());
            rowObCursorData.setOpen(false);
        }
        super.close();
    }
}
