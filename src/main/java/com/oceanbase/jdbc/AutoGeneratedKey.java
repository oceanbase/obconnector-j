/*
 *
 * OceanBase Client for Java
 *
 * Copyright (c) 2023 OceanBase.
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
package com.oceanbase.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import com.oceanbase.jdbc.internal.com.read.resultset.ColumnDefinition;
import com.oceanbase.jdbc.internal.protocol.Protocol;
import com.oceanbase.jdbc.internal.util.Utils;
import com.oceanbase.jdbc.internal.util.dao.ServerPrepareResult;

public class AutoGeneratedKey {

    private Protocol      protocol;
    private int           sqlKind = OceanBaseStatement.STMT_UNINITIALIZED;
    private String        originalSql;
    private String        simpleSql;
    private String        newSql;
    private String        tableName;
    int[]                 columnIndexes;
    String[]              columnNames;
    ColumnDefinition[]    columns;
    int[]                 returnTypes;

    public AutoGeneratedKey(final String sql, final int[] columnIndexes, final Protocol protocol) throws SQLException {
        if (sql == null) {
            throw new SQLException("SQL can't be null");
        }
        if (columnIndexes == null || columnIndexes.length == 0) {
            throw new SQLException("columnIndexes can't be empty");
        }

        this.protocol = protocol;
        this.originalSql = sql;
        this.columnIndexes = columnIndexes;

        parseSql();
    }

    public AutoGeneratedKey(final String sql, final String[] columnNames, final Protocol protocol) throws SQLException {
        if (sql == null) {
            throw new SQLException("SQL can't be null");
        }
        if (columnNames == null || columnNames.length == 0) {
            throw new SQLException("columnNames can't be empty");
        }

        this.protocol = protocol;
        this.originalSql = sql;
        this.columnNames = columnNames;

        parseSql();
    }

    private void parseSql() {
        Utils.TrimSQLInfo tmp = Utils.trimSQLStringInternal(originalSql,
            protocol.noBackslashEscapes(), protocol.isOracleMode(), false);
        simpleSql = tmp.getTrimedString().toLowerCase();
        sqlKind = Utils.getStatementType(simpleSql);
    }

    public boolean isInsertSql() {
        return this.sqlKind == OceanBaseStatement.STMT_INSERT;
    }

    private String getTableName() throws SQLException {
        if (tableName != null) {
            return tableName;
        }

        int intoStartPos = simpleSql.indexOf("into", simpleSql.indexOf("insert"));
        if (intoStartPos < 0) {
            throw new SQLException("Failed to find \"insert...into\"");
        }

        int tableStartPos = intoStartPos + 5;
        while (tableStartPos < simpleSql.length() && simpleSql.charAt(tableStartPos) == ' ') {
            tableStartPos++;
        }
        if (tableStartPos >= simpleSql.length()) {
            throw new SQLException("Failed to find the start position of table name");
        }

        int tableEndPos = tableStartPos + 1;
        while (tableEndPos < simpleSql.length() && simpleSql.charAt(tableEndPos) != ' '
               && simpleSql.charAt(tableEndPos) != '(') {
            tableEndPos++;
        }
        if (tableStartPos + 1 == tableEndPos) {
            throw new SQLException("Failed to find the end position of table name");
        }

        tableName = simpleSql.substring(tableStartPos, tableEndPos);
        return tableName;
    }

    public void getColumnDefinition() throws SQLException {
        ServerPrepareResult serverPrepareResult = protocol.prepare("select * from " + getTableName(),
            protocol.isMasterConnection());
        columns = serverPrepareResult.getColumns();
        protocol.releasePrepareStatement(serverPrepareResult);
    }

    public String getNewSqlByIndex() throws SQLException {
        if (this.newSql != null) {
            return this.newSql;
        }
        getColumnDefinition();

        StringBuilder returningBuilder = new StringBuilder(originalSql);
        returningBuilder.append(" returning ");
        StringBuilder intoBuilder = new StringBuilder(" into ");
        returnTypes = new int[columnIndexes.length];

        int i;
        for (i = 0; i < columnIndexes.length; i++) {
            if (i != 0) {
                returningBuilder.append(", ");
                intoBuilder.append(", ");
            }

            int index = columnIndexes[i] - 1;
            if (index < 0 || index >= columns.length) {
                throw new ArrayIndexOutOfBoundsException("index " + index + " must be in [0, "
                                                         + (columns.length - 1) + "]");
            }

            returnTypes[i] = columns[index].getSqltype();
            returningBuilder.append(columns[index].getOriginalName());
            intoBuilder.append('?');
        }

        sqlKind = OceanBaseStatement.STMT_UNINITIALIZED;
        newSql = returningBuilder.toString() + intoBuilder.toString();
        return newSql;
    }

    public String getNewSqlByName() throws SQLException {
        if (this.newSql != null) {
            return this.newSql;
        }
        getColumnDefinition();

        StringBuilder returningBuilder = new StringBuilder(originalSql);
        returningBuilder.append(" returning ");
        StringBuilder intoBuilder = new StringBuilder(" into ");
        returnTypes = new int[columnNames.length];
        columnIndexes = new int[columnNames.length];

        int keyNo;
        for (keyNo = 0; keyNo < columnNames.length; keyNo++) {
            if (keyNo != 0) {
                returningBuilder.append(", ");
                intoBuilder.append(", ");
            }

            boolean found = false;
            for (int i = 0; i < this.columns.length; ++i) {
                if (columnNames[keyNo].equalsIgnoreCase(columns[i].getOriginalName())) {
                    found = true;
                    columnIndexes[keyNo] = i + 1;
                    returnTypes[keyNo] = columns[i].getSqltype();
                }
            }
            if (!found) {
                throw new SQLException("Invalid parameter \"" + columnNames[keyNo] + "\"");
            }

            returningBuilder.append(columnNames[keyNo]);
            intoBuilder.append('?');
        }

        sqlKind = OceanBaseStatement.STMT_UNINITIALIZED;
        newSql = returningBuilder.toString() + intoBuilder.toString();
        return newSql;
    }

}
