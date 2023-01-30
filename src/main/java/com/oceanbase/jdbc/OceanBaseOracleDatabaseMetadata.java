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
package com.oceanbase.jdbc;

import static com.oceanbase.jdbc.util.DefaultSQLs.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import com.oceanbase.jdbc.internal.ColumnType;
import com.oceanbase.jdbc.internal.com.read.resultset.SelectResultSet;

public abstract class OceanBaseOracleDatabaseMetadata implements DatabaseMetaData {

    private final UrlParser           urlParser;
    private final OceanBaseConnection connection;

    public OceanBaseOracleDatabaseMetadata(UrlParser urlParser, Connection connection) {
        this.urlParser = urlParser;
        this.connection = (OceanBaseConnection) connection;
    }

    public ResultSet executeQuery(String sql) throws SQLException {
        Statement stmt = connection.createStatement();
        SelectResultSet rs = (SelectResultSet) stmt.executeQuery(sql);
        if (!rs.getStatement().isCursorResultSet()) {
            rs.setStatement(null); // bypass Hibernate statement tracking (CONJ-49)
        }
        rs.setForceTableAlias();
        return rs;
    }

    @Override
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern)
                                                                                                     throws SQLException {
        PreparedStatement stmt;
        String schemaStr;
        if (schemaPattern == null) {
            schemaStr = "%";
        } else if (schemaPattern.equals("")) {
            schemaStr = this.getUserName().toUpperCase();
        } else {
            schemaStr = schemaPattern.toUpperCase();
        }
        String procedureStr;
        if (procedureNamePattern == null) {
            procedureStr = "%";
        } else if (procedureNamePattern.equals("")) {
            throw new SQLException();
        } else {
            procedureStr = procedureNamePattern.toUpperCase();
        }
        if (catalog == null) {
            stmt = this.connection.prepareStatement(OB_ORA_GET_PROCEDURE_CATALOG_NULL);
            stmt.setString(1, schemaStr);
            stmt.setString(2, procedureStr);
            stmt.setString(3, schemaStr);
            stmt.setString(4, procedureStr);
            stmt.setString(5, schemaStr);
            stmt.setString(6, procedureStr);
            stmt.setString(7, schemaStr);
            stmt.setString(8, procedureStr);
        } else if (catalog.equals("")) {
            stmt = this.connection.prepareStatement(OB_ORA_GET_GET_PROCEDURE_CATALOG_EMPTY);
            stmt.setString(1, schemaStr);
            stmt.setString(2, procedureStr);
        } else {
            stmt = this.connection.prepareStatement(OB_ORA_GET_GET_PROCEDURE_WITH_CATALOG);
            stmt.setString(1, catalog);
            stmt.setString(2, schemaStr);
            stmt.setString(3, procedureStr);
            stmt.setString(4, catalog);
            stmt.setString(5, schemaStr);
            stmt.setString(6, procedureStr);
            stmt.setString(7, catalog);
            stmt.setString(8, schemaStr);
            stmt.setString(9, procedureStr);
        }
        stmt.closeOnCompletion();
        ResultSet rs = stmt.executeQuery();
        return rs;
    }

    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern,
                               String[] types) throws SQLException {
        String typeList;
        if (types != null) {
            typeList = "  AND o.object_type IN ('xxx'";
            for (int i = 0; i < types.length; ++i) {
                if (types[i].equals("SYNONYM")) {
                    typeList = typeList + ", '" + types[i] + "'";
                } else {
                    typeList = typeList + ", '" + types[i] + "'";
                }
            }
            typeList = typeList + ")\n";
        } else {
            typeList = "  AND o.object_type IN ('TABLE', 'SYNONYM', 'VIEW') ";
        }
        String orderBy = "  ORDER BY table_type, table_schem, table_name ";
        String sql = OB_ORA_GET_TABLES + typeList + orderBy;
        PreparedStatement stmt = this.connection.prepareStatement(sql);
        stmt.setString(1, schemaPattern == null ? "%" : schemaPattern);
        stmt.setString(2, tableNamePattern == null ? "%" : tableNamePattern);
        stmt.closeOnCompletion();
        ResultSet rs = stmt.executeQuery();
        return rs;
    }

    @Override
    public ResultSet getSchemas() throws SQLException {
        Statement stmt = this.connection.createStatement();
        stmt.closeOnCompletion();
        ResultSet rs = stmt.executeQuery(OB_ORA_GET_SCHEMAS);
        return rs;
    }

    @Override
    public ResultSet getCatalogs() throws SQLException {
        return executeQuery(OB_ORA_GET_CATALOG);
    }

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern,
                                String columnNamePattern) throws SQLException {
        Statement stmt = this.connection.getMetadataSafeStatement();
        String querySql = String
            .format(OB_ORA_GET_COLUMNS,connection.getRemarksReporting() ?  "c.comments ":"NULL ",connection.getRemarksReporting() ? ",all_col_comments c " : "",
                schemaPattern == null ? "%" : schemaPattern, tableNamePattern == null ? "%"
                    : tableNamePattern, columnNamePattern == null ? '%' : columnNamePattern,connection.getRemarksReporting() ?  "  AND t.owner = c.owner (+)\n" +
                            "  AND t.table_name = c.table_name (+)\n" +
                            "  AND t.column_name = c.column_name (+)":"");
        stmt.closeOnCompletion();
        ResultSet results = stmt.executeQuery(querySql);
//        int count = results.getFetchSize();
        String[] data = new String[24];
        List<String[]> list = new ArrayList<> ();
        int i = 0;
        while (results.next()) {
            data[0] = null;
            data[1] = results.getString("TABLE_SCHEM");
            data[2] = results.getString("TABLE_NAME");
            data[3] = results.getString("COLUMN_NAME");
            data[4] = results.getString("DATA_TYPE");
            data[5] = results.getString("TYPE_NAME");
            data[6] = results.getString("COLUMN_SIZE");
            data[7] = results.getString("BUFFER_LENGTH");
            data[8] = results.getString("DECIMAL_DIGITS");
            data[9] = results.getString("NUM_PREC_RADIX");
            String nullabilityInfo = results.getString("IS_NULLABLE");
            String isNullable = null;
            int  nullability = 0;
            if (nullabilityInfo != null) {
                if (nullabilityInfo.equals("YES")) {
                    nullability = java.sql.DatabaseMetaData.columnNullable;
                    isNullable = "YES";
                } else if (nullabilityInfo.equals("UNKNOWN")) {
                    nullability = java.sql.DatabaseMetaData.columnNullableUnknown;
                    isNullable = "";
                } else {
                    nullability = java.sql.DatabaseMetaData.columnNoNulls;
                    isNullable = "NO";
                }
            } else {
                nullability = java.sql.DatabaseMetaData.columnNoNulls;
                isNullable = "NO";
            }
            data[10] = Integer.toString(nullability);
            data[11] = results.getString("REMARKS");
            data[12] = results.getString("COLUMN_DEF");
            data[13] = results.getString("SQL_DATA_TYPE");
            data[14] = results.getString("SQL_DATETIME_SUB");
            data[15] = results.getString("CHAR_OCTET_LENGTH");
            data[16] = Integer.toString(results.getInt("ORDINAL_POSITION") -15);
            data[17] = isNullable;
            data[18] = null;
            data[19] = null;
            data[20] = null;
            data[21] = null;
            data[22] = "NO";
            data[23] = null;
            i ++ ;
            String[] tmp = new String[24];
            System.arraycopy(data,0,tmp,0,data.length);
            list.add(tmp);
        }
        String[][] val = new String[list.size()][];
        for(int j=0; j<list.size(); j++)  {
            val[j] = list.get(j);
        }
        String[] columnNames = {
                "TABLE_CAT",
                "TABLE_SCHEM",
                "TABLE_NAME",
                "COLUMN_NAME",
                "DATA_TYPE",
                "TYPE_NAME",
                "COLUMN_SIZE",
                "BUFFER_LENGTH",
                "DECIMAL_DIGITS",
                "NUM_PREC_RADIX",
                "NULLABLE",
                "REMARKS",
                "COLUMN_DEF",
                "SQL_DATA_TYPE",
                "SQL_DATETIME_SUB",
                "CHAR_OCTET_LENGTH",
                "ORDINAL_POSITION",
                "IS_NULLABLE",
                "SCOPE_CATALOG",
                "SCOPE_SCHEMA",
                "SCOPE_TABLE",
                "SOURCE_DATA_TYPE",
                "IS_AUTOINCREMENT",
                "IS_GENERATEDCOLUMN"
        };
        ColumnType[] columnTypes = {
                ColumnType.ENUM,
                ColumnType.ENUM,
                ColumnType.ENUM,
                ColumnType.ENUM,
                ColumnType.INTEGER,
                ColumnType.ENUM,
                ColumnType.INTEGER,
                ColumnType.INTEGER,
                ColumnType.INTEGER,
                ColumnType.INTEGER,
                ColumnType.INTEGER,
                ColumnType.ENUM,
                ColumnType.ENUM,
                ColumnType.INTEGER,
                ColumnType.INTEGER,
                ColumnType.INTEGER,
                ColumnType.INTEGER,
                ColumnType.ENUM,
                ColumnType.ENUM,
                ColumnType.ENUM,
                ColumnType.ENUM,
                ColumnType.SMALLINT,
                ColumnType.ENUM,
                ColumnType.ENUM
        };
        ResultSet rs = SelectResultSet.createResultSet(columnNames,columnTypes,val,this.connection.getProtocol());
        return rs;
    }

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        if (schemaPattern == null) {
            return this.getSchemas();
        } else {
            PreparedStatement stmt = this.connection
                .prepareStatement(OB_ORA_GET_SCHEMAS_WITH_SCHEMAPATTERN);
            stmt.setString(1, schemaPattern);
            stmt.closeOnCompletion();
            ResultSet rs = stmt.executeQuery();
            return rs;
        }
    }

    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern)
                                                                                                   throws SQLException {
        PreparedStatement stmt;
        String schemaName;
        if (schemaPattern == null) {
            schemaName = "%";
        } else if (schemaPattern.equals("")) {
            schemaName = this.getUserName().toUpperCase();
        } else {
            schemaName = schemaPattern.toUpperCase();
        }
        String functionName = functionNamePattern;
        if (functionNamePattern == null) {
            functionName = "%";
        } else if (functionNamePattern.equals("")) {
            throw new SQLException();
        } else {
            functionName = functionNamePattern.toUpperCase();
        }
        if (catalog == null) {
            stmt = this.connection.prepareStatement(OB_ORA_GET_FUNCTION_CATALOG_NULL);
            stmt.setString(1, schemaName);
            stmt.setString(2, functionName);
            stmt.setString(3, schemaName);
            stmt.setString(4, functionName);
        } else if (catalog.equals("")) {
            stmt = this.connection.prepareStatement(OB_ORA_GET_FUNCTION_CATALOG_EMPTY);
            stmt.setString(1, schemaName);
            stmt.setString(2, functionName);
        } else {
            stmt = this.connection.prepareStatement(OB_ORA_GET_FUNCTION_WITH_CATALOG);
            stmt.setString(1, schemaName);
            stmt.setString(2, schemaName);
            stmt.setString(3, functionName);
        }
        stmt.closeOnCompletion();
        ResultSet rs = stmt.executeQuery();
        return rs;
    }

    public String getUserName() throws SQLException {
        ResultSet rs = executeQuery(OB_ORA_GET_USER_NAME);
        rs.next();
        String userName = rs.getString(1);
        return userName;
    }

    public ResultSet getTableTypes() throws SQLException {
        return executeQuery(OB_ORA_GET_TABLE_TYPES);
    }

    public ResultSet getTablePrivileges(String catalog, String schemaPattern,
                                        String tableNamePattern) throws SQLException {
        PreparedStatement stmt = this.connection.prepareStatement(OB_ORA_GET_TABLE_PRIVILEGE);
        stmt.setString(1, schemaPattern == null ? "%" : schemaPattern);
        stmt.setString(2, tableNamePattern == null ? "%" : tableNamePattern.toUpperCase());
        stmt.closeOnCompletion();
        ResultSet rs = stmt.executeQuery();
        return rs;
    }

    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique,
                                  boolean approximate) throws SQLException {
        String prepare = String.format(OB_ORA_GET_INDEX, table, table, table,
            unique ? "  and i.uniqueness = 'UNIQUE'" : "");
        return executeQuery(prepare);
    }

    public ResultSet getPrimaryKeys(String catalog, String schema, String table)
                                                                                throws SQLException {

        PreparedStatement stmt = this.connection.prepareStatement(OB_ORA_GET_PRIMARY_KEYS);
        stmt.setString(1, table);
        stmt.setString(2, schema == null ? "%" : schema);
        stmt.closeOnCompletion();
        ResultSet rs = stmt.executeQuery();
        return rs;
    }

    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope,
                                          final boolean nullable) throws SQLException {

        PreparedStatement stmt = this.connection.prepareStatement(OB_ORA_GET_BEST_ROW_ID);
        switch (scope) {
            case 1:
                stmt.setInt(1, 1);
                stmt.setInt(2, 1);
                break;
            case 2:
                stmt.setInt(1, 0);
                stmt.setInt(2, 1);
                break;
            case 0:
            default:
                stmt.setInt(1, 0);
                stmt.setInt(2, 0);
                break;
        }
        stmt.setString(3, table);
        stmt.setString(4, schema == null ? "%" : schema);
        stmt.setString(5, nullable ? "X" : "Y");
        stmt.closeOnCompletion();
        ResultSet rs = stmt.executeQuery();
        return rs;
    }

    public ResultSet getColumnPrivileges(String catalog, String schema, String table,
                                         String columnNamePattern) throws SQLException {
        PreparedStatement stmt = this.connection.prepareStatement(OB_ORA_GET_COLUMN_PRIVILEGES);
        stmt.setString(1, schema == null ? "%" : schema);
        stmt.setString(2, table == null ? "%" : table.toUpperCase());
        stmt.setString(3, columnNamePattern == null ? "%" : columnNamePattern);
        stmt.closeOnCompletion();
        return stmt.executeQuery();
    }

    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        return true;
    }

    public boolean storesLowerCaseIdentifiers() throws SQLException {
        //there return true in 1.X,but oracle-jdbc return false
        return false;
    }

    public boolean storesMixedCaseIdentifiers() throws SQLException {
        return false;
    }

    public boolean supportsANSI92IntermediateSQL() {
        return false;
    }

    public boolean supportsOpenCursorsAcrossRollback() {
        return false;
    }

    public boolean allProceduresAreCallable() {
        return false;
    }

    public boolean allTablesAreSelectable() {
        return false;
    }

    public boolean supportsOpenCursorsAcrossCommit() {
        return false;
    }

    public boolean supportsOpenStatementsAcrossRollback() {
        return false;
    }

    public boolean supportsOpenStatementsAcrossCommit() {
        return false;
    }

    public boolean nullsAreSortedAtEnd() {
        return false;
    }

    public boolean locatorsUpdateCopy() {
        return true;
    }

    public boolean supportsANSI92FullSQL() {
        return false;
    }

    public boolean storesUpperCaseIdentifiers() {
        return true;
    }

    public boolean doesMaxRowSizeIncludeBlobs() {
        return true;
    }

    public boolean supportsConvert() {
        return false;
    }

    public boolean supportsCatalogsInTableDefinitions() {
        return false;
    }

    public boolean supportsSchemasInTableDefinitions() {
        return true;
    }

    public ResultSet keysQuery(String parentSchema, String parentTable, String foreignSchema,
                               String foreignTable, String orderBy) throws SQLException {
        int index = 1;
        int parentTableIndex = parentTable != null ? index++ : 0;
        int foreignTableIndex = foreignTable != null ? index++ : 0;
        int parentSchemaIndex = parentSchema != null && parentSchema.length() > 0 ? index++ : 0;
        int foreignSchemaIndex = foreignSchema != null && foreignSchema.length() > 0 ? index++ : 0;
        String sql = String.format(OB_ORA_KEYS_QUERY,
            parentTableIndex != 0 ? "  AND p_cons.table_name = ? " : "",
            foreignTableIndex != 0 ? "  AND f_cons.table_name = ? " : "",
            parentSchemaIndex != 0 ? "  AND p_cons.owner = ? " : "",
            foreignSchemaIndex != 0 ? "  AND f_cons.owner = ? " : "");
        PreparedStatement stmt = this.connection.prepareStatement(sql + orderBy);
        if (parentTableIndex != 0) {
            stmt.setString(parentTableIndex, parentTable);
        }

        if (foreignTableIndex != 0) {
            stmt.setString(foreignTableIndex, foreignTable);
        }

        if (parentSchemaIndex != 0) {
            stmt.setString(parentSchemaIndex, parentSchema);
        }

        if (foreignSchemaIndex != 0) {
            stmt.setString(foreignSchemaIndex, foreignSchema);
        }

        stmt.closeOnCompletion();
        ResultSet rs = stmt.executeQuery();
        return rs;
    }
}