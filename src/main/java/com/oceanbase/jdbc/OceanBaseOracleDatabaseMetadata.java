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

import com.oceanbase.jdbc.internal.ColumnType;
import com.oceanbase.jdbc.internal.com.read.resultset.SelectResultSet;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

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
        rs.setStatement(null); // bypass Hibernate statement tracking (CONJ-49)
        rs.setForceTableAlias();
        return rs;
    }

    @Override
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern)
                                                                                                     throws SQLException {
        String var1 = "SELECT\n  -- Standalone procedures and functions\n  NULL AS procedure_cat,\n  owner AS procedure_schem,\n  object_name AS procedure_name,\n  NULL,\n  NULL,\n  NULL,\n  'Standalone procedure or function' AS remarks,\n  DECODE(object_type, 'PROCEDURE', 1,\n                      'FUNCTION', 2,\n                      0) AS procedure_type\n,  NULL AS specific_name\nFROM all_objects\nWHERE (object_type = 'PROCEDURE' OR object_type = 'FUNCTION')\n  AND owner LIKE ? \n  AND object_name LIKE ? \n";
        String var2 = "SELECT\n  -- Packaged procedures with no arguments\n  package_name AS procedure_cat,\n  owner AS procedure_schem,\n  object_name AS procedure_name,\n  NULL,\n  NULL,\n  NULL,\n  'Packaged procedure' AS remarks,\n  1 AS procedure_type\n,  NULL AS specific_name\nFROM all_arguments\nWHERE argument_name IS NULL\n  AND data_type IS NULL\n  AND ";
        String var3 = "SELECT\n  -- Packaged procedures with arguments\n  package_name AS procedure_cat,\n  owner AS procedure_schem,\n  object_name AS procedure_name,\n  NULL,\n  NULL,\n  NULL,\n  'Packaged procedure' AS remarks,\n  1 AS procedure_type\n,  NULL AS specific_name\nFROM all_arguments\nWHERE argument_name IS NOT NULL\n  AND position = 1\n  AND position = sequence\n  AND ";
        String var4 = "SELECT\n  -- Packaged functions\n  package_name AS procedure_cat,\n  owner AS procedure_schem,\n  object_name AS procedure_name,\n  NULL,\n  NULL,\n  NULL,\n  'Packaged function' AS remarks,\n  2 AS procedure_type\n,  NULL AS specific_name\nFROM all_arguments\nWHERE argument_name IS NULL\n  AND in_out = 'OUT'\n  AND   data_level = 0\n  AND ";
        String var5 = "package_name LIKE ? \n  AND owner LIKE ? \n  AND object_name LIKE ? \n";
        String var6 = "package_name IS NOT NULL\n  AND owner LIKE ? \n  AND object_name LIKE ? \n";
        String var7 = "ORDER BY procedure_schem, procedure_name\n";
        PreparedStatement stmt = null;
        String var8 = null;
        String var9 = null;
        if (schemaPattern == null) {
            var9 = "%";
        } else if (schemaPattern.equals("")) {
            var9 = this.getUserName().toUpperCase();
        } else {
            var9 = schemaPattern.toUpperCase();
        }

        String var14 = null;
        if (procedureNamePattern == null) {
            var14 = "%";
        } else if (procedureNamePattern.equals("")) {
            throw new SQLException();
        } else {
            var14 = procedureNamePattern.toUpperCase();
        }

        if (catalog == null) {
            var8 = var1 + "UNION ALL " + var2 + var6 + "UNION ALL " + var3 + var6 + "UNION ALL "
                   + var4 + var6 + var7;
            stmt = this.connection.prepareStatement(var8);
            stmt.setString(1, var9);
            stmt.setString(2, var14);
            stmt.setString(3, var9);
            stmt.setString(4, var14);
            stmt.setString(5, var9);
            stmt.setString(6, var14);
            stmt.setString(7, var9);
            stmt.setString(8, var14);
        } else if (catalog.equals("")) {
            stmt = this.connection.prepareStatement(var1);
            stmt.setString(1, var9);
            stmt.setString(2, var14);
        } else {
            var8 = var2 + var5 + "UNION ALL " + var3 + var5 + "UNION ALL " + var4 + var5 + var7;
            stmt = this.connection.prepareStatement(var8);
            stmt.setString(1, catalog);
            stmt.setString(2, var9);
            stmt.setString(3, var14);
            stmt.setString(4, catalog);
            stmt.setString(5, var9);
            stmt.setString(6, var14);
            stmt.setString(7, catalog);
            stmt.setString(8, var9);
            stmt.setString(9, var14);
        }

        ResultSet resultSet = stmt.executeQuery();
        if (stmt != null) {
            stmt.close();
        }
        return resultSet;
    }

    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern,
                               String[] types) throws SQLException {
        String var1 = "SELECT NULL AS table_cat,\n       o.owner AS table_schem,\n       o.object_name AS table_name,\n       o.object_type AS table_type,\n";
        String var2 = "  c.comments AS remarks\n";
        String var3 = "  FROM all_objects o, all_tab_comments c\n";
        String var4 = "  WHERE o.owner LIKE ? ESCAPE '/'\n    AND o.object_name LIKE ? ESCAPE '/'\n      AND o.owner = c.owner (+)\n    AND o.object_name = c.table_name (+)\n  AND o.owner != '__recyclebin'\n";
        String var5 = "";
        String var6 = "";
        if (types != null) {
            var5 = "    AND o.object_type IN ('xxx'";
            var6 = "    AND o.object_type IN ('xxx'";
            for (int i = 0; i < types.length; ++i) {
                if (types[i].equals("SYNONYM")) {
                    var5 = var5 + ", '" + types[i] + "'";
                } else {
                    var5 = var5 + ", '" + types[i] + "'";
                    var6 = var6 + ", '" + types[i] + "'";
                }
            }
            var5 = var5 + ")\n";
        } else {
            var5 = "    AND o.object_type IN ('TABLE', 'SYNONYM', 'VIEW')\n";
        }
        String var7 = "  ORDER BY table_type, table_schem, table_name\n";
        String sql = "";
        sql = sql + var1;
        sql = sql + var2 + var3;
        sql = sql + var4;
        sql = sql + var5;
        sql = sql + var7;
        PreparedStatement stmt = this.connection.prepareStatement(sql);
        stmt.setString(1, schemaPattern == null ? "%" : schemaPattern);
        stmt.setString(2, tableNamePattern == null ? "%" : tableNamePattern.toUpperCase());
        ResultSet resultSet = stmt.executeQuery();
        if (stmt != null) {
            stmt.close();
        }
        return resultSet;
    }

    @Override
    public ResultSet getSchemas() throws SQLException {
        Statement stmt = this.connection.createStatement();
        String sql = "SELECT username AS table_schem,null as table_catalog  FROM all_users ORDER BY table_schem";
        ResultSet rs = stmt.executeQuery(sql);
        if (stmt != null) {
            stmt.close();
        }
        return rs;
    }

    @Override
    public ResultSet getCatalogs() throws SQLException {
        return executeQuery("select 'nothing' as table_cat from dual where 1 = 2");
    }

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern,
                                String columnNamePattern) throws SQLException {
        /**
                String var1 = "SELECT ";
                String var2 = " NULL AS table_cat,\n";
                String var3 = "         DECODE (t.data_type, 'CHAR', t.char_length,                   'VARCHAR', t.char_length,                   'VARCHAR2', t.char_length,                   'NVARCHAR2', t.char_length,                   'NCHAR', t.char_length,                   'NUMBER', 0,           t.data_length)";
                String var5 = "       t.owner AS table_schem,\n       t.table_name AS table_name,\n";
                String var6 = "       t.column_name AS column_name,\n       DECODE (t.data_type, 'CHAR', 1, 'VARCHAR2', 12, 'NUMBER', 3,\n               'LONG', -1, 'DATE', 93 , 'RAW', -3, 'LONG RAW', -4,  \n" + "               'BLOB', 2004, 'CLOB', 2005, 'BFILE', -13, 'FLOAT', 6, \n" + "               'TIMESTAMP(6)', 93, 'TIMESTAMP(6) WITH TIME ZONE', -101, \n" + "               'TIMESTAMP(6) WITH LOCAL TIME ZONE', -102, \n" + "               'INTERVAL YEAR(2) TO MONTH', -103, \n" + "               'INTERVAL DAY(2) TO SECOND(6)', -104, \n" + "               'BINARY_FLOAT', 100, 'BINARY_DOUBLE', 101, \n" + "               'XMLTYPE', 2009, \n" + "               1111)\n" + "              AS data_type,\n" + "       t.data_type AS type_name,\n" + "       DECODE (t.data_precision, " + "               null, DECODE(t.data_type, " + "                       'NUMBER', DECODE(t.data_scale, " + " null, 0 ,38), " + var3 + "                           )," + "         t.data_precision)\n" + "              AS column_size,\n" + "       0 AS buffer_length,\n" + "       DECODE (t.data_type, " + "               'NUMBER', DECODE(t.data_precision, " + "                                null, DECODE(t.data_scale, " + "                                             null, -127                                             , t.data_scale), " + "                                 t.data_scale), " + "               t.data_scale) AS decimal_digits,\n" + "       10 AS num_prec_radix,\n" + "       DECODE (t.nullable, 'N', 0, 1) AS nullable,\n";
                String var7 = "       NULL AS remarks,\n";
                String var8 = "       t.data_default AS column_def,\n       0 AS sql_data_type,\n       0 AS sql_datetime_sub,\n       t.data_length AS char_octet_length,\n       t.column_id AS ordinal_position,\n       DECODE (t.nullable, 'N', 'NO', 'YES') AS is_nullable,\n";
                String var9 = "         null as SCOPE_CATALOG,\n       null as SCOPE_SCHEMA,\n       null as SCOPE_TABLE,\n       null as SOURCE_DATA_TYPE,\n       'NO' as IS_AUTOINCREMENT\n";
                String var10 = "FROM all_tab_columns t";
                String var11 = "WHERE t.owner LIKE ?  AND t.table_name LIKE ?  AND t.column_name LIKE ?";
                String var12 = "ORDER BY table_schem, table_name, ordinal_position\n";

                String var13 = var1 + var2;
                var13 = var13 + var5;
                var13 = var13 + var6;
                var13 = var13 + var7;
                var13 = var13 + var8 + var9 + var10;
                var13 = var13 + "\n" + var11;
                var13 = var13 + "\n" + var12;
        */
        Statement stmt = this.connection.getMetadataSafeStatement();
        String querySql = String
            .format(
                "SELECT  NULL AS table_cat,\n"
                        + "       t.owner AS table_schem,\n"
                        + "       t.table_name AS table_name,\n"
                        + "       t.column_name AS column_name,\n"
                        + "       DECODE (t.data_type, 'CHAR', 1, 'VARCHAR2', 12, 'NUMBER', 3,\n"
                        + "               'LONG', -1, 'DATE', 93, 'RAW', -3, 'LONG RAW', -4,  \n"
                        + "               'BLOB', 2004, 'CLOB', 2005, 'BFILE', -13, 'FLOAT', 6, \n"
                        + "               'TIMESTAMP(6)', 93, 'TIMESTAMP(6) WITH TIME ZONE', -101, \n"
                        + "               'TIMESTAMP(6) WITH LOCAL TIME ZONE', -102, \n"
                        + "               'INTERVAL YEAR(2) TO MONTH', -103, \n"
                        + "               'INTERVAL DAY(2) TO SECOND(6)', -104, \n"
                        + "               'BINARY_FLOAT', 100, 'BINARY_DOUBLE', 101, \n"
                        + "               'XMLTYPE', 2009, \n"
                        + "               1111)\n"
                        + "              AS data_type,\n"
                        + "       t.data_type AS type_name,\n"
                        + "       DECODE (t.data_precision, null,          DECODE (t.data_type, 'CHAR', t.char_length,                   'VARCHAR', t.char_length,                   'VARCHAR2', t.char_length,                   'NVARCHAR2', t.char_length,                   'NCHAR', t.char_length,                   'NUMBER', 0,           t.data_length),         t.data_precision)\n"
                        + "              AS column_size,\n"
                        + "       0 AS buffer_length,\n"
                        + "       DECODE (t.data_type,                'NUMBER', DECODE (t.data_precision,                                  null, -127,                                  t.data_scale),                t.data_scale) AS decimal_digits,\n"
                        + "       10 AS num_prec_radix,\n"
                        + "       DECODE (t.nullable, 'N', 0, 1) AS nullable,\n"
                        + "       NULL AS remarks,\n" + "       t.data_default AS column_def,\n"
                        + "       0 AS sql_data_type,\n" + "       0 AS sql_datetime_sub,\n"
                        + "       t.data_length AS char_octet_length,\n"
                        + "       t.column_id AS ordinal_position,\n"
                        + "       DECODE (t.nullable, 'N', 'NO', 'YES') AS is_nullable\n"
                        + "FROM all_tab_columns t\n" + "WHERE t.owner LIKE '%s' ESCAPE '/'\n"
                        + "  AND t.table_name LIKE '%s' ESCAPE '/'\n"
                        + "  AND t.column_name LIKE '%s' ESCAPE '/'\n"
                        + "  AND t.owner != '__recyclebin' \n" // Avoid return __recyclebin tables
                        + "\n" + "ORDER BY table_schem, table_name, ordinal_position\n",
                schemaPattern == null ? "%" : schemaPattern, tableNamePattern == null ? "%"
                    : tableNamePattern, columnNamePattern == null ? '%' : columnNamePattern);
        ResultSet results = stmt.executeQuery(querySql);
        if (stmt != null) {
            stmt.close();
        }
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
            String sql = "SELECT username AS table_schem, null as table_catalog FROM all_users WHERE username LIKE ? ORDER BY table_schem";
            PreparedStatement stmt = this.connection.prepareStatement(sql);
            stmt.setString(1, schemaPattern);
            ResultSet rs = stmt.executeQuery();
            if (stmt != null) {
                stmt.close();
            }
            return rs;
        }
    }

    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern)
                                                                                                   throws SQLException {
        String var1 = "SELECT\n  NULL AS function_cat,\n  owner AS function_schem,\n  object_name AS function_name,\n  'Standalone function' AS remarks,\n  0 AS function_type,\n  NULL AS specific_name\nFROM all_objects\nWHERE object_type = 'FUNCTION'\n  AND owner LIKE ? \n  AND object_name LIKE ? \n";
        String var2 = "SELECT\n  -- Packaged functions\n  package_name AS function_cat,\n  owner AS function_schem,\n  object_name AS function_name,\n  'Packaged function' AS remarks,\n  decode (data_type, 'TABLE', 2, 'PL/SQL TABLE', 2, 1) AS function_type,\n  NULL AS specific_name\nFROM all_arguments\nWHERE argument_name IS NULL\n  AND in_out = 'OUT'\n  AND data_level = 0\n";
        String var3 = "  AND package_name LIKE ? \n  AND owner LIKE ? \n  AND object_name LIKE ? \n";
        String var4 = "  AND package_name IS NOT NULL\n  AND owner LIKE ? \n  AND object_name LIKE ? \n";
        String var5 = "ORDER BY function_schem, function_name\n";
        PreparedStatement stmt = null;
        String var6 = null;
        String var7 = schemaPattern;
        if (schemaPattern == null) {
            var7 = "%";
        } else if (schemaPattern.equals("")) {
            var7 = this.getUserName().toUpperCase();
        } else {
            var7 = schemaPattern.toUpperCase();
        }
        String var8 = functionNamePattern;
        if (functionNamePattern == null) {
            var8 = "%";
        } else if (functionNamePattern.equals("")) {
            throw new SQLException();
        } else {
            var8 = functionNamePattern.toUpperCase();
        }
        if (catalog == null) {
            var6 = var1 + "UNION ALL " + var2 + var4 + var5;
            stmt = this.connection.prepareStatement(var6);
            stmt.setString(1, var7);
            stmt.setString(2, var8);
            stmt.setString(3, var7);
            stmt.setString(4, var8);
        } else if (catalog.equals("")) {
            stmt = this.connection.prepareStatement(var1);
            stmt.setString(1, var7);
            stmt.setString(2, var8);
        } else {
            var6 = var2 + var3 + var5;
            stmt = this.connection.prepareStatement(var6);
            stmt.setString(1, var7);
            stmt.setString(2, var7);
            stmt.setString(3, var8);
        }
        ResultSet rs = stmt.executeQuery();
        if (stmt != null) {
            stmt.close();
        }
        return rs;
    }

    public String getUserName() throws SQLException {
        ResultSet rs = executeQuery("SELECT USER FROM DUAL");
        rs.next();
        String userName = rs.getString(1);
        return userName;
    }

    public ResultSet getTableTypes() throws SQLException {
        return executeQuery("select 'TABLE' as table_type from dual\nunion select 'VIEW' as table_type from dual\nunion select 'SYNONYM' as table_type from dual\n");
    }

    public ResultSet getTablePrivileges(String catalog, String schemaPattern,
                                        String tableNamePattern) throws SQLException {
        PreparedStatement pstmt = this.connection
            .prepareStatement("SELECT NULL AS table_cat,\n       table_schema AS table_schem,\n       table_name,\n       grantor,\n       grantee,\n       privilege,\n       grantable AS is_grantable\nFROM all_tab_privs\nWHERE table_schema LIKE ? ESCAPE '/'\n  AND table_name LIKE ? ESCAPE '/'\nORDER BY table_schem, table_name, privilege\n");
        pstmt.setString(1, schemaPattern == null ? "%" : schemaPattern);
        pstmt.setString(2, tableNamePattern == null ? "%" : tableNamePattern.toUpperCase());
        pstmt.closeOnCompletion();
        ResultSet resultSet = pstmt.executeQuery();
        return resultSet;
    }

    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique,
                                  boolean approximate) throws SQLException {

        Statement stmt = this.connection.createStatement();
        String sql = "";

        sql = "select null as table_cat,\n" + "       owner as table_schem,\n"
              + "       table_name,\n" + "       0 as NON_UNIQUE,\n"
              + "       null as index_qualifier,\n" + "       null as index_name, 0 as type,\n"
              + "       0 as ordinal_position, null as column_name,\n"
              + "       null as asc_or_desc,\n" + "       num_rows as cardinality,\n"
              + "       blocks as pages,\n" + "       null as filter_condition\n"
              + "from all_tables\n" + "where table_name = '" + table + "'\n";

        String sql2 = "select null as table_cat,\n" + "       i.owner as table_schem,\n"
                      + "       i.table_name,\n"
                      + "       decode (i.uniqueness, 'UNIQUE', 0, 1),\n"
                      + "       null as index_qualifier,\n" + "       i.index_name,\n"
                      + "       1 as type,\n" + "       c.column_position as ordinal_position,\n"
                      + "       c.column_name,\n" + "       null as asc_or_desc,\n"
                      + "       i.distinct_keys as cardinality,\n"
                      + "       i.leaf_blocks as pages,\n" + "       null as filter_condition\n"
                      + "from all_indexes i, all_ind_columns c\n" + "where i.table_name = '"
                      + table + "'\n";
        String sql3 = "";
        if (unique) {
            sql3 = "  and i.uniqueness = 'UNIQUE'\n";
        }

        String sql4 = "  and i.index_name = c.index_name\n  and i.table_owner = c.table_owner\n  and i.table_name = c.table_name\n  and i.owner = c.index_owner\n";
        String sql5 = "order by non_unique, type, index_name, ordinal_position\n";
        String sql6 = sql + "union\n" + sql2 + sql3 + sql4 + sql5;
        stmt.closeOnCompletion();
        ResultSet resultSet = stmt.executeQuery(sql6);
        return resultSet;
    }

    public ResultSet getPrimaryKeys(String catalog, String schema, String table)
                                                                                throws SQLException {

        PreparedStatement pstmt = this.connection
            .prepareStatement("SELECT NULL AS table_cat,\n       c.owner AS table_schem,\n       c.table_name,\n       c.column_name,\n       c.position AS key_seq,\n       c.constraint_name AS pk_name\nFROM all_cons_columns c, all_constraints k\nWHERE k.constraint_type = 'P'\n  AND k.table_name = ?\n  AND k.owner like ? escape '/'\n  AND k.constraint_name = c.constraint_name \n  AND k.table_name = c.table_name \n  AND k.owner = c.owner \nORDER BY column_name\n");
        pstmt.setString(1, table);
        pstmt.setString(2, schema == null ? "%" : schema);
        pstmt.closeOnCompletion();
        ResultSet resultSet = pstmt.executeQuery();
        return resultSet;
    }

    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope,
                                          final boolean nullable) throws SQLException {

        PreparedStatement pstmt = this.connection
            .prepareStatement("SELECT 1 AS scope, 'ROWID' AS column_name, -8 AS data_type,\n"
                              + " 'ROWID' AS type_name, 0 AS column_size, 0 AS buffer_length,\n"
                              + "       0 AS decimal_digits, 2 AS pseudo_column\n"
                              + "FROM DUAL\n"
                              + "WHERE ? = 1\n"
                              + "UNION\n"
                              + "SELECT 2 AS scope,\n"
                              + " t.column_name,\n"
                              + "  DECODE(substr(t.data_type, 1, 9), \n"
                              + "    'TIMESTAMP', \n"
                              + "      DECODE(substr(t.data_type, 10, 1), \n"
                              + "        '(', \n"
                              + "          DECODE(substr(t.data_type, 19, 5), \n"
                              + "            'LOCAL', -102, 'TIME ', -101, 93), \n"
                              + "        DECODE(substr(t.data_type, 16, 5), \n"
                              + "          'LOCAL', -102, 'TIME ', -101, 93)), \n"
                              + "    'INTERVAL ', \n"
                              + "      DECODE(substr(t.data_type, 10, 3), \n"
                              + "       'DAY', -104, 'YEA', -103), \n"
                              + "    DECODE(t.data_type, \n"
                              + "      'BINARY_DOUBLE', 101, \n"
                              + "      'BINARY_FLOAT', 100, \n"
                              + "      'BFILE', -13, \n"
                              + "      'BLOB', 2004, \n"
                              + "      'CHAR', 1, \n"
                              + "      'CLOB', 2005, \n"
                              + "      'COLLECTION', 2003, \n"
                              + "      'DATE', 93, \n"
                              + "      'FLOAT', 6, \n"
                              + "      'LONG', -1, \n"
                              + "      'LONG RAW', -4, \n"
                              + "      'NCHAR', -15, \n"
                              + "      'NCLOB', 2011, \n"
                              + "      'NUMBER', 2, \n"
                              + "      'NVARCHAR', -9, \n"
                              + "      'NVARCHAR2', -9, \n"
                              + "      'OBJECT', 2002, \n"
                              + "      'OPAQUE/XMLTYPE', 2009, \n"
                              + "      'RAW', -3, \n"
                              + "      'REF', 2006, \n"
                              + "      'ROWID', -8, \n"
                              + "      'SQLXML', 2009, \n"
                              + "      'UROWID', -8, \n"
                              + "      'VARCHAR2', 12, \n"
                              + "      'VARRAY', 2003, \n"
                              + "      'XMLTYPE', 2009, \n"
                              + "      DECODE((SELECT a.typecode \n"
                              + "        FROM ALL_TYPES a \n"
                              + "        WHERE a.type_name = t.data_type\n"
                              + "       AND ((a.owner IS NULL AND \n"
                              + "t.data_type_owner IS NULL)\n"
                              + "         OR (a.owner = t.data_type_owner))\n"
                              + "        ), \n"
                              + "        'OBJECT', 2002, \n"
                              + "        'COLLECTION', 2003, 1111))) \n"
                              + " AS data_type,\n"
                              + " t.data_type AS type_name,\n"
                              + " DECODE (t.data_precision, null,  DECODE (t.data_type, 'CHAR', t.char_length, 'VARCHAR', t.char_length, 'VARCHAR2', t.char_length, 'NVARCHAR2', t.char_length, 'NCHAR', t.char_length, t.data_length), t.data_precision)\n"
                              + "  AS column_size,\n" + "  0 AS buffer_length,\n"
                              + "  t.data_scale AS decimal_digits,\n"
                              + "       1 AS pseudo_column\n"
                              + "FROM all_tab_columns t, all_ind_columns i\n" + "WHERE ? = 1\n"
                              + "  AND t.table_name = ?\n" + "  AND t.owner like ? escape '/'\n"
                              + "  AND t.nullable != ?\n" + "  AND t.owner = i.table_owner\n"
                              + "  AND t.table_name = i.table_name\n"
                              + "  AND t.column_name = i.column_name\n");
        switch (scope) {
            case 1:
                pstmt.setInt(1, 1);
                pstmt.setInt(2, 1);
                break;
            case 2:
                pstmt.setInt(1, 0);
                pstmt.setInt(2, 1);
                break;
            case 0:
            default:
                pstmt.setInt(1, 0);
                pstmt.setInt(2, 0);
                break;
        }
        pstmt.setString(3, table);
        pstmt.setString(4, schema == null ? "%" : schema);
        pstmt.setString(5, nullable ? "X" : "Y");
        pstmt.closeOnCompletion();
        ResultSet resultSet = pstmt.executeQuery();
        return resultSet;
    }

    public ResultSet getColumnPrivileges(String catalog, String schema, String table,
                                         String columnNamePattern) throws SQLException {
        PreparedStatement pstmt = this.connection
            .prepareStatement("SELECT NULL AS table_cat,\n       OWNER AS table_schem,\n       table_name,\n       column_name,\n       grantor,\n       grantee,\n       privilege,\n       grantable AS is_grantable\nFROM all_col_privs\nWHERE OWNER LIKE ? ESCAPE '/'\n  AND table_name LIKE ? ESCAPE '/'\n  AND column_name LIKE ? ESCAPE '/'\nORDER BY column_name, privilege\n");
        pstmt.setString(1, schema == null ? "%" : schema);
        pstmt.setString(2, table == null ? "%" : table.toUpperCase());
        pstmt.setString(3, columnNamePattern == null ? "%" : columnNamePattern);
        pstmt.closeOnCompletion();
        return pstmt.executeQuery();
    }

    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        return false;
    }

    public boolean storesLowerCaseIdentifiers() throws SQLException {
        //there return true in 1.X,but oracle-jdbc return false
        return true;
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

    public ResultSet keysQuery(String var1, String var2, String var3, String var4, String var5)
                                                                                               throws SQLException {
        int var6 = 1;
        int var7 = var2 != null ? var6++ : 0;
        int var8 = var4 != null ? var6++ : 0;
        int var9 = var1 != null && var1.length() > 0 ? var6++ : 0;
        int var10 = var3 != null && var3.length() > 0 ? var6++ : 0;
        PreparedStatement var11 = this.connection
            .prepareStatement("SELECT NULL AS pktable_cat,\n       p.owner as pktable_schem,\n       p.table_name as pktable_name,\n       pc.column_name as pkcolumn_name,\n       NULL as fktable_cat,\n       f.owner as fktable_schem,\n       f.table_name as fktable_name,\n       fc.column_name as fkcolumn_name,\n       fc.position as key_seq,\n       NULL as update_rule,\n       decode (f.delete_rule, 'CASCADE', 0, 'SET NULL', 2, 1) as delete_rule,\n       f.constraint_name as fk_name,\n       p.constraint_name as pk_name,\n       decode(f.deferrable,       'DEFERRABLE',5      ,'NOT DEFERRABLE',7      , 'DEFERRED', 6      ) deferrability \n      FROM all_cons_columns pc, all_constraints p,\n      all_cons_columns fc, all_constraints f\nWHERE 1 = 1\n"
                              + (var7 != 0 ? "  AND p.table_name = ?\n" : "")
                              + (var8 != 0 ? "  AND f.table_name = ?\n" : "")
                              + (var9 != 0 ? "  AND p.owner = ?\n" : "")
                              + (var10 != 0 ? "  AND f.owner = ?\n" : "")
                              + "  AND f.constraint_type = 'R'\n  AND p.owner = f.r_owner\n  AND p.constraint_name = f.r_constraint_name\n  AND p.constraint_type = 'P'\n  AND pc.owner = p.owner\n  AND pc.constraint_name = p.constraint_name\n  AND pc.table_name = p.table_name\n  AND fc.owner = f.owner\n  AND fc.constraint_name = f.constraint_name\n  AND fc.table_name = f.table_name\n  AND fc.position = pc.position\n"
                              + var5);
        if (var7 != 0) {
            var11.setString(var7, var2);
        }

        if (var8 != 0) {
            var11.setString(var8, var4);
        }

        if (var9 != 0) {
            var11.setString(var9, var1);
        }

        if (var10 != 0) {
            var11.setString(var10, var3);
        }

        var11.closeOnCompletion();
        ResultSet resultSet = var11.executeQuery();
        return resultSet;
    }
}
