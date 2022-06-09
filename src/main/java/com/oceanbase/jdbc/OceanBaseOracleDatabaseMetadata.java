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

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import com.oceanbase.jdbc.internal.ColumnType;
import com.oceanbase.jdbc.internal.com.read.resultset.SelectResultSet;

public abstract class OceanBaseOracleDatabaseMetadata implements DatabaseMetaData {

    private final UrlParser           urlParser;
    private final OceanBaseConnection connection;
    private final String              OB_ORA_GET_PROCEDURE_CATALOG_NULL      = "SELECT   NULL AS procedure_cat,owner AS procedure_schem,object_name AS procedure_name,NULL,NULL,NULL,'Standalone procedure or function' AS remarks,DECODE(object_type, 'PROCEDURE', 1,'FUNCTION', 2,0) AS procedure_type,  NULL AS specific_name FROM all_objects WHERE (object_type = 'PROCEDURE' OR object_type = 'FUNCTION') AND owner LIKE ?  AND object_name LIKE ? UNION ALL SELECT  package_name AS procedure_cat, owner AS procedure_schem,object_name AS procedure_name,NULL, NULL,NULL,'Packaged procedure' AS remarks, 1 AS procedure_type, NULL AS specific_name FROM all_arguments WHERE argument_name IS NULL   AND data_type IS NULL   AND package_name IS NOT NULL  AND owner LIKE ?  AND object_name LIKE ? UNION ALL SELECT  package_name AS procedure_cat, owner AS procedure_schem,object_name AS procedure_name,NULL, NULL,NULL,'Packaged procedure' AS remarks, 1 AS procedure_type, NULL AS specific_name FROM all_arguments WHERE argument_name IS NOT NULL  AND position = 1  AND position = sequence  AND package_name IS NOT NULL  AND owner LIKE ?  AND object_name LIKE ? UNION ALL SELECT  package_name AS procedure_cat, owner AS procedure_schem,object_name AS procedure_name,NULL, NULL,NULL,'Packaged function' AS remarks, 2 AS procedure_type,  NULL AS specific_name FROM all_arguments WHERE argument_name IS NULL  AND in_out = 'OUT'  AND   data_level = 0  AND package_name IS NOT NULL  AND owner LIKE ?  AND object_name LIKE ? ORDER BY procedure_schem, procedure_name";
    private final String              OB_ORA_GET_GET_PROCEDURE_CATALOG_EMPTY = "SELECT  NULL AS procedure_cat,owner AS procedure_schem,object_name AS procedure_name,NULL,NULL,NULL,'Standalone procedure or function' AS remarks,DECODE(object_type, 'PROCEDURE', 1,'FUNCTION', 2,0) AS procedure_type,  NULL AS specific_name FROM all_objects WHERE (object_type = 'PROCEDURE' OR object_type = 'FUNCTION') AND owner LIKE ?  AND object_name LIKE ? ";
    private final String              OB_ORA_GET_GET_PROCEDURE_WITH_CATALOG  = "SELECT  package_name AS procedure_cat, owner AS procedure_schem,object_name AS procedure_name,NULL, NULL,NULL,'Packaged procedure' AS remarks, 1 AS procedure_type, NULL AS specific_name FROM all_arguments WHERE argument_name IS NULL   AND data_type IS NULL   AND package_name LIKE ?  AND owner LIKE ?   AND object_name LIKE ? UNION ALL SELECT  package_name AS procedure_cat, owner AS procedure_schem,object_name AS procedure_name,NULL, NULL,NULL,'Packaged procedure' AS remarks, 1 AS procedure_type, NULL AS specific_name FROM all_arguments WHERE argument_name IS NOT NULL  AND position = 1  AND position = sequence  AND package_name LIKE ?  AND owner LIKE ?   AND object_name LIKE ? UNION ALL SELECT  package_name AS procedure_cat, owner AS procedure_schem,object_name AS procedure_name,NULL, NULL,NULL,'Packaged function' AS remarks, 2 AS procedure_type,  NULL AS specific_name FROM all_arguments WHERE argument_name IS NULL  AND in_out = 'OUT'  AND   data_level = 0  AND package_name LIKE ?  AND owner LIKE ?   AND object_name LIKE ? ORDER BY procedure_schem, procedure_name";
    private final String              OB_ORA_GET_SCHEMAS                     = "SELECT username AS table_schem,null as table_catalog  FROM all_users ORDER BY table_schem";
    private final String              OB_ORA_GET_SCHEMAS_WITH_SCHEMAPATTERN  = "SELECT username AS table_schem, null as table_catalog FROM all_users WHERE username LIKE ? ORDER BY table_schem";
    private final String              OB_ORA_GET_CATALOG                     = "select 'nothing' as table_cat from dual where 1 = 2";
    private final String              OB_ORA_GET_USER_NAME                   = "SELECT USER FROM DUAL";
    private final String              OB_ORA_GET_TABLE_TYPES                 = "select 'TABLE' as table_type from dual union select 'VIEW' as table_type from dual union select 'SYNONYM' as table_type from dual";
    private final String              OB_ORA_GET_TABLE_PRIVILEGE             = "SELECT NULL AS table_cat, table_schema AS table_schem, table_name, grantor,grantee, privilege,grantable AS is_grantable FROM all_tab_privs WHERE table_schema LIKE ? ESCAPE '/'  AND table_name LIKE ? ESCAPE '/' ORDER BY table_schem, table_name, privilege";
    private final String              OB_ORA_GET_COLUMN_PRIVILEGES           = "SELECT NULL AS table_cat,OWNER AS table_schem,table_name,column_name,grantor,grantee,privilege,grantable AS is_grantable FROM all_col_privs WHERE OWNER LIKE ? ESCAPE '/' AND table_name LIKE ? ESCAPE '/' AND column_name LIKE ? ESCAPE '/' ORDER BY column_name, privilege";
    private final String              OB_ORA_GET_PRIMARY_KEYS                = "SELECT NULL AS table_cat,c.owner AS table_schem,c.table_name, c.column_name, c.position AS key_seq, c.constraint_name AS pk_name FROM all_cons_columns c, all_constraints k WHERE k.constraint_type = 'P'  AND k.table_name = ?  AND k.owner like ? escape '/'  AND k.constraint_name = c.constraint_name  AND k.table_name = c.table_name   AND k.owner = c.owner ORDER BY column_name ";
    private final String              OB_ORA_GET_TABLES                      = "SELECT NULL AS table_cat,o.owner AS table_schem,o.object_name AS table_name,o.object_type AS table_type,c.comments AS remarks FROM all_objects o, all_tab_comments c WHERE o.owner LIKE ? ESCAPE '/' AND o.object_name LIKE ? ESCAPE '/' AND o.owner = c.owner (+)  AND o.object_name = c.table_name (+)  AND o.owner != '__recyclebin' ";
    private final String              OB_ORA_GET_COLUMNS                     = "SELECT  NULL AS table_cat,\n"
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
                                                                               + "       NULL AS remarks,\n"
                                                                               + "       t.data_default AS column_def,\n"
                                                                               + "       0 AS sql_data_type,\n"
                                                                               + "       0 AS sql_datetime_sub,\n"
                                                                               + "       t.data_length AS char_octet_length,\n"
                                                                               + "       t.column_id AS ordinal_position,\n"
                                                                               + "       DECODE (t.nullable, 'N', 'NO', 'YES') AS is_nullable\n"
                                                                               + "FROM all_tab_columns t\n"
                                                                               + "WHERE t.owner LIKE '%s' ESCAPE '/'\n"
                                                                               + "  AND t.table_name LIKE '%s' ESCAPE '/'\n"
                                                                               + "  AND t.column_name LIKE '%s' ESCAPE '/'\n"
                                                                               + "  AND t.owner != '__recyclebin' ORDER BY table_schem, table_name, ordinal_position";
    private final String              OB_ORA_GET_FUNCTION_CATALOG_NULL       = "SELECT NULL         AS function_cat,owner AS function_schem,object_name AS function_name,'Standalone function' AS remarks,0 AS function_type,NULL AS specific_name FROM all_objects WHERE object_type = 'FUNCTION'  AND owner LIKE ?   AND object_name LIKE ?  UNION ALL SELECT package_name AS function_cat,owner AS function_schem,object_name AS function_name,'Packaged function' AS remarks,decode (data_type, 'TABLE', 2, 'PL/SQL TABLE', 2, 1) AS function_type, NULL AS specific_name FROM all_arguments WHERE argument_name IS NULL  AND in_out = 'OUT'  AND data_level = 0   AND package_name IS NOT NULL  AND owner LIKE ?   AND object_name LIKE ?  ORDER BY function_schem, function_name";
    private final String              OB_ORA_GET_FUNCTION_WITH_CATALOG       = "SELECT package_name AS function_cat,owner AS function_schem,object_name AS function_name,'Packaged function' AS remarks,decode (data_type, 'TABLE', 2, 'PL/SQL TABLE', 2, 1) AS function_type, NULL AS specific_name FROM all_arguments WHERE argument_name IS NULL  AND in_out = 'OUT'  ";
    private final String              OB_ORA_GET_FUNCTION_CATALOG_EMPTY      = "SELECT NULL         AS function_cat,owner AS function_schem,object_name AS function_name,'Standalone function' AS remarks,0 AS function_type,NULL AS specific_name FROM all_objects WHERE object_type = 'FUNCTION'  AND owner LIKE ?   AND object_name LIKE ?  ";
    private final String              OB_ORA_GET_INDEX                       = "select null as table_cat, owner as table_schem,table_name, 0 as NON_UNIQUE,null as index_qualifier,null as index_name, 0 as type,0 as ordinal_position, null as column_name,null as asc_or_desc, num_rows as cardinality,blocks as pages,null as filter_condition from all_tables where table_name = '%s' union  select null as table_cat,i.owner as table_schem,i.table_name,decode (i.uniqueness, 'UNIQUE', 0, 1),null as index_qualifier,i.index_name,1 as type,c.column_position as ordinal_position,c.column_name,null as asc_or_desc,i.distinct_keys as cardinality,i.leaf_blocks as pages, null as filter_condition from (select /*+no_merge*/ * from all_indexes i where i.table_name = '%s') i, all_ind_columns c where i.table_name = '%s'  %s   and i.index_name = c.index_name  and i.table_owner = c.table_owner  and i.table_name = c.table_name  and i.owner = c.index_owner  order by non_unique, type, index_name, ordinal_position ";
    private final String              OB_ORA_GET_BEST_ROW_ID                 = "SELECT 1 AS scope, 'ROWID' AS column_name, -8 AS data_type,\n"
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
                                                                               + "  AS column_size,\n"
                                                                               + "  0 AS buffer_length,\n"
                                                                               + "  t.data_scale AS decimal_digits,\n"
                                                                               + "       1 AS pseudo_column\n"
                                                                               + "FROM all_tab_columns t, all_ind_columns i\n"
                                                                               + "WHERE ? = 1\n"
                                                                               + "  AND t.table_name = ?\n"
                                                                               + "  AND t.owner like ? escape '/'\n"
                                                                               + "  AND t.nullable != ?\n"
                                                                               + "  AND t.owner = i.table_owner\n"
                                                                               + "  AND t.table_name = i.table_name\n"
                                                                               + "  AND t.column_name = i.column_name\n";
    private final String              OB_ORA_KEYS_QUERY                      = "SELECT NULL AS pktable_cat, p.owner as pktable_schem,p.table_name as pktable_name, pc.column_name as pkcolumn_name,NULL as fktable_cat,f.owner as fktable_schem,f.table_name as fktable_name,fc.column_name as fkcolumn_name, fc.position as key_seq,NULL as update_rule,decode (f.delete_rule, 'CASCADE', 0, 'SET NULL', 2, 1) as delete_rule,f.constraint_name as fk_name, p.constraint_name as pk_name, decode(f.deferrable,'DEFERRABLE',5,'NOT DEFERRABLE',7 ,'DEFERRED', 6) deferrability  FROM all_cons_columns pc, all_constraints p,all_cons_columns fc, all_constraints f WHERE 1 = 1  %s %s %s %s  AND f.constraint_type = 'R'   AND p.owner = f.r_owner  AND p.constraint_name = f.r_constraint_name  AND p.constraint_type = 'P'  AND pc.owner = p.owner  AND pc.constraint_name = p.constraint_name  AND pc.table_name = p.table_name  AND fc.owner = f.owner  AND fc.constraint_name = f.constraint_name  AND fc.table_name = f.table_name  AND fc.position = pc.position ";

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
            .format(OB_ORA_GET_COLUMNS,
                schemaPattern == null ? "%" : schemaPattern, tableNamePattern == null ? "%"
                    : tableNamePattern, columnNamePattern == null ? '%' : columnNamePattern);
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

    public ResultSet keysQuery(String parentSchema, String parentTable, String foreignSchema,
                               String foreignTable, String orderBy) throws SQLException {
        int index = 1;
        int parentTableIndex = parentTable != null ? index++ : 0;
        int foreignTableIndex = foreignTable != null ? index++ : 0;
        int parentSchemaIndex = parentSchema != null && parentSchema.length() > 0 ? index++ : 0;
        int foreignSchemaIndex = foreignSchema != null && foreignSchema.length() > 0 ? index++ : 0;
        String sql = String.format(OB_ORA_KEYS_QUERY,
            parentTableIndex != 0 ? "  AND p.table_name = ? " : "",
            foreignTableIndex != 0 ? "  AND f.table_name = ? " : "",
            parentSchemaIndex != 0 ? "  AND p.owner = ? " : "",
            foreignSchemaIndex != 0 ? "  AND f.owner = ? " : "");
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
