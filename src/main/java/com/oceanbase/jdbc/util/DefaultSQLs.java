/*
 *
 * OceanBase Client for Java
 *
 * Copyright (c) 2022 OceanBase.
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
package com.oceanbase.jdbc.util;

public class DefaultSQLs {
    public static final String OB_ORA_GET_PROCEDURE_CATALOG_PROCEDURE_TYPE    = "DECODE(object_type, 'PROCEDURE', 1,'FUNCTION', 2,0)";
    public static final String OB_ORA_GET_PROCEDURE_CATALOG_NULL              = "SELECT  NULL AS procedure_cat,owner AS procedure_schem,object_name AS procedure_name,NULL,NULL,NULL,'Standalone procedure or function' AS remarks,"
                                                                                + OB_ORA_GET_PROCEDURE_CATALOG_PROCEDURE_TYPE
                                                                                + " AS procedure_type,  NULL AS specific_name FROM all_objects WHERE (object_type = 'PROCEDURE' OR object_type = 'FUNCTION') AND owner LIKE ?  AND object_name LIKE ? UNION ALL SELECT  package_name AS procedure_cat, owner AS procedure_schem,object_name AS procedure_name,NULL, NULL,NULL,'Packaged procedure' AS remarks, 1 AS procedure_type, NULL AS specific_name FROM all_arguments WHERE argument_name IS NULL   AND data_type IS NULL   AND package_name IS NOT NULL  AND owner LIKE ?  AND object_name LIKE ? UNION ALL SELECT  package_name AS procedure_cat, owner AS procedure_schem,object_name AS procedure_name,NULL, NULL,NULL,'Packaged procedure' AS remarks, 1 AS procedure_type, NULL AS specific_name FROM all_arguments WHERE argument_name IS NOT NULL  AND position = 1  AND position = sequence  AND package_name IS NOT NULL  AND owner LIKE ?  AND object_name LIKE ? UNION ALL SELECT  package_name AS procedure_cat, owner AS procedure_schem,object_name AS procedure_name,NULL, NULL,NULL,'Packaged function' AS remarks, 2 AS procedure_type,  NULL AS specific_name FROM all_arguments WHERE argument_name IS NULL  AND in_out = 'OUT'  AND   data_level = 0  AND package_name IS NOT NULL  AND owner LIKE ?  AND object_name LIKE ? ORDER BY procedure_schem, procedure_name";
    public static final String OB_ORA_GET_GET_PROCEDURE_CATALOG_EMPTY         = "SELECT  NULL AS procedure_cat,owner AS procedure_schem,object_name AS procedure_name,NULL,NULL,NULL,'Standalone procedure or function' AS remarks,"
                                                                                + OB_ORA_GET_PROCEDURE_CATALOG_PROCEDURE_TYPE
                                                                                + " AS procedure_type,  NULL AS specific_name FROM all_objects WHERE (object_type = 'PROCEDURE' OR object_type = 'FUNCTION') AND owner LIKE ?  AND object_name LIKE ? ";
    public static final String OB_ORA_GET_GET_PROCEDURE_WITH_CATALOG          = "SELECT  package_name AS procedure_cat, owner AS procedure_schem,object_name AS procedure_name,NULL, NULL,NULL,'Packaged procedure' AS remarks, 1 AS procedure_type, NULL AS specific_name FROM all_arguments WHERE argument_name IS NULL   AND data_type IS NULL   AND package_name LIKE ?  AND owner LIKE ?   AND object_name LIKE ? UNION ALL SELECT  package_name AS procedure_cat, owner AS procedure_schem,object_name AS procedure_name,NULL, NULL,NULL,'Packaged procedure' AS remarks, 1 AS procedure_type, NULL AS specific_name FROM all_arguments WHERE argument_name IS NOT NULL  AND position = 1  AND position = sequence  AND package_name LIKE ?  AND owner LIKE ?   AND object_name LIKE ? UNION ALL SELECT  package_name AS procedure_cat, owner AS procedure_schem,object_name AS procedure_name,NULL, NULL,NULL,'Packaged function' AS remarks, 2 AS procedure_type,  NULL AS specific_name FROM all_arguments WHERE argument_name IS NULL  AND in_out = 'OUT'  AND   data_level = 0  AND package_name LIKE ?  AND owner LIKE ?   AND object_name LIKE ? ORDER BY procedure_schem, procedure_name";
    public static final String OB_ORA_GET_SCHEMAS                             = "SELECT username AS table_schem,null as table_catalog  FROM all_users ORDER BY table_schem";
    public static final String OB_ORA_GET_SCHEMAS_WITH_SCHEMAPATTERN          = "SELECT username AS table_schem,null as table_catalog FROM all_users WHERE username LIKE ? ORDER BY table_schem";
    public static final String OB_ORA_GET_CATALOG                             = "select 'getCatalog return null' as table_cat from dual where 2=1";
    public static final String OB_ORA_GET_USER_NAME                           = "SELECT USER FROM DUAL";
    public static final String OB_ORA_GET_TABLE_TYPES                         = "select 'TABLE' as table_type from dual union select 'VIEW' as table_type from dual union select 'SYNONYM' as table_type from dual";
    public static final String OB_ORA_GET_TABLE_PRIVILEGE                     = "SELECT NULL AS table_cat, table_schema AS table_schem, table_name, grantor,grantee, privilege,grantable AS is_grantable FROM all_tab_privs WHERE table_schema LIKE ? ESCAPE '/'  AND table_name LIKE ? ESCAPE '/' ORDER BY table_schem, table_name, privilege";
    public static final String OB_ORA_GET_COLUMN_PRIVILEGES                   = "SELECT NULL AS table_cat,OWNER AS table_schem,table_name,column_name,grantor,grantee,privilege,grantable AS is_grantable FROM all_col_privs WHERE OWNER LIKE ? ESCAPE '/' AND table_name LIKE ? ESCAPE '/' AND column_name LIKE ? ESCAPE '/' ORDER BY column_name, privilege";
    public static final String OB_ORA_GET_PRIMARY_KEYS                        = "SELECT NULL AS table_cat,c.owner AS table_schem,c.table_name, c.column_name, c.position AS key_seq, c.constraint_name AS pk_name FROM all_cons_columns c, all_constraints k WHERE k.constraint_type = 'P'  AND k.table_name = ?  AND k.owner like ? escape '/'  AND k.constraint_name = c.constraint_name  AND k.table_name = c.table_name   AND k.owner = c.owner ORDER BY column_name ";
    public static final String OB_ORA_GET_TABLES                              = "SELECT NULL AS table_cat,o.owner AS table_schem,o.object_name AS table_name,o.object_type AS table_type,c.comments AS remarks FROM all_objects o, all_tab_comments c WHERE o.owner LIKE ? ESCAPE '/' AND o.object_name LIKE ? ESCAPE '/' AND o.owner = c.owner (+)  AND o.object_name = c.table_name (+)  AND o.owner != '__recyclebin' ";
    public static final String OB_ORA_GET_COLUMNS_DATA_TYPE                   = "DECODE (t.data_type, 'CHAR', 1, 'VARCHAR2', 12, 'NUMBER', 3,\n"
                                                                                + "      'LONG', -1, 'DATE', 93, 'RAW', -3, 'LONG RAW', -4,  \n"
                                                                                + "               'BLOB', 2004, 'CLOB', 2005, 'BFILE', -13, 'FLOAT', 6, \n"
                                                                                + "               'TIMESTAMP(6)', 93, 'TIMESTAMP(6) WITH TIME ZONE', -101, \n"
                                                                                + "               'TIMESTAMP(6) WITH LOCAL TIME ZONE', -102, \n"
                                                                                + "               'INTERVAL YEAR(2) TO MONTH', -103, \n"
                                                                                + "               'INTERVAL DAY(2) TO SECOND(6)', -104, \n"
                                                                                + "               'BINARY_FLOAT', 100, 'BINARY_DOUBLE', 101, \n"
                                                                                + "               'XMLTYPE', 2009, \n"
                                                                                + "               1111)\n";
    public static final String OB_ORA_GET_COLUMNS_COLUMN_SIZE                 = "DECODE (t.data_precision, null,DECODE (t.data_type, 'CHAR', t.char_length,'VARCHAR', t.char_length, 'VARCHAR2', t.char_length,'NVARCHAR2', t.char_length,'NCHAR', t.char_length,'NUMBER', 0,t.data_length),t.data_precision)\n";
    public static final String OB_ORA_GET_COLUMNS_DECIMAL_DIGITS              = "DECODE (t.data_type,'NUMBER', DECODE (t.data_precision,null, -127,t.data_scale),t.data_scale) ";
    public static final String OB_ORA_GET_COLUMNS_NULLABLE                    = "DECODE (t.nullable, 'N', 0, 1)";
    public static final String OB_ORA_GET_COLUMNS_IS_NULLABLE                 = "DECODE (t.nullable, 'N', 'NO', 'YES')";
    public static final String OB_ORA_GET_COLUMNS                             = "SELECT  NULL AS table_cat,\n"
                                                                                + "       t.owner AS table_schem,\n"
                                                                                + "       t.table_name AS table_name,\n"
                                                                                + "       t.column_name AS column_name,\n"
                                                                                + OB_ORA_GET_COLUMNS_DATA_TYPE
                                                                                + "              AS data_type,\n"
                                                                                + "       t.data_type AS type_name,\n"
                                                                                + OB_ORA_GET_COLUMNS_COLUMN_SIZE
                                                                                + "              AS column_size,\n"
                                                                                + "       0 AS buffer_length,\n"
                                                                                + OB_ORA_GET_COLUMNS_DECIMAL_DIGITS
                                                                                + "      AS decimal_digits,\n"
                                                                                + "       10 AS num_prec_radix,\n"
                                                                                + OB_ORA_GET_COLUMNS_NULLABLE
                                                                                + "       AS nullable,\n"
                                                                                + "       %s AS remarks,\n"
                                                                                + "       t.data_default AS column_def,\n"
                                                                                + "       0 AS sql_data_type,\n"
                                                                                + "       0 AS sql_datetime_sub,\n"
                                                                                + "       t.data_length AS char_octet_length,\n"
                                                                                + "       t.column_id AS ordinal_position,\n"
                                                                                + OB_ORA_GET_COLUMNS_IS_NULLABLE
                                                                                + "        AS is_nullable\n"
                                                                                + "FROM all_tab_columns t %s\n"
                                                                                + "WHERE t.owner LIKE '%s' ESCAPE '/'\n"
                                                                                + "  AND t.table_name LIKE '%s' ESCAPE '/'\n"
                                                                                + "  AND t.column_name LIKE '%s' ESCAPE '/'\n"
                                                                                + " %s AND t.owner != '__recyclebin' ORDER BY table_schem, table_name, ordinal_position";
    public static final String OB_ORA_GET_FUNCTION_CATALOG_NULL_FUNCTION_TYPE = "DECODE (data_type, 'TABLE', 2, 'PL/SQL TABLE', 2, 1)";
    public static final String OB_ORA_GET_FUNCTION_CATALOG_NULL               = "SELECT NULL   AS function_cat,owner AS function_schem,object_name AS function_name,'Standalone function' AS remarks,0 AS function_type,NULL AS specific_name FROM all_objects WHERE object_type = 'FUNCTION'  AND owner LIKE ?   AND object_name LIKE ?  UNION ALL SELECT package_name AS function_cat,owner AS function_schem,object_name AS function_name,'Packaged function' AS remarks,"
                                                                                + OB_ORA_GET_FUNCTION_CATALOG_NULL_FUNCTION_TYPE
                                                                                + "AS function_type, NULL AS specific_name FROM all_arguments WHERE argument_name IS NULL  AND in_out = 'OUT'  AND data_level = 0   AND package_name IS NOT NULL  AND owner LIKE ?   AND object_name LIKE ?  ORDER BY function_schem, function_name";
    public static final String OB_ORA_GET_FUNCTION_WITH_CATALOG               = "SELECT package_name AS function_cat,owner AS function_schem,object_name AS function_name,'Packaged function' AS remarks,"
                                                                                + OB_ORA_GET_FUNCTION_CATALOG_NULL_FUNCTION_TYPE
                                                                                + "AS function_type, NULL AS specific_name FROM all_arguments WHERE argument_name IS NULL  AND in_out = 'OUT'  ";
    public static final String OB_ORA_GET_FUNCTION_CATALOG_EMPTY              = "SELECT NULL  AS function_cat,owner AS function_schem,object_name AS function_name,'Standalone function' AS remarks,0 AS function_type,NULL AS specific_name FROM all_objects WHERE object_type = 'FUNCTION'  AND owner LIKE ?   AND object_name LIKE ?  ";
    public static final String OB_ORA_GET_INDEX_NO_UNIQUE                     = "DECODE (i.uniqueness, 'UNIQUE', 0, 1)";
    public static final String OB_ORA_GET_INDEX                               = "select null as table_cat, owner as table_schem,table_name, 0 as NON_UNIQUE,null as index_qualifier,null as index_name, 0 as type,0 as ordinal_position, null as column_name,null as asc_or_desc, num_rows as cardinality,blocks as pages,null as filter_condition from all_tables where table_name = '%s' union  select null as table_cat,i.owner as table_schem,i.table_name,"
                                                                                + OB_ORA_GET_INDEX_NO_UNIQUE
                                                                                + ",null as index_qualifier,i.index_name,1 as type,c.column_position as ordinal_position,c.column_name,null as asc_or_desc,i.distinct_keys as cardinality,i.leaf_blocks as pages, null as filter_condition from (select /*+no_merge*/ * from all_indexes i where i.table_name = '%s') i, all_ind_columns c where i.table_name = '%s'  %s   and i.index_name = c.index_name  and i.table_owner = c.table_owner  and i.table_name = c.table_name  and i.owner = c.index_owner  order by non_unique, type, index_name, ordinal_position ";
    public static final String OB_ORA_GET_BEST_ROW_ID_DATA_TYPE               = "  DECODE(substr(t.data_type, 1, 9), 'TIMESTAMP', \n"
                                                                                + "      DECODE(substr(t.data_type, 10, 1), '(',DECODE(substr(t.data_type, 19, 5), 'LOCAL', -102, 'TIME ', -101, 93), DECODE(substr(t.data_type, 16, 5), \n"
                                                                                + "      'LOCAL', -102, 'TIME ', -101, 93)),'INTERVAL ',DECODE(substr(t.data_type, 10, 3), 'DAY', -104, 'YEA', -103), \n"
                                                                                + "      DECODE(t.data_type, \n"
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
                                                                                + "       DECODE((SELECT a.typecode FROM ALL_TYPES a WHERE a.type_name = t.data_type AND ((a.owner IS NULL AND t.data_type_owner IS NULL) OR (a.owner = t.data_type_owner))), \n"
                                                                                + "        'OBJECT', 2002,'COLLECTION', 2003, 1111))) \n";
    public static final String OB_ORA_GET_BEST_ROW_ID_COLUMN_SIZE             = " DECODE (t.data_precision, null,  DECODE (t.data_type, 'CHAR', t.char_length, 'VARCHAR', t.char_length, 'VARCHAR2', t.char_length, 'NVARCHAR2', t.char_length, 'NCHAR', t.char_length, t.data_length), t.data_precision)\n";
    public static final String OB_ORA_GET_BEST_ROW_ID                         = "SELECT 1 AS scope, 'ROWID' AS column_name, -8 AS data_type,\n"
                                                                                + " 'ROWID' AS type_name, 0 AS column_size, 0 AS buffer_length,\n"
                                                                                + "       0 AS decimal_digits, 2 AS pseudo_column\n"
                                                                                + "FROM DUAL\n"
                                                                                + "WHERE ? = 1\n"
                                                                                + "UNION\n"
                                                                                + "SELECT 2 AS scope,\n"
                                                                                + " t.column_name,\n"
                                                                                + OB_ORA_GET_BEST_ROW_ID_DATA_TYPE
                                                                                + " AS data_type,\n"
                                                                                + " t.data_type AS type_name,\n"
                                                                                + OB_ORA_GET_BEST_ROW_ID_COLUMN_SIZE
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
    public static final String OB_ORA_KEYS_QUERY                              = "SELECT NULL AS pktable_cat, p_cons.owner as pktable_schem,p_cons.table_name as pktable_name, p_cons_cols.column_name as pkcolumn_name,NULL as fktable_cat,f_cons.owner as fktable_schem,f_cons.table_name as fktable_name,f_cons_cols.column_name as fkcolumn_name, f_cons_cols.position as key_seq,NULL as update_rule,decode (f_cons.delete_rule, 'CASCADE', 0, 'SET NULL', 2, 1) as delete_rule,f_cons.constraint_name as fk_name, p_cons.constraint_name as pk_name, decode(f_cons.deferrable,'DEFERRABLE',5,'NOT DEFERRABLE',7 ,'DEFERRED', 6) deferrability  FROM all_cons_columns p_cons_cols, all_constraints p_cons,all_cons_columns f_cons_cols, all_constraints f_cons WHERE 1 = 1  %s %s %s %s  AND f_cons.constraint_type = 'R'   AND p_cons.owner = f_cons.r_owner  AND p_cons.constraint_name = f_cons.r_constraint_name  AND p_cons.constraint_type = 'P'  AND p_cons_cols.owner = p_cons.owner  AND p_cons_cols.constraint_name = p_cons.constraint_name  AND p_cons_cols.table_name = p_cons.table_name  AND f_cons_cols.owner = f_cons.owner  AND f_cons_cols.constraint_name = f_cons.constraint_name  AND f_cons_cols.table_name = f_cons.table_name  AND f_cons_cols.position = p_cons_cols.position ";

}
