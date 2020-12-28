package com.alipay.oceanbase.jdbc;

import com.alipay.oceanbase.jdbc.internal.ColumnType;
import com.alipay.oceanbase.jdbc.internal.com.read.resultset.ColumnDefinition;
import com.alipay.oceanbase.jdbc.internal.com.read.resultset.SelectResultSet;
import com.alipay.oceanbase.jdbc.internal.protocol.Protocol;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

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
        String var2 = "       NULL AS remarks\n";
        String var3 = "  FROM all_objects o\n";
        String var4 = "  WHERE o.owner LIKE ? \n    AND o.object_name LIKE ? \n";
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
        stmt.setString(2, tableNamePattern == null ? "%" : tableNamePattern);
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
    class TypeDescriptor {
        int     bufferLength;

        int     charOctetLength;

        Integer columnSize;

        short   dataType;

        Integer decimalDigits;

        String  isNullable;

        int     nullability;

        int     numPrecRadix = 10;

        String  typeName;

//        TypeDescriptor(String typeInfo, String nullabilityInfo) throws SQLException {
//            if (typeInfo == null) {
//                throw new SQLException("NULL typeinfo not supported.");
//            }
//
//            String mysqlType = "";
//            String fullMysqlType = null;
//
//            if (typeInfo.indexOf("(") != -1) {
//                mysqlType = typeInfo.substring(0, typeInfo.indexOf("(")).trim();
//            } else {
//                mysqlType = typeInfo;
//            }
//
//            int indexOfUnsignedInMysqlType = StringUtils.indexOfIgnoreCase(mysqlType, "unsigned");
//
//            if (indexOfUnsignedInMysqlType != -1) {
//                mysqlType = mysqlType.substring(0, (indexOfUnsignedInMysqlType - 1));
//            }
//
//            // Add unsigned to typename reported to enduser as 'native type', if present
//
//            boolean isUnsigned = false;
//
//            if ((StringUtils.indexOfIgnoreCase(typeInfo, "unsigned") != -1)
//                    && (StringUtils.indexOfIgnoreCase(typeInfo, "set") != 0)
//                    && (StringUtils.indexOfIgnoreCase(typeInfo, "enum") != 0)) {
//                fullMysqlType = mysqlType + " unsigned";
//                isUnsigned = true;
//            } else {
//                fullMysqlType = mysqlType;
//            }
//
//            if (DatabaseMetaData.this.conn.getCapitalizeTypeNames()) {
//                fullMysqlType = fullMysqlType.toUpperCase(Locale.ENGLISH);
//            }
//
//            this.dataType = (short) MysqlDefs.mysqlToJavaType(mysqlType);
//
//            this.typeName = fullMysqlType;
//
//            // Figure Out the Size
//
//            if (StringUtils.startsWithIgnoreCase(typeInfo, "enum")) {
//                String temp = typeInfo.substring(typeInfo.indexOf("("), typeInfo.lastIndexOf(")"));
//                java.util.StringTokenizer tokenizer = new java.util.StringTokenizer(temp, ",");
//                int maxLength = 0;
//
//                while (tokenizer.hasMoreTokens()) {
//                    maxLength = Math.max(maxLength, (tokenizer.nextToken().length() - 2));
//                }
//
//                this.columnSize = Integer.valueOf(maxLength);
//                this.decimalDigits = null;
//            } else if (StringUtils.startsWithIgnoreCase(typeInfo, "set")) {
//                String temp = typeInfo.substring(typeInfo.indexOf("(") + 1,
//                        typeInfo.lastIndexOf(")"));
//                java.util.StringTokenizer tokenizer = new java.util.StringTokenizer(temp, ",");
//                int maxLength = 0;
//
//                int numElements = tokenizer.countTokens();
//
//                if (numElements > 0) {
//                    maxLength += (numElements - 1);
//                }
//
//                while (tokenizer.hasMoreTokens()) {
//                    String setMember = tokenizer.nextToken().trim();
//
//                    if (setMember.startsWith("'") && setMember.endsWith("'")) {
//                        maxLength += setMember.length() - 2;
//                    } else {
//                        maxLength += setMember.length();
//                    }
//                }
//
//                this.columnSize = Integer.valueOf(maxLength);
//                this.decimalDigits = null;
//            } else if (typeInfo.indexOf(",") != -1) {
//                // Numeric with decimals
//                this.columnSize = Integer.valueOf(typeInfo.substring((typeInfo.indexOf("(") + 1),
//                        (typeInfo.indexOf(","))).trim());
//                this.decimalDigits = Integer.valueOf(typeInfo.substring(
//                        (typeInfo.indexOf(",") + 1), (typeInfo.indexOf(")"))).trim());
//            } else {
//                this.columnSize = null;
//                this.decimalDigits = null;
//
//                /* If the size is specified with the DDL, use that */
//                if ((StringUtils.indexOfIgnoreCase(typeInfo, "char") != -1
//                        || StringUtils.indexOfIgnoreCase(typeInfo, "text") != -1
//                        || StringUtils.indexOfIgnoreCase(typeInfo, "blob") != -1
//                        || StringUtils.indexOfIgnoreCase(typeInfo, "binary") != -1 || StringUtils
//                        .indexOfIgnoreCase(typeInfo, "bit") != -1) && typeInfo.indexOf("(") != -1) {
//                    int endParenIndex = typeInfo.indexOf(")");
//
//                    if (endParenIndex == -1) {
//                        endParenIndex = typeInfo.length();
//                    }
//
//                    this.columnSize = Integer.valueOf(typeInfo.substring(
//                            (typeInfo.indexOf("(") + 1), endParenIndex).trim());
//
//                    // Adjust for pseudo-boolean
//                    if (DatabaseMetaData.this.conn.getTinyInt1isBit()
//                            && this.columnSize.intValue() == 1
//                            && StringUtils.startsWithIgnoreCase(typeInfo, 0, "tinyint")) {
//                        if (DatabaseMetaData.this.conn.getTransformedBitIsBoolean()) {
//                            this.dataType = Types.BOOLEAN;
//                            this.typeName = "BOOLEAN";
//                        } else {
//                            this.dataType = Types.BIT;
//                            this.typeName = "BIT";
//                        }
//                    }
//                } else if (StringUtils.startsWithIgnoreCaseAndWs(typeInfo, "tinyint")) {
//                    if (DatabaseMetaData.this.conn.getTinyInt1isBit()
//                            && typeInfo.indexOf("(1)") != -1) {
//                        if (DatabaseMetaData.this.conn.getTransformedBitIsBoolean()) {
//                            this.dataType = Types.BOOLEAN;
//                            this.typeName = "BOOLEAN";
//                        } else {
//                            this.dataType = Types.BIT;
//                            this.typeName = "BIT";
//                        }
//                    } else {
//                        this.columnSize = Integer.valueOf(3);
//                        this.decimalDigits = Integer.valueOf(0);
//                    }
//                } else if (StringUtils.startsWithIgnoreCaseAndWs(typeInfo, "smallint")) {
//                    this.columnSize = Integer.valueOf(5);
//                    this.decimalDigits = Integer.valueOf(0);
//                } else if (StringUtils.startsWithIgnoreCaseAndWs(typeInfo, "mediumint")) {
//                    this.columnSize = Integer.valueOf(isUnsigned ? 8 : 7);
//                    this.decimalDigits = Integer.valueOf(0);
//                } else if (StringUtils.startsWithIgnoreCaseAndWs(typeInfo, "int")) {
//                    this.columnSize = Integer.valueOf(10);
//                    this.decimalDigits = Integer.valueOf(0);
//                } else if (StringUtils.startsWithIgnoreCaseAndWs(typeInfo, "integer")) {
//                    this.columnSize = Integer.valueOf(10);
//                    this.decimalDigits = Integer.valueOf(0);
//                } else if (StringUtils.startsWithIgnoreCaseAndWs(typeInfo, "bigint")) {
//                    this.columnSize = Integer.valueOf(isUnsigned ? 20 : 19);
//                    this.decimalDigits = Integer.valueOf(0);
//                } else if (StringUtils.startsWithIgnoreCaseAndWs(typeInfo, "int24")) {
//                    this.columnSize = Integer.valueOf(19);
//                    this.decimalDigits = Integer.valueOf(0);
//                } else if (StringUtils.startsWithIgnoreCaseAndWs(typeInfo, "real")) {
//                    this.columnSize = Integer.valueOf(12);
//                } else if (StringUtils.startsWithIgnoreCaseAndWs(typeInfo, "float")) {
//                    this.columnSize = Integer.valueOf(12);
//                } else if (StringUtils.startsWithIgnoreCaseAndWs(typeInfo, "decimal")) {
//                    this.columnSize = Integer.valueOf(12);
//                } else if (StringUtils.startsWithIgnoreCaseAndWs(typeInfo, "numeric")) {
//                    this.columnSize = Integer.valueOf(12);
//                } else if (StringUtils.startsWithIgnoreCaseAndWs(typeInfo, "double")) {
//                    this.columnSize = Integer.valueOf(22);
//                } else if (StringUtils.startsWithIgnoreCaseAndWs(typeInfo, "char")) {
//                    this.columnSize = Integer.valueOf(1);
//                } else if (StringUtils.startsWithIgnoreCaseAndWs(typeInfo, "varchar")) {
//                    this.columnSize = Integer.valueOf(255);
//                } else if (StringUtils.startsWithIgnoreCaseAndWs(typeInfo, "timestamp")) {
//                    this.columnSize = Integer.valueOf(19);
//                } else if (StringUtils.startsWithIgnoreCaseAndWs(typeInfo, "datetime")) {
//                    this.columnSize = Integer.valueOf(19);
//                } else if (StringUtils.startsWithIgnoreCaseAndWs(typeInfo, "date")) {
//                    this.columnSize = Integer.valueOf(10);
//                } else if (StringUtils.startsWithIgnoreCaseAndWs(typeInfo, "time")) {
//                    this.columnSize = Integer.valueOf(8);
//
//                } else if (StringUtils.startsWithIgnoreCaseAndWs(typeInfo, "tinyblob")) {
//                    this.columnSize = Integer.valueOf(255);
//                } else if (StringUtils.startsWithIgnoreCaseAndWs(typeInfo, "blob")) {
//                    this.columnSize = Integer.valueOf(65535);
//                } else if (StringUtils.startsWithIgnoreCaseAndWs(typeInfo, "mediumblob")) {
//                    this.columnSize = Integer.valueOf(16777215);
//                } else if (StringUtils.startsWithIgnoreCaseAndWs(typeInfo, "longblob")) {
//                    this.columnSize = Integer.valueOf(Integer.MAX_VALUE);
//                } else if (StringUtils.startsWithIgnoreCaseAndWs(typeInfo, "tinytext")) {
//                    this.columnSize = Integer.valueOf(255);
//                } else if (StringUtils.startsWithIgnoreCaseAndWs(typeInfo, "text")) {
//                    this.columnSize = Integer.valueOf(65535);
//                } else if (StringUtils.startsWithIgnoreCaseAndWs(typeInfo, "mediumtext")) {
//                    this.columnSize = Integer.valueOf(16777215);
//                } else if (StringUtils.startsWithIgnoreCaseAndWs(typeInfo, "longtext")) {
//                    this.columnSize = Integer.valueOf(Integer.MAX_VALUE);
//                } else if (StringUtils.startsWithIgnoreCaseAndWs(typeInfo, "enum")) {
//                    this.columnSize = Integer.valueOf(255);
//                } else if (StringUtils.startsWithIgnoreCaseAndWs(typeInfo, "set")) {
//                    this.columnSize = Integer.valueOf(255);
//                }
//
//            }
//
//            // BUFFER_LENGTH
//            this.bufferLength = MysqlIO.getMaxBuf();
//
//            // NUM_PREC_RADIX (is this right for char?)
//            this.numPrecRadix = 10;
//
//            // Nullable?
//            if (nullabilityInfo != null) {
//                if (nullabilityInfo.equals("YES")) {
//                    this.nullability = java.sql.DatabaseMetaData.columnNullable;
//                    this.isNullable = "YES";
//
//                } else if (nullabilityInfo.equals("UNKNOWN")) {
//                    this.nullability = java.sql.DatabaseMetaData.columnNullableUnknown;
//                    this.isNullable = "";
//
//                    // IS_NULLABLE
//                } else {
//                    this.nullability = java.sql.DatabaseMetaData.columnNoNulls;
//                    this.isNullable = "NO";
//                }
//            } else {
//                this.nullability = java.sql.DatabaseMetaData.columnNoNulls;
//                this.isNullable = "NO";
//            }
//        }
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
            String nullabilityInfo = results.getString("NULLABLE");
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
}
