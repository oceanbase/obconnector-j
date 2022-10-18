/**
 *  OceanBase Client for Java
 *
 *  Copyright (c) 2012-2014 Monty Program Ab.
 *  Copyright (c) 2015-2020 MariaDB Corporation Ab.
 *  Copyright (c) 2021 OceanBase.
 *
 *  This library is free software; you can redistribute it and/or modify it under
 *  the terms of the GNU Lesser General Public License as published by the Free
 *  Software Foundation; either version 2.1 of the License, or (at your option)
 *  any later version.
 *  This library is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 *  FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 *  for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public License along
 *  with this library; if not, write to Monty Program Ab info@montyprogram.com.
 *
 *  This particular MariaDB Client for Java file is work
 *  derived from a Drizzle-JDBC. Drizzle-JDBC file which is covered by subject to
 *  the following copyright and notice provisions:
 *
 *  Copyright (c) 2009-2011, Marcus Eriksson
 *
 *  Redistribution and use in source and binary forms, with or without modification,
 *  are permitted provided that the following conditions are met:
 *  Redistributions of source code must retain the above copyright notice, this list
 *  of conditions and the following disclaimer.
 *
 *  Redistributions in binary form must reproduce the above copyright notice, this
 *  list of conditions and the following disclaimer in the documentation and/or
 *  other materials provided with the distribution.
 *
 *  Neither the name of the driver nor the names of its contributors may not be
 *  used to endorse or promote products derived from this software without specific
 *  prior written permission.
 *
 *  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS  AND CONTRIBUTORS "AS IS"
 *  AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 *  WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 *  IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 *  INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 *  NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 *  PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 *  WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 *  ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
 *  OF SUCH DAMAGE.
 */
package com.oceanbase.jdbc;

import java.sql.*;
import java.text.ParseException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.oceanbase.jdbc.internal.ColumnType;
import com.oceanbase.jdbc.internal.com.read.resultset.ColumnDefinition;
import com.oceanbase.jdbc.internal.com.read.resultset.SelectResultSet;
import com.oceanbase.jdbc.internal.io.input.StandardPacketInputStream;
import com.oceanbase.jdbc.internal.protocol.Protocol;
import com.oceanbase.jdbc.internal.util.Utils;
import com.oceanbase.jdbc.internal.util.constant.Version;
import com.oceanbase.jdbc.internal.util.dao.Identifier;
import com.oceanbase.jdbc.util.Options;

public class JDBC4DatabaseMetaData extends OceanBaseOracleDatabaseMetadata {

    public static final String        DRIVER_NAME               = "OceanBase Connector/J";
    private final UrlParser           urlParser;
    private final OceanBaseConnection connection;
    private boolean                   datePrecisionColumnExist  = true;
    Protocol                          protocol                  = null;
    private static final Pattern      RETURN_PATTERN            = Pattern
                                                                    .compile(
                                                                        "\\s*(UNSIGNED\\s+)?(\\w+)\\s*(\\([\\d,]+\\))?\\s*(CHARSET\\s+)?(\\w+)?\\s*",
                                                                        Pattern.CASE_INSENSITIVE);
    private static final Pattern      PARAMETER_PATTERN         = Pattern
                                                                    .compile(
                                                                        "\\s*(IN\\s+|OUT\\s+|INOUT\\s+)?(\\`[\\w\\d\\S]+\\`)\\s+(UNSIGNED\\s+)?(\\w+)\\s*(\\([\\d,]+\\))?\\s*",
                                                                        Pattern.CASE_INSENSITIVE);
    private static final Pattern      ORALCLE_PARAMETER_PATTERN = Pattern
                                                                    .compile(
                                                                        "\\s*(IN\\s+|OUT\\s+|INOUT\\s+)?(\"[\\w\\d\\S]+\")\\s+(UNSIGNED\\s+)?(\\w+)\\s*(\\([\\d,]+\\))?\\s*",
                                                                        Pattern.CASE_INSENSITIVE);
    String[]                          exportKeysColumnNames     = { "PKTABLE_CAT", "PKTABLE_SCHEM",
            "PKTABLE_NAME", "PKCOLUMN_NAME", "FKTABLE_CAT", "FKTABLE_SCHEM", "FKTABLE_NAME",
            "FKCOLUMN_NAME", "KEY_SEQ", "UPDATE_RULE", "DELETE_RULE", "FK_NAME", "PK_NAME",
            "DEFERRABILITY"                                    };
    ColumnType[]                      exportKeysColumnTypes     = { ColumnType.VARCHAR,
            ColumnType.VARCHAR, ColumnType.VARCHAR, ColumnType.VARCHAR, ColumnType.VARCHAR,
            ColumnType.VARCHAR, ColumnType.VARCHAR, ColumnType.VARCHAR, ColumnType.SMALLINT,
            ColumnType.SMALLINT, ColumnType.SMALLINT, ColumnType.VARCHAR, ColumnType.VARCHAR,
            ColumnType.SMALLINT                                };

    /**
     * Constructor.
     *
     * @param connection connection
     * @param urlParser  Url parser
     */
    public JDBC4DatabaseMetaData(Connection connection, UrlParser urlParser) {
        super(urlParser, connection);
        this.connection = (OceanBaseConnection) connection;
        this.urlParser = urlParser;
        this.protocol = this.connection.getProtocol();
    }

    private static String columnTypeClause(Options options) {
        String upperCaseWithoutSize = " UCASE(IF( COLUMN_TYPE LIKE '%(%)%', CONCAT(SUBSTRING( COLUMN_TYPE,1, LOCATE('(',"
                                      + "COLUMN_TYPE) - 1 ), SUBSTRING(COLUMN_TYPE ,1+locate(')', COLUMN_TYPE))), "
                                      + "COLUMN_TYPE))";

        if (options.tinyInt1isBit) {
            String type = null;
            if (options.transformedBitIsBoolean) {
                type = "BOOLEAN";
            } else {
                type = "BIT";
            }

            upperCaseWithoutSize = " IF(COLUMN_TYPE like 'tinyint(1)%', '" + type + "', "
                                   + upperCaseWithoutSize + ")";

        }

        if (!options.yearIsDateType) {
            return " IF(COLUMN_TYPE IN ('year(2)', 'year(4)'), 'SMALLINT', " + upperCaseWithoutSize
                   + ")";
        }

        return upperCaseWithoutSize;
    }

    // Extract identifier quoted string from input String.
    // Return new position, or -1 on error
    private static int skipWhite(char[] part, int startPos) {
        for (int i = startPos; i < part.length; i++) {
            if (!Character.isWhitespace(part[i])) {
                return i;
            }
        }
        return part.length;
    }

    private static int parseIdentifier(char[] part, int startPos, Identifier identifier)
                                                                                        throws ParseException {
        int pos = skipWhite(part, startPos);
        if (part[pos] != '`') {
            throw new ParseException(new String(part), pos);
        }
        pos++;
        StringBuilder sb = new StringBuilder();
        int quotes = 0;
        for (; pos < part.length; pos++) {
            char ch = part[pos];
            if (ch == '`') {
                quotes++;
            } else {
                for (int j = 0; j < quotes / 2; j++) {
                    sb.append('`');
                }
                if (quotes % 2 == 1) {
                    if (ch == '.') {
                        if (identifier.schema != null) {
                            throw new ParseException(new String(part), pos);
                        }
                        identifier.schema = sb.toString();
                        return parseIdentifier(part, pos + 1, identifier);
                    }
                    identifier.name = sb.toString();
                    return pos;
                }
                quotes = 0;
                sb.append(ch);
            }
        }
        throw new ParseException(new String(part), startPos);
    }

    private static int parseIdentifierList(char[] part, int startPos, List<Identifier> list)
                                                                                            throws ParseException {
        int pos = skipWhite(part, startPos);
        if (part[pos] != '(') {
            throw new ParseException(new String(part), pos);
        }
        pos++;
        for (;;) {
            pos = skipWhite(part, pos);
            char ch = part[pos];
            switch (ch) {
                case ')':
                    return pos + 1;
                case '`':
                    Identifier id = new Identifier();
                    pos = parseIdentifier(part, pos, id);
                    list.add(id);
                    break;
                case ',':
                    pos++;
                    break;
                default:
                    throw new ParseException(new String(part, startPos, part.length - startPos),
                        startPos);
            }
        }
    }

    private static int skipKeyword(char[] part, int startPos, String keyword) throws ParseException {
        int pos = skipWhite(part, startPos);
        for (int i = 0; i < keyword.length(); i++, pos++) {
            if (part[pos] != keyword.charAt(i)) {
                throw new ParseException(new String(part), pos);
            }
        }
        return pos;
    }

    private static int getImportedKeyAction(String actionKey) {
        if (actionKey == null) {
            return importedKeyRestrict;
        }
        switch (actionKey) {
            case "NO ACTION":
                return importedKeyNoAction;

            case "CASCADE":
                return importedKeyCascade;

            case "SET NULL":
                return importedKeySetNull;

            case "SET DEFAULT":
                return importedKeySetDefault;

            case "RESTRICT":
                return importedKeyRestrict;

            default:
                throw new IllegalArgumentException("Illegal key action '" + actionKey
                                                   + "' specified.");
        }
    }

    /**
     * Get imported keys.
     *
     * @param tableDef   table definition
     * @param tableName  table name
     * @param catalog    catalog
     * @param connection connection
     * @return resultset resultset
     * @throws ParseException exception
     */
    private static ResultSet getImportedKeys(
            String tableDef, String tableName, String catalog, OceanBaseConnection connection)
            throws ParseException {
        String[] columnNames = {
                "PKTABLE_CAT", "PKTABLE_SCHEM", "PKTABLE_NAME",
                "PKCOLUMN_NAME", "FKTABLE_CAT", "FKTABLE_SCHEM",
                "FKTABLE_NAME", "FKCOLUMN_NAME", "KEY_SEQ",
                "UPDATE_RULE", "DELETE_RULE", "FK_NAME",
                "PK_NAME", "DEFERRABILITY"
        };
        ColumnType[] columnTypes = {
                ColumnType.VARCHAR, ColumnType.NULL, ColumnType.VARCHAR,
                ColumnType.VARCHAR, ColumnType.VARCHAR, ColumnType.NULL,
                ColumnType.VARCHAR, ColumnType.VARCHAR, ColumnType.SMALLINT,
                ColumnType.SMALLINT, ColumnType.SMALLINT, ColumnType.VARCHAR,
                ColumnType.NULL, ColumnType.SMALLINT
        };

        String[] parts = tableDef.split("\n");

        List<String[]> data = new ArrayList<>();

        for (String part : parts) {
            part = part.trim();
            if (!part.startsWith("CONSTRAINT") && !part.contains("FOREIGN KEY")) {
                continue;
            }
            char[] partChar = part.toCharArray();

            Identifier constraintName = new Identifier();

            int pos = skipKeyword(partChar, 0, "CONSTRAINT");
            pos = parseIdentifier(partChar, pos, constraintName);
            pos = skipKeyword(partChar, pos, "FOREIGN KEY");
            List<Identifier> foreignKeyCols = new ArrayList<>();
            pos = parseIdentifierList(partChar, pos, foreignKeyCols);
            pos = skipKeyword(partChar, pos, "REFERENCES");
            Identifier pkTable = new Identifier();
            pos = parseIdentifier(partChar, pos, pkTable);
            List<Identifier> primaryKeyCols = new ArrayList<>();
            parseIdentifierList(partChar, pos, primaryKeyCols);
            if (primaryKeyCols.size() != foreignKeyCols.size()) {
                throw new ParseException(tableDef, 0);
            }
            int onUpdateReferenceAction = importedKeyRestrict;
            int onDeleteReferenceAction = importedKeyRestrict;

            for (String referenceAction : new String[]{"RESTRICT", "CASCADE", "SET NULL", "NO ACTION"}) {
                if (part.contains("ON UPDATE " + referenceAction)) {
                    onUpdateReferenceAction = getImportedKeyAction(referenceAction);
                }
                if (part.contains("ON DELETE " + referenceAction)) {
                    onDeleteReferenceAction = getImportedKeyAction(referenceAction);
                }
            }

            for (int i = 0; i < primaryKeyCols.size(); i++) {

                String[] row = new String[columnNames.length];
                row[0] = pkTable.schema;
                if (row[0] == null) {
                    row[0] = catalog;
                }
                row[1] = null;
                row[2] = pkTable.name;
                row[3] = primaryKeyCols.get(i).name;
                row[4] = catalog;
                row[5] = null;
                row[6] = tableName;
                row[7] = foreignKeyCols.get(i).name;
                row[8] = Integer.toString(i + 1);
                row[9] = Integer.toString(onUpdateReferenceAction);
                row[10] = Integer.toString(onDeleteReferenceAction);
                row[11] = constraintName.name;
                row[12] = null;
                row[13] = Integer.toString(importedKeyNotDeferrable);
                data.add(row);
            }
        }
        String[][] arr = data.toArray(new String[0][]);

        /* Sort array by PKTABLE_CAT, PKTABLE_NAME, and KEY_SEQ.*/
        Arrays.sort(
                arr,
                (row1, row2) -> {
                    int result = row1[0].compareTo(row2[0]); // PKTABLE_CAT
                    if (result == 0) {
                        result = row1[2].compareTo(row2[2]); // PKTABLE_NAME
                        if (result == 0) {
                            result = row1[8].length() - row2[8].length(); // KEY_SEQ
                            if (result == 0) {
                                result = row1[8].compareTo(row2[8]);
                            }
                        }
                    }
                    return result;
                });
        return SelectResultSet.createResultSet(columnNames, columnTypes, arr, connection.getProtocol());
    }

    /**
     * Retrieves a description of the primary key columns that are referenced by the given table's
     * foreign key columns (the primary keys imported by a table). They are ordered by PKTABLE_CAT,
     * PKTABLE_SCHEM, PKTABLE_NAME, and KEY_SEQ.
     *
     * <p>Each primary key column description has the following columns:
     *
     * <OL>
     * <LI><B>PKTABLE_CAT</B> String {@code =>} primary key table catalog being imported (may be
     * <code>null</code>)
     * <LI><B>PKTABLE_SCHEM</B> String {@code =>} primary key table schema being imported (may be
     * <code>null</code>)
     * <LI><B>PKTABLE_NAME</B> String {@code =>} primary key table name being imported
     * <LI><B>PKCOLUMN_NAME</B> String {@code =>} primary key column name being imported
     * <LI><B>FKTABLE_CAT</B> String {@code =>} foreign key table catalog (may be <code>null</code>)
     * <LI><B>FKTABLE_SCHEM</B> String {@code =>} foreign key table schema (may be <code>null</code>
     * )
     * <LI><B>FKTABLE_NAME</B> String {@code =>} foreign key table name
     * <LI><B>FKCOLUMN_NAME</B> String {@code =>} foreign key column name
     * <LI><B>KEY_SEQ</B> short {@code =>} sequence number within a foreign key( a value of 1
     * represents the first column of the foreign key, a value of 2 would represent the second
     * column within the foreign key).
     * <LI><B>UPDATE_RULE</B> short {@code =>} What happens to a foreign key when the primary key is
     * updated:
     * <UL>
     * <LI>importedNoAction - do not allow update of primary key if it has been imported
     * <LI>importedKeyCascade - change imported key to agree with primary key update
     * <LI>importedKeySetNull - change imported key to <code>NULL</code> if its primary key
     * has been updated
     * <LI>importedKeySetDefault - change imported key to default values if its primary key
     * has been updated
     * <LI>importedKeyRestrict - same as importedKeyNoAction (for ODBC 2.x compatibility)
     * </UL>
     * <LI><B>DELETE_RULE</B> short {@code =>} What happens to the foreign key when primary is
     * deleted.
     * <UL>
     * <LI>importedKeyNoAction - do not allow delete of primary key if it has been imported
     * <LI>importedKeyCascade - delete rows that import a deleted key
     * <LI>importedKeySetNull - change imported key to NULL if its primary key has been
     * deleted
     * <LI>importedKeyRestrict - same as importedKeyNoAction (for ODBC 2.x compatibility)
     * <LI>importedKeySetDefault - change imported key to default if its primary key has been
     * deleted
     * </UL>
     * <LI><B>FK_NAME</B> String {@code =>} foreign key name (may be <code>null</code>)
     * <LI><B>PK_NAME</B> String {@code =>} primary key name (may be <code>null</code>)
     * <LI><B>DEFERRABILITY</B> short {@code =>} can the evaluation of foreign key constraints be
     * deferred until commit
     * <UL>
     * <LI>importedKeyInitiallyDeferred - see SQL92 for definition
     * <LI>importedKeyInitiallyImmediate - see SQL92 for definition
     * <LI>importedKeyNotDeferrable - see SQL92 for definition
     * </UL>
     * </OL>
     *
     * @param catalog a catalog name; must match the catalog name as it is stored in the database; ""
     *                retrieves those without a catalog; <code>null</code> means that the catalog name should not
     *                be used to narrow the search
     * @param schema  a schema name; must match the schema name as it is stored in the database; ""
     *                retrieves those without a schema; <code>null</code> means that the schema name should not
     *                be used to narrow the search
     * @param table   a table name; must match the table name as it is stored in the database
     * @return <code>ResultSet</code> - each row is a primary key column description
     * @throws SQLException if a database access error occurs
     * @see #getExportedKeys
     */
    public ResultSet getImportedKeys(String catalog, String schema, String table)
                                                                                 throws SQLException {
        String database = catalog;
        // We avoid using information schema queries by default, because this appears to be an expensive
        // query (CONJ-41).

        if (this.connection.getProtocol().isOracleMode()) {
            return this.keysQuery((String) null, (String) null, schema, table,
                "ORDER BY pktable_schem, pktable_name, key_seq");
        }
        ArrayList<String> tableNameList;
        try {
            tableNameList = getALlTableNames(catalog);
            ResultSet allTableRS = getAllTablesResultSet(tableNameList, catalog);
            ArrayList<String[]> list = new ArrayList<>();
            while (allTableRS.next()) {
                String[] data = new String[14];
                String tableType = allTableRS.getString("Type");
                if (tableType != null && (tableType.toUpperCase(Locale.ROOT).equals("INNODB") || tableType.toUpperCase(Locale.ROOT).equals("SUPPORTS_FK"))) {
                    String tableComment = allTableRS.getString("Comment").trim();
                    String fkTableName = allTableRS.getString("Name");
                    if (tableComment != null) {
                        StringTokenizer stringTokenizer = new StringTokenizer(tableComment, ";", false);
                        if (stringTokenizer.hasMoreTokens()) {
                            String dummy = stringTokenizer.nextToken();
                            // Skip InnoDB comment
                        }
                        while (stringTokenizer.hasMoreTokens()) {
                            String keys = stringTokenizer.nextToken();
                            int fkParamOpenIndex = keys.indexOf("(");
                            if (fkParamOpenIndex == -1) {
                                throw new SQLException("Error parsing foreign keys definition, couldn't find start of local columns list.");
                            }
                            String constraintName = keys.substring(1, fkParamOpenIndex);
                            keys = keys.substring(fkParamOpenIndex);
                            int fkParamCloseIndex = keys.indexOf(")");
                            if (fkParamCloseIndex == -1) {
                                throw new SQLException("Error parsing foreign keys definition, couldn't find start of local columns list.");
                            }
                            String fkParamNames = keys.substring(1, fkParamCloseIndex);
                            int refIndex = keys.indexOf("REFER");
                            if (refIndex == -1) {
                                throw new SQLException("Error parsing foreign keys definition, couldn't find start of referenced tables list");
                            }
                            int refParamOpenIndex = keys.indexOf("(", refIndex);
                            if (refParamOpenIndex == -1) {
                                throw new SQLException("Error parsing foreign keys definition, couldn't find start of referenced columns list");
                            }
                            String refCatalogAndTable = keys.substring(refIndex + "REFER".length() + 1, refParamOpenIndex);
                            int slashIndex = refCatalogAndTable.indexOf("/");
                            if (slashIndex == -1) {
                                throw new SQLException("Error parsing foreign keys definition, couldn't find name of referenced catalog.");
                            }
                            String refCatalog = refCatalogAndTable.substring(0, slashIndex);
                            refCatalog = removeQuoted(refCatalog);
                            String refTableName = refCatalogAndTable.substring(slashIndex + 1);
                            refTableName = removeQuoted(refTableName);
                            if (fkTableName.compareTo(table) != 0) {
                                continue;
                            }
                            int refParamCloseIndex = keys.indexOf(")", refParamOpenIndex);
                            if (refParamCloseIndex == -1) {
                                throw new SQLException("Error parsing foreign keys definition, couldn't find end of referenced columns list.");
                            }
                            String refParamNames = keys.substring(refParamOpenIndex + 1, refParamCloseIndex);
                            String[] refParamList = refParamNames.split(",");
                            String[] fkParamList = fkParamNames.split(",");
                            int fkCur = 0;
                            int refCur = 0;
                            int keySeq = 1;
                            while (fkCur < fkParamList.length) {
                                String lColumnName = removeQuoted(fkParamList[fkCur++]);
                                String rColumnName = null;
                                if (refCur < refParamList.length) {
                                    rColumnName = removeQuoted(refParamList[refCur++]);
                                }
                                int updateRuleAction = importedKeyNoAction;
                                int deleteRuleAction = importedKeyNoAction;
                                int lastIndex = keys.lastIndexOf(")");
                                if (lastIndex != keys.length() - 1) {
                                    String options = keys.substring(lastIndex + 1);
                                    String optionsForUpdate = options.substring(options.indexOf("ON UPDATE"));
                                    if (optionsForUpdate.startsWith("ON UPDATE CASCADE")) {
                                        updateRuleAction = importedKeyCascade;
                                    } else if (optionsForUpdate.startsWith("ON UPDATE SET NUL")) {
                                        updateRuleAction = importedKeySetNull;
                                    } else if (optionsForUpdate.startsWith("ON UPDATE RESTRICT")) {
                                        updateRuleAction = importedKeyRestrict;
                                    } else if (optionsForUpdate.startsWith("ON UPDATE NO ACTION")) {
                                        updateRuleAction = importedKeyNoAction;
                                    }
                                    String optionsForDelete = options.substring(options.indexOf("ON DELETE"));
                                    if (optionsForDelete.startsWith("ON DELETE CASCADE")) {
                                        deleteRuleAction = importedKeyCascade;
                                    } else if (optionsForDelete.startsWith("ON DELETE SET NUL")) {
                                        deleteRuleAction = importedKeySetNull;
                                    } else if (optionsForDelete.startsWith("ON DELETE RESTRICT")) {
                                        deleteRuleAction = importedKeyRestrict;
                                    } else if (optionsForDelete.startsWith("ON DELETE NO ACTIO")) {
                                        deleteRuleAction = importedKeyNoAction;
                                    }
                                }
                                data[0] = refCatalog;
                                data[1] = null;
                                data[2] = refTableName;
                                data[3] = rColumnName;
                                data[4] = catalog == null ? this.protocol.getCatalog(): catalog;
                                data[5] = null;
                                data[6] = table;
                                data[7] = lColumnName;
                                data[8] = Integer.toString(keySeq++);
                                data[9] = Integer.toString(updateRuleAction);
                                data[10] = Integer.toString(deleteRuleAction);
                                data[11] = constraintName;
                                data[12] = null;
                                data[13] = Integer.toString(importedKeyNotDeferrable);
                                list.add(data);
                                data = new String[14]; // reset the date
                            }
                        }
                    }
                }

            }
            String[][] val = new String[list.size()][];
            for (int j = 0; j < list.size(); j++) {
                val[j] = list.get(j);
            }
            return SelectResultSet.createResultSet(exportKeysColumnNames, exportKeysColumnTypes, val, this.protocol);
        } catch (SQLException e) {
            throw  e;
        }
    }

    private String dataTypeClause(String fullTypeColumnName) {
        Options options = urlParser.getOptions();
        return " CASE data_type" + " WHEN 'bit' THEN "
               + Types.BIT
               + " WHEN 'tinyblob' THEN "
               + Types.VARBINARY
               + " WHEN 'mediumblob' THEN "
               + Types.LONGVARBINARY
               + " WHEN 'longblob' THEN "
               + Types.LONGVARBINARY
               + " WHEN 'blob' THEN "
               + Types.LONGVARBINARY
               + " WHEN 'tinytext' THEN "
               + Types.VARCHAR
               + " WHEN 'mediumtext' THEN "
               + Types.LONGVARCHAR
               + " WHEN 'longtext' THEN "
               + Types.LONGVARCHAR
               + " WHEN 'text' THEN "
               + Types.LONGVARCHAR
               + " WHEN 'date' THEN "
               + Types.DATE
               + " WHEN 'datetime' THEN "
               + Types.TIMESTAMP
               + " WHEN 'decimal' THEN "
               + Types.DECIMAL
               + " WHEN 'double' THEN "
               + Types.DOUBLE
               + " WHEN 'enum' THEN "
               + Types.VARCHAR
               + " WHEN 'float' THEN "
               + Types.REAL
               + " WHEN 'int' THEN IF( "
               + fullTypeColumnName
               + " like '%unsigned%', "
               + Types.INTEGER
               + ","
               + Types.INTEGER
               + ")"
               + " WHEN 'bigint' THEN "
               + Types.BIGINT
               + " WHEN 'mediumint' THEN "
               + Types.INTEGER
               + " WHEN 'null' THEN "
               + Types.NULL
               + " WHEN 'set' THEN "
               + Types.VARCHAR
               + " WHEN 'smallint' THEN IF( "
               + fullTypeColumnName
               + " like '%unsigned%', "
               + Types.SMALLINT
               + ","
               + Types.SMALLINT
               + ")"
               + " WHEN 'varchar' THEN "
               + Types.VARCHAR
               + " WHEN 'varbinary' THEN "
               + Types.VARBINARY
               + " WHEN 'char' THEN "
               + Types.CHAR
               + " WHEN 'binary' THEN "
               + Types.BINARY
               + " WHEN 'time' THEN "
               + Types.TIME
               + " WHEN 'timestamp' THEN "
               + Types.TIMESTAMP
               + " WHEN 'tinyint' THEN "
               + (options.tinyInt1isBit ? "IF("
                                          + fullTypeColumnName
                                          + " like 'tinyint(1)%',"
                                          + (options.transformedBitIsBoolean ? Types.BOOLEAN
                                              : Types.BIT) + "," + Types.TINYINT + ") "
                   : Types.TINYINT) + " WHEN 'year' THEN "
               + (options.yearIsDateType ? Types.DATE : Types.SMALLINT) + " ELSE " + Types.OTHER
               + " END ";
    }

    private String escapeQuote(String value) {
        if (value == null) {
            return "NULL";
        }
        return "'" + Utils.escapeString(value, connection.getProtocol().noBackslashEscapes()) + "'";
    }

    /**
     * Generate part of the information schema query that restricts catalog names In the driver,
     * catalogs is the equivalent to MariaDB schemas.
     *
     * @param columnName - column name in the information schema table
     * @param catalog    - catalog name. This driver does not (always) follow JDBC standard for following
     *                   special values, due to ConnectorJ compatibility 1. empty string ("") - matches current
     *                   catalog (i.e database). JDBC standard says only tables without catalog should be returned -
     *                   such tables do not exist in MariaDB. If there is no current catalog, then empty string
     *                   matches any catalog. 2. null - if nullCatalogMeansCurrent=true (which is the default), then
     *                   the handling is the same as for "" . i.e return current catalog.JDBC-conforming way would
     *                   be to match any catalog with null parameter. This can be switched with
     *                   nullCatalogMeansCurrent=false in the connection URL.
     * @return part of SQL query ,that restricts search for the catalog.
     */
    private String catalogCond(String columnName, String catalog) {
        if (catalog == null) {
            /* Treat null catalog as current */
            if (connection.nullCatalogMeansCurrent) {
                return "(ISNULL(database()) OR (" + columnName + " = database()))";
            }
            return "(1 = 1)";
        }
        if (catalog.isEmpty()) {
            return "(ISNULL(database()) OR (" + columnName + " = database()))";
        }
        return "(" + columnName + " = " + escapeQuote(catalog) + ")";
    }

    // Helper to generate  information schema queries with "like" or "equals" condition (typically  on
    // table name)
    private String patternCond(String columnName, String tableName) {
        if (tableName == null) {
            return "";
        }
        String predicate = (tableName.indexOf('%') == -1 && tableName.indexOf('_') == -1) ? "="
            : "LIKE";
        return " AND " + columnName + " " + predicate + " '" + Utils.escapeString(tableName, true)
               + "' ";
    }

    /**
     * Retrieves a description of the given table's primary key columns. They are ordered by
     * COLUMN_NAME.
     *
     * <p>Each primary key column description has the following columns:
     *
     * <OL>
     * <li><B>TABLE_CAT</B> String {@code =>} table catalog
     * <li><B>TABLE_SCHEM</B> String {@code =>} table schema (may be <code>null</code>)
     * <li><B>TABLE_NAME</B> String {@code =>} table name
     * <li><B>COLUMN_NAME</B> String {@code =>} column name
     * <li><B>KEY_SEQ</B> short {@code =>} sequence number within primary key( a value of 1
     * represents the first column of the primary key, a value of 2 would represent the second
     * column within the primary key).
     * <li><B>PK_NAME</B> String {@code =>} primary key name
     * </OL>
     *
     * @param catalog a catalog name; must match the catalog name as it is stored in the database; ""
     *                retrieves those without a catalog; <code>null</code> means that the catalog name should not
     *                be used to narrow the search
     * @param schema  a schema name; must match the schema name as it is stored in the database; ""
     *                retrieves those without a schema; <code>null</code> means that the schema name should not
     *                be used to narrow the search
     * @param table   a table name; must match the table name as it is stored in the database
     * @return <code>ResultSet</code> - each row is a primary key column description
     * @throws SQLException if a database access error occurs
     */
    public ResultSet getPrimaryKeys(String catalog, String schema, String table)
                                                                                throws SQLException {
        if (this.connection.getProtocol().isOracleMode()) {
            return super.getPrimaryKeys(catalog, schema, table);
        }
        // MySQL 8 now use 'PRI' in place of 'pri'
        String sql = "SELECT A.TABLE_SCHEMA TABLE_CAT, NULL TABLE_SCHEM, A.TABLE_NAME, A.COLUMN_NAME, B.SEQ_IN_INDEX KEY_SEQ, B.INDEX_NAME PK_NAME "
                     + " FROM INFORMATION_SCHEMA.COLUMNS A, INFORMATION_SCHEMA.STATISTICS B"
                     + " WHERE A.COLUMN_KEY in ('PRI','pri') AND B.INDEX_NAME='PRIMARY' "
                     + " AND "
                     + catalogCond("A.TABLE_SCHEMA", catalog)
                     + " AND "
                     + catalogCond("B.TABLE_SCHEMA", catalog)
                     + patternCond("A.TABLE_NAME", table)
                     + patternCond("B.TABLE_NAME", table)
                     + " AND A.TABLE_SCHEMA = B.TABLE_SCHEMA AND A.TABLE_NAME = B.TABLE_NAME AND A.COLUMN_NAME = B.COLUMN_NAME "
                     + " ORDER BY A.COLUMN_NAME";

        return executeQuery(sql);
    }

    /**
     * Retrieves a description of the tables available in the given catalog. Only table descriptions
     * matching the catalog, schema, table name and type criteria are returned. They are ordered by
     * <code>TABLE_TYPE</code>, <code>TABLE_CAT</code>, <code>TABLE_SCHEM</code> and <code>TABLE_NAME
     * </code>. Each table description has the following columns:
     *
     * <OL>
     * <LI><B>TABLE_CAT</B> String {@code =>} table catalog (may be <code>null</code>)
     * <LI><B>TABLE_SCHEM</B> String {@code =>} table schema (may be <code>null</code>)
     * <LI><B>TABLE_NAME</B> String {@code =>} table name
     * <LI><B>TABLE_TYPE</B> String {@code =>} table type. Typical types are "TABLE", "VIEW",
     * "SYSTEM TABLE", "GLOBAL TEMPORARY", "LOCAL TEMPORARY", "ALIAS", "SYNONYM".
     * <LI><B>REMARKS</B> String {@code =>} explanatory comment on the table
     * <LI><B>TYPE_CAT</B> String {@code =>} the types catalog (may be <code>null</code>)
     * <LI><B>TYPE_SCHEM</B> String {@code =>} the types schema (may be <code>null</code>)
     * <LI><B>TYPE_NAME</B> String {@code =>} type name (may be <code>null</code>)
     * <LI><B>SELF_REFERENCING_COL_NAME</B> String {@code =>} name of the designated "identifier"
     * column of a typed table (may be <code>null</code>)
     * <LI><B>REF_GENERATION</B> String {@code =>} specifies how values in SELF_REFERENCING_COL_NAME
     * are created. Values are "SYSTEM", "USER", "DERIVED". (may be <code>null</code>)
     * </OL>
     *
     * <p><B>Note:</B> Some databases may not return information for all tables.
     *
     * @param catalog          a catalog name; must match the catalog name as it is stored in the database; ""
     *                         retrieves those without a catalog; <code>null</code> means that the catalog name should not
     *                         be used to narrow the search
     * @param schemaPattern    a schema name pattern; must match the schema name as it is stored in the
     *                         database; "" retrieves those without a schema; <code>null</code> means that the schema name
     *                         should not be used to narrow the search
     * @param tableNamePattern a table name pattern; must match the table name as it is stored in the
     *                         database
     * @param types            a list of table types, which must be from the list of table types returned from
     *                         {@link #getTableTypes},to include; <code>null</code> returns all types
     * @return <code>ResultSet</code> - each row is a table description
     * @throws SQLException if a database access error occurs
     * @see #getSearchStringEscape
     */
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern,
                               String[] types) throws SQLException {

        if (connection.getProtocol().isOracleMode()) {
            return super.getTables(catalog, schemaPattern, tableNamePattern, types);
        }

        String[] columnNames = { "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE", "REMARKS",
                "TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "SELF_REFERENCING_COL_NAME",
                "REF_GENERATION" };

        ColumnType[] columnTypes = { ColumnType.VARCHAR, ColumnType.VARCHAR, ColumnType.VARCHAR,
                ColumnType.VARCHAR, ColumnType.VARCHAR, ColumnType.VARCHAR, ColumnType.VARCHAR,
                ColumnType.VARCHAR, ColumnType.VARCHAR, ColumnType.VARCHAR, };
        ResultSet rs = getTablesImpl(columnTypes, columnNames, tableNamePattern, types, catalog);
        return rs;
    }

    ResultSet getTablesImpl(ColumnType[] columnTypes, String[] columnNames,
                            String tableNamePattern, String[] types, String catalog)
                                                                                    throws SQLException {
        boolean isSystemDb = "information_schema".equalsIgnoreCase(catalog)
                             || "mysql".equalsIgnoreCase(catalog)
                             || "performance_schema".equalsIgnoreCase(catalog);
        if (catalog == null || catalog.length() == 0) {
            catalog = this.connection.getCatalog();
        }
        StringBuilder getTablesSql = new StringBuilder();
        getTablesSql.append("SHOW FULL TABLES FROM ");
        getTablesSql.append("`");
        getTablesSql.append(catalog);
        getTablesSql.append("` LIKE '");
        if (tableNamePattern == null) {
            getTablesSql.append("%");
        } else {
            getTablesSql.append(tableNamePattern);
        }
        getTablesSql.append("'");
        ResultSet rs = executeQuery(getTablesSql.toString());
        boolean isTables = false;
        boolean isViews = false;
        boolean isSystemTables = false;
        boolean isSystemViews = false;
        boolean isLocalTemporaries = false;
        if (types == null || types.length == 0) {
            isTables = true;
            isViews = true;
            isSystemTables = true;
            isSystemViews = true;
            isLocalTemporaries = true;
        } else {
            for (int i = 0; i < types.length; i++) {
                if ("TABLE".equals(types[i]) || "BASE TABLE".equals(types[i])) {
                    isTables = true;
                } else if ("VIEW".equals(types[i])) {
                    isViews = true;
                } else if ("SYSTEM TABLE".equals(types[i])) {
                    isSystemTables = true;
                } else if ("SYSTEM VIEW".equals(types[i])) {
                    isSystemViews = true;
                } else if ("LOCAL TEMPORARY".equals(types[i])) {
                    isLocalTemporaries = true;
                }
            }
        }
        int columnIndex = 1;
        boolean hasTableType = true;
        columnIndex = rs.findColumn("table_type");
        SortedMap<TableMetaData, String[]> map = new TreeMap<TableMetaData, String[]>();
        while (rs.next()) {
            String[] data = new String[10];
            boolean shouldPut = false;
            data[0] = catalog;
            data[1] = null;
            data[2] = rs.getString(1);
            data[4] = new byte[0].toString();
            data[5] = null;
            data[6] = null;
            data[7] = null;
            data[8] = null;
            data[9] = null;
            TableMetaData key = null;
            if (hasTableType) {
                String tableType = rs.getString(columnIndex);
                String typeName = null;
                if ("TABLE".equals(tableType) || "BASE TABLE".equals(tableType)) {

                    if (isSystemDb && isSystemTables) {
                        shouldPut = true;
                        key = new TableMetaData("SYSTEM TABLE", catalog, null, rs.getString(1));
                    } else if (!isSystemDb && isTables) {
                        shouldPut = true;
                        key = new TableMetaData("TABLE", catalog, null, rs.getString(1));
                    }
                    if (shouldPut) {
                        data[3] = "TABLE";
                        map.put(key, data);
                    }
                } else if ("VIEW".equals(tableType)) {
                    if (isViews) {
                        key = new TableMetaData("VIEW", catalog, null, rs.getString(1));
                        data[3] = "VIEW";
                        map.put(key, data);
                    }
                } else if ("SYSTEM VIEW".equals(tableType)) {
                    if (isSystemViews) {
                        key = new TableMetaData("SYSTEM VIEW", catalog, null, rs.getString(1));
                        data[3] = "SYSTEM VIEW";
                        map.put(key, data);
                    }
                } else if ("SYSTEM TABLE".equals(tableType)) {
                    if (isSystemTables) {
                        key = new TableMetaData("SYSTEM TABLE", catalog, null, rs.getString(1));
                        data[3] = "SYSTEM TABLE";
                        map.put(key, data);
                    }
                } else if ("LOCAL TEMPORARY".equals(tableType)) {
                    if (isLocalTemporaries) {
                        key = new TableMetaData("LOCAL TEMPORARY", catalog, null, rs.getString(1));
                        data[3] = "LOCAL TEMPORARY";
                        map.put(key, data);
                    }
                } else {
                    key = new TableMetaData("TABLE", catalog, null, rs.getString(1));
                    data[3] = "TABLE";
                    map.put(key, data);
                }
            } else {
                key = new TableMetaData("TABLE", catalog, null, rs.getString(1));
                data[3] = "TABLE";
                map.put(key, data);
            }
        }
        Object[] a = map.values().toArray();
        String[][] val = new String[map.size()][];

        for (int j = 0; j < a.length; j++) {
            val[j] = (String[]) a[j];
        }
        return JDBC4ResultSet.createResultSet(columnNames, columnTypes, val, this.protocol);
    }

    protected class TableMetaData implements Comparable<TableMetaData> {
        String type;
        String catalog;
        String schema;
        String name;

        TableMetaData(String type, String catalog, String schema, String name) {
            this.type = type == null ? "" : type;
            this.catalog = catalog == null ? "" : catalog;
            this.schema = schema == null ? "" : schema;
            this.name = name == null ? "" : name;
        }

        public int compareTo(TableMetaData tablesKey) {
            int ret;
            if ((ret = this.type.compareTo(tablesKey.type)) != 0) {
                return ret;
            }
            if ((ret = this.catalog.compareTo(tablesKey.catalog)) != 0) {
                return ret;
            }
            if ((ret = this.schema.compareTo(tablesKey.schema)) != 0) {
                return ret;
            }
            return this.name.compareTo(tablesKey.name);
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof TableMetaData)) {
                return false;
            }
            if (obj == null) {
                return false;
            }
            if (obj == this) {
                return true;
            }
            return compareTo((TableMetaData) obj) == 0;
        }
    }

    /**
     * Retrieves a description of table columns available in the specified catalog.
     *
     * <p>Only column descriptions matching the catalog, schema, table and column name criteria are
     * returned. They are ordered by <code>TABLE_CAT</code>,<code>TABLE_SCHEM</code>, <code>TABLE_NAME
     * </code>, and <code>ORDINAL_POSITION</code>.
     *
     * <p>Each column description has the following columns:
     *
     * <OL>
     * <LI><B>TABLE_CAT</B> String {@code =>} table catalog (may be <code>null</code>)
     * <LI><B>TABLE_SCHEM</B> String {@code =>} table schema (may be <code>null</code>)
     * <LI><B>TABLE_NAME</B> String {@code =>} table name
     * <LI><B>COLUMN_NAME</B> String {@code =>} column name
     * <LI><B>DATA_TYPE</B> int {@code =>} SQL type from java.sql.Types
     * <LI><B>TYPE_NAME</B> String {@code =>} Data source dependent type name, for a UDT the type
     * name is fully qualified
     * <LI><B>COLUMN_SIZE</B> int {@code =>} column size.
     * <LI><B>BUFFER_LENGTH</B> is not used.
     * <LI><B>DECIMAL_DIGITS</B> int {@code =>} the number of fractional digits. Null is returned
     * for data types where DECIMAL_DIGITS is not applicable.
     * <LI><B>NUM_PREC_RADIX</B> int {@code =>} Radix (typically either 10 or 2)
     * <LI><B>NULLABLE</B> int {@code =>} is NULL allowed.
     * <UL>
     * <LI>columnNoNulls - might not allow <code>NULL</code> values
     * <LI>columnNullable - definitely allows <code>NULL</code> values
     * <LI>columnNullableUnknown - nullability unknown
     * </UL>
     * <LI><B>REMARKS</B> String {@code =>} comment describing column (may be <code>null</code>)
     * <LI><B>COLUMN_DEF</B> String {@code =>} default value for the column, which should be
     * interpreted as a string when the value is enclosed in single quotes (may be <code>null
     * </code>)
     * <LI><B>SQL_DATA_TYPE</B> int {@code =>} unused
     * <LI><B>SQL_DATETIME_SUB</B> int {@code =>} unused
     * <LI><B>CHAR_OCTET_LENGTH</B> int {@code =>} for char types the maximum number of bytes in the
     * column
     * <LI><B>ORDINAL_POSITION</B> int {@code =>} index of column in table (starting at 1)
     * <LI><B>IS_NULLABLE</B> String {@code =>} ISO rules are used to determine the nullability for
     * a column.
     * <UL>
     * <LI>YES --- if the column can include NULLs
     * <LI>NO --- if the column cannot include NULLs
     * <LI>empty string --- if the nullability for the column is unknown
     * </UL>
     * <LI><B>SCOPE_CATALOG</B> String {@code =>} catalog of table that is the scope of a reference
     * attribute (<code>null</code> if DATA_TYPE isn't REF)
     * <LI><B>SCOPE_SCHEMA</B> String {@code =>} schema of table that is the scope of a reference
     * attribute (<code>null</code> if the DATA_TYPE isn't REF)
     * <LI><B>SCOPE_TABLE</B> String {@code =>} table name that this the scope of a reference
     * attribute (<code>null</code> if the DATA_TYPE isn't REF)
     * <LI><B>SOURCE_DATA_TYPE</B> short {@code =>} source type of a distinct type or user-generated
     * Ref type, SQL type from java.sql.Types (<code>null</code> if DATA_TYPE isn't DISTINCT or
     * user-generated REF)
     * <LI><B>IS_AUTOINCREMENT</B> String {@code =>} Indicates whether this column is auto
     * incremented
     * <UL>
     * <LI>YES --- if the column is auto incremented
     * <LI>NO --- if the column is not auto incremented
     * <LI>empty string --- if it cannot be determined whether the column is auto incremented
     * </UL>
     * <LI><B>IS_GENERATEDCOLUMN</B> String {@code =>} Indicates whether this is a generated column
     * <UL>
     * <LI>YES --- if this a generated column
     * <LI>NO --- if this not a generated column
     * <LI>empty string --- if it cannot be determined whether this is a generated column
     * </UL>
     * </OL>
     *
     * <p>The COLUMN_SIZE column specifies the column size for the given column. For numeric data,
     * this is the maximum precision. For character data, this is the length in characters. For
     * datetime datatypes, this is the length in characters of the String representation (assuming the
     * maximum allowed precision of the fractional seconds component). For binary data, this is the
     * length in bytes. For the ROWID datatype, this is the length in bytes. Null is returned for data
     * types where the column size is not applicable.
     *
     * @param catalog           a catalog name; must match the catalog name as it is stored in the database; ""
     *                          retrieves those without a catalog; <code>null</code> means that the catalog name should not
     *                          be used to narrow the search
     * @param schemaPattern     a schema name pattern; must match the schema name as it is stored in the
     *                          database; "" retrieves those without a schema; <code>null</code> means that the schema name
     *                          should not be used to narrow the search
     * @param tableNamePattern  a table name pattern; must match the table name as it is stored in the
     *                          database
     * @param columnNamePattern a column name pattern; must match the column name as it is stored in
     *                          the database
     * @return <code>ResultSet</code> - each row is a column description
     * @throws SQLException if a database access error occurs
     * @see #getSearchStringEscape
     */
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern,
                                String columnNamePattern) throws SQLException {
        if (connection.getProtocol().isOracleMode()) {
            return super.getColumns(catalog, schemaPattern, tableNamePattern == null ? null
                : tableNamePattern, columnNamePattern == null ? null : columnNamePattern);
        }
        Options options = urlParser.getOptions();
        String sql = "SELECT TABLE_SCHEMA TABLE_CAT, NULL TABLE_SCHEM, TABLE_NAME, COLUMN_NAME,"
                     + dataTypeClause("COLUMN_TYPE")
                     + " DATA_TYPE,"
                     + columnTypeClause(options)
                     + " TYPE_NAME, "
                     + " CASE DATA_TYPE"
                     + "  WHEN 'time' THEN "
                     + (datePrecisionColumnExist ? "IF(DATETIME_PRECISION = 0, 10, CAST(11 + DATETIME_PRECISION as signed integer))"
                         : "10")
                     + "  WHEN 'date' THEN 10"
                     + "  WHEN 'datetime' THEN "
                     + (datePrecisionColumnExist ? "IF(DATETIME_PRECISION = 0, 19, CAST(20 + DATETIME_PRECISION as signed integer))"
                         : "19")
                     + "  WHEN 'timestamp' THEN "
                     + (datePrecisionColumnExist ? "IF(DATETIME_PRECISION = 0, 19, CAST(20 + DATETIME_PRECISION as signed integer))"
                         : "19")
                     + (options.yearIsDateType ? "" : " WHEN 'year' THEN 5")
                     + "  ELSE "
                     + "  IF(NUMERIC_PRECISION IS NULL, LEAST(CHARACTER_MAXIMUM_LENGTH,"
                     + Integer.MAX_VALUE
                     + "), NUMERIC_PRECISION) "
                     + " END"
                     + " COLUMN_SIZE, 65535 BUFFER_LENGTH, "
                     + " CONVERT (CASE DATA_TYPE"
                     + " WHEN 'year' THEN "
                     + (options.yearIsDateType ? "NUMERIC_SCALE" : "0")
                     + " WHEN 'tinyint' THEN "
                     + (options.tinyInt1isBit ? "0" : "NUMERIC_SCALE")
                     + " ELSE NUMERIC_SCALE END, UNSIGNED INTEGER) DECIMAL_DIGITS,"
                     + " 10 NUM_PREC_RADIX, IF(IS_NULLABLE = 'yes',1,0) NULLABLE,COLUMN_COMMENT REMARKS,"
                     + " COLUMN_DEFAULT COLUMN_DEF, 0 SQL_DATA_TYPE, 0 SQL_DATETIME_SUB,  "
                     + " LEAST(CHARACTER_OCTET_LENGTH,"
                     + Integer.MAX_VALUE
                     + ") CHAR_OCTET_LENGTH,"
                     + " ORDINAL_POSITION, IS_NULLABLE, NULL SCOPE_CATALOG, NULL SCOPE_SCHEMA, NULL SCOPE_TABLE, NULL SOURCE_DATA_TYPE,"
                     + " IF(EXTRA = 'auto_increment','YES','NO') IS_AUTOINCREMENT, "
                     + " IF(EXTRA in ('VIRTUAL', 'PERSISTENT', 'VIRTUAL GENERATED', 'STORED GENERATED') ,'YES','NO') IS_GENERATEDCOLUMN "
                     + " FROM INFORMATION_SCHEMA.COLUMNS  WHERE "
                     + catalogCond("TABLE_SCHEMA", catalog)
                     + patternCond("TABLE_NAME", tableNamePattern)
                     + patternCond("COLUMN_NAME", columnNamePattern)
                     + " ORDER BY TABLE_CAT, TABLE_SCHEM, TABLE_NAME, ORDINAL_POSITION";

        try {
            return executeQuery(sql);
        } catch (SQLException sqlException) {
            if (sqlException.getMessage().contains("Unknown column 'DATETIME_PRECISION'")) {
                datePrecisionColumnExist = false;
                return getColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern);
            }
            throw sqlException;
        }
    }

    /**
     * Retrieves a description of the foreign key columns that reference the given table's primary key
     * columns (the foreign keys exported by a table). They are ordered by FKTABLE_CAT, FKTABLE_SCHEM,
     * FKTABLE_NAME, and KEY_SEQ.
     *
     * <p>Each foreign key column description has the following columns:
     *
     * <OL>
     * <LI><B>PKTABLE_CAT</B> String {@code =>} primary key table catalog (may be <code>null</code>)
     * <LI><B>PKTABLE_SCHEM</B> String {@code =>} primary key table schema (may be <code>null</code>
     * )
     * <LI><B>PKTABLE_NAME</B> String {@code =>} primary key table name
     * <LI><B>PKCOLUMN_NAME</B> String {@code =>} primary key column name
     * <LI><B>FKTABLE_CAT</B> String {@code =>} foreign key table catalog (may be <code>null</code>)
     * being exported (may be <code>null</code>)
     * <LI><B>FKTABLE_SCHEM</B> String {@code =>} foreign key table schema (may be <code>null</code>
     * ) being exported (may be <code>null</code>)
     * <LI><B>FKTABLE_NAME</B> String {@code =>} foreign key table name being exported
     * <LI><B>FKCOLUMN_NAME</B> String {@code =>} foreign key column name being exported
     * <LI><B>KEY_SEQ</B> short {@code =>} sequence number within foreign key( a value of 1
     * represents the first column of the foreign key, a value of 2 would represent the second
     * column within the foreign key).
     * <LI><B>UPDATE_RULE</B> short {@code =>} What happens to foreign key when primary is updated:
     * <UL>
     * <LI>importedNoAction - do not allow update of primary key if it has been imported
     * <LI>importedKeyCascade - change imported key to agree with primary key update
     * <LI>importedKeySetNull - change imported key to <code>NULL</code> if its primary key
     * has been updated
     * <LI>importedKeySetDefault - change imported key to default values if its primary key
     * has been updated
     * <LI>importedKeyRestrict - same as importedKeyNoAction (for ODBC 2.x compatibility)
     * </UL>
     * <LI><B>DELETE_RULE</B> short {@code =>} What happens to the foreign key when primary is
     * deleted.
     * <UL>
     * <LI>importedKeyNoAction - do not allow delete of primary key if it has been imported
     * <LI>importedKeyCascade - delete rows that import a deleted key
     * <LI>importedKeySetNull - change imported key to <code>NULL</code> if its primary key
     * has been deleted
     * <LI>importedKeyRestrict - same as importedKeyNoAction (for ODBC 2.x compatibility)
     * <LI>importedKeySetDefault - change imported key to default if its primary key has been
     * deleted
     * </UL>
     * <LI><B>FK_NAME</B> String {@code =>} foreign key name (may be <code>null</code>)
     * <LI><B>PK_NAME</B> String {@code =>} primary key name (may be <code>null</code>)
     * <LI><B>DEFERRABILITY</B> short {@code =>} can the evaluation of foreign key constraints be
     * deferred until commit
     * <UL>
     * <LI>importedKeyInitiallyDeferred - see SQL92 for definition
     * <LI>importedKeyInitiallyImmediate - see SQL92 for definition
     * <LI>importedKeyNotDeferrable - see SQL92 for definition
     * </UL>
     * </OL>
     *
     * @param catalog a catalog name; must match the catalog name as it is stored in this database; ""
     *                retrieves those without a catalog; <code>null</code> means that the catalog name should not
     *                be used to narrow the search
     * @param schema  a schema name; must match the schema name as it is stored in the database; ""
     *                retrieves those without a schema; <code>null</code> means that the schema name should not
     *                be used to narrow the search
     * @param table   a table name; must match the table name as it is stored in this database
     * @return a <code>ResultSet</code> object in which each row is a foreign key column description
     * @throws SQLException if a database access error occurs
     * @see #getImportedKeys
     */
    public ResultSet getExportedKeys(String catalog, String schema, String table)
                                                                                 throws SQLException {
        if (this.connection.getProtocol().isOracleMode()) {
            return this.keysQuery(schema, table, (String) null, (String) null,
                "ORDER BY fktable_schem, fktable_name, key_seq");
        }
        ArrayList<String> tableNameList;
        if (table == null) {
            throw  new SQLException("Table not specified.");
        }
        String sql = "SELECT KCU.REFERENCED_TABLE_SCHEMA PKTABLE_CAT, NULL PKTABLE_SCHEM,  KCU.REFERENCED_TABLE_NAME PKTABLE_NAME,"
                     + " KCU.REFERENCED_COLUMN_NAME PKCOLUMN_NAME, KCU.TABLE_SCHEMA FKTABLE_CAT, NULL FKTABLE_SCHEM, "
                     + " KCU.TABLE_NAME FKTABLE_NAME, KCU.COLUMN_NAME FKCOLUMN_NAME, KCU.POSITION_IN_UNIQUE_CONSTRAINT KEY_SEQ,"
                     + " CASE update_rule "
                     + "   WHEN 'RESTRICT' THEN 1"
                     + "   WHEN 'NO ACTION' THEN 3"
                     + "   WHEN 'CASCADE' THEN 0"
                     + "   WHEN 'SET NULL' THEN 2"
                     + "   WHEN 'SET DEFAULT' THEN 4"
                     + " END UPDATE_RULE,"
                     + " CASE DELETE_RULE"
                     + "  WHEN 'RESTRICT' THEN 1"
                     + "  WHEN 'NO ACTION' THEN 3"
                     + "  WHEN 'CASCADE' THEN 0"
                     + "  WHEN 'SET NULL' THEN 2"
                     + "  WHEN 'SET DEFAULT' THEN 4"
                     + " END DELETE_RULE,"
                     + " RC.CONSTRAINT_NAME FK_NAME,"
                     + " 'PRIMARY' PK_NAME,"
                     + importedKeyNotDeferrable
                     + " DEFERRABILITY"
                     + " FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE KCU"
                     + " INNER JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS RC"
                     + " ON KCU.CONSTRAINT_SCHEMA = RC.CONSTRAINT_SCHEMA"
                     + " AND KCU.CONSTRAINT_NAME = RC.CONSTRAINT_NAME"
                     + " WHERE "
                     + catalogCond("KCU.REFERENCED_TABLE_SCHEMA", catalog)
                     + patternCond("KCU.REFERENCED_TABLE_NAME", table)
                     + " ORDER BY FKTABLE_CAT, FKTABLE_SCHEM, FKTABLE_NAME, KEY_SEQ";

        // not suppport multi catalogs
        try {
            tableNameList= getALlTableNames(catalog);
            ResultSet allTableRS = getAllTablesResultSet(tableNameList,catalog);
            ArrayList<String[]> list = new ArrayList<>();
            while(allTableRS.next()) {
                String tableType = allTableRS.getString("Type");
                if (tableType!= null && (tableType.toUpperCase(Locale.ROOT).equals("INNODB") || tableType.toUpperCase(Locale.ROOT).equals("SUPPORTS_FK"))) {
                    String tableComment  = allTableRS.getString("Comment").trim();
                    String fkTableName = allTableRS.getString("Name");
                    if(tableComment != null) {
                        StringTokenizer stringTokenizer = new StringTokenizer(tableComment,";",false);
                        if (stringTokenizer.hasMoreTokens()) {
                            String dummy = stringTokenizer.nextToken();
                            // Skip InnoDB comment
                        }
                        while(stringTokenizer.hasMoreTokens()) {
                            String keys = stringTokenizer.nextToken();
                            int fkParamOpenIndex = keys.indexOf("(");
                            if (fkParamOpenIndex == -1) {
                                throw  new SQLException("Error parsing foreign keys definition, couldn't find start of local columns list.");
                            }
                            String constraintName = keys.substring(1,fkParamOpenIndex);
                            keys = keys.substring(fkParamOpenIndex );
                            int fkParamCloseIndex = keys.indexOf(")");
                            if (fkParamCloseIndex == -1) {
                                throw  new SQLException("Error parsing foreign keys definition, couldn't find start of local columns list.");
                            }
                            String fkParamNames =  keys.substring(1,fkParamCloseIndex);
                            int refIndex = keys.indexOf("REFER");
                            if (refIndex == -1) {
                                throw new SQLException("Error parsing foreign keys definition, couldn't find start of referenced tables list");
                            }
                            int refParamOpenIndex = keys.indexOf("(",refIndex);
                            if (refParamOpenIndex == -1) {
                                throw new SQLException("Error parsing foreign keys definition, couldn't find start of referenced columns list");
                            }
                            String refCatalogAndTable = keys.substring(refIndex + "REFER".length() + 1,refParamOpenIndex);
                            int slashIndex = refCatalogAndTable.indexOf("/");
                            if(slashIndex == -1) {
                                throw  new SQLException("Error parsing foreign keys definition, couldn't find name of referenced catalog.");
                            }
                            String refCatalog = refCatalogAndTable.substring(0,slashIndex);
                            refCatalog = removeQuoted(refCatalog);
                            String refTableName = refCatalogAndTable.substring(slashIndex + 1);
                            refTableName = removeQuoted(refTableName);
                            boolean isExportKeys = true;
                            if(!refTableName.equals(table) && isExportKeys) {
                                continue;
                            }
                            int refParamCloseIndex = keys.indexOf(")",refParamOpenIndex);
                            if (refParamCloseIndex == -1) {
                                throw new SQLException("Error parsing foreign keys definition, couldn't find end of referenced columns list.");
                            }
                            String refParamNames = keys.substring(refParamOpenIndex + 1,refParamCloseIndex );
                            String[] refParamList = refParamNames.split(",");
                            String[] fkParamList = fkParamNames.split(",");
                            int fkCur = 0;
                            int refCur = 0;
                            int keySeq = 1;
                            while(fkCur < fkParamList.length) {
                                String lColumnName = removeQuoted(fkParamList[fkCur++]);
                                String rColumnName = null;
                                if (refCur < refParamList.length) {
                                    rColumnName = removeQuoted(refParamList[refCur++]);
                                }
                                int updateRuleAction  = importedKeyNoAction;
                                int deleteRuleAction  = importedKeyNoAction;
                                int lastIndex = keys.lastIndexOf(")");
                                if (lastIndex != keys.length() - 1) {
                                    String options = keys.substring(lastIndex + 1);
                                    String optionsForUpdate = options.substring(options.indexOf("ON UPDATE"));
                                    if (optionsForUpdate.startsWith("ON UPDATE CASCADE")){
                                        updateRuleAction = importedKeyCascade;
                                    } else if (optionsForUpdate.startsWith("ON UPDATE SET NUL")) {
                                        updateRuleAction = importedKeySetNull;
                                    } else if (optionsForUpdate.startsWith("ON UPDATE RESTRICT")) {
                                        updateRuleAction = importedKeyRestrict;
                                    } else if (optionsForUpdate.startsWith("ON UPDATE NO ACTION")) {
                                        updateRuleAction = importedKeyNoAction;
                                    }
                                    String optionsForDelete = options.substring(options.indexOf("ON DELETE"));
                                    if (optionsForDelete.startsWith("ON DELETE CASCADE")) {
                                        deleteRuleAction = importedKeyCascade;
                                    } else if (optionsForDelete.startsWith("ON DELETE SET NUL")) {
                                        deleteRuleAction = importedKeySetNull;
                                    } else if (optionsForDelete.startsWith("ON DELETE RESTRICT")) {
                                        deleteRuleAction = importedKeyRestrict;
                                    } else if (optionsForDelete.startsWith("ON DELETE NO ACTIO")) {
                                        deleteRuleAction = importedKeyNoAction;
                                    }
                                }
                                String[] data = new String[14];
                                data[0] = refCatalog;
                                data[1] = null;
                                data[2] = isExportKeys ? refTableName : table;
                                data[3] = rColumnName;
                                data[4] = catalog;
                                data[5] = null;
                                data[6] = isExportKeys ? fkTableName : table;
                                data[7] = lColumnName;
                                data[8] = Integer.toString(keySeq++);
                                data[9] = Integer.toString(updateRuleAction);
                                data[10] = Integer.toString(deleteRuleAction);
                                data[11] = constraintName;
                                data[12] = null;
                                data[13] = Integer.toString(importedKeyNotDeferrable);
                                list.add(data);
                            }
                        }
                    }
                }

            }
            String[][] val = new String[list.size()][];
            for (int j = 0; j < list.size(); j++) {
                val[j] = list.get(j);
            }
            return SelectResultSet.createResultSet(exportKeysColumnNames,exportKeysColumnTypes,val,this.protocol);
        } catch (SQLException e) {
            throw e;
        }
    }

    String removeQuoted(String str) {
        String tmp = str.trim();
        if (tmp.startsWith("`") && tmp.endsWith("`")) {
            return tmp.substring(1, tmp.length() - 1);
        } else {
            return tmp;
        }
    }

    ArrayList<String>   getALlTableNames(String catalog) throws SQLException {
        ResultSet rs = getTables(catalog, "", "%", new String[] { "TABLE" });
        ArrayList<String> tableNameList = new ArrayList<>();
        while (rs.next()) {
            tableNameList.add(rs.getString("TABLE_NAME"));
        }
        return tableNameList;
    }

    ResultSet getAllTablesResultSet(ArrayList<String> tableNameList,String catalog) throws SQLException {
        String[] columnNames = {"Name", "Type", "Comment"};
        ColumnType[] columnTypes = {ColumnType.VARCHAR, ColumnType.VARCHAR, ColumnType.VARCHAR};
        ArrayList<String[]> list = new ArrayList<>();
        if (catalog == null || catalog.length() == 0) {
            catalog = this.connection.getCatalog();
        }
        for(int i = 0 ;i <tableNameList.size() ; i++) {
            StringBuilder query = new StringBuilder();
            query.append("SHOW CREATE TABLE ");
            query.append("`");
            query.append(catalog);
            query.append("`.");
            String name = OceanBaseConnection.quoteIdentifier(tableNameList.get(i));
            query.append(name);
            ResultSet rs = executeQuery(query.toString());
            String[] data = new String[3];
            StringBuilder sb  = new StringBuilder("comment; ");
            while(rs.next()) {
                String constraintName = null;
                String columnName = null;
                String refColumnName = null;
                String refTableName;
                String referencedCatalog = null;
                String referencedTable = null;
                data[0] = rs.getString(1);
                data[1] = "SUPPORTS_FK";
                String createTableString  = rs.getString(2);
                StringTokenizer stringTokenizer = new StringTokenizer(createTableString, "\n");
                boolean firstTime = true;
                while(stringTokenizer.hasMoreTokens()) {
                    String str = stringTokenizer.nextToken().trim();
                    if(str.toUpperCase(Locale.ROOT).startsWith("CONSTRAINT")) {
                        int beginIndex = str.indexOf("`");
                        int endIndex = 0;

                        boolean useBackTicks = false;
                        if(beginIndex == -1) {
                            beginIndex = str.indexOf("\"");
                            useBackTicks = true;
                        }
                        if (beginIndex != -1) {
                            if (!useBackTicks) {
                                endIndex = str.indexOf("`", beginIndex + 1);
                            } else {
                                endIndex = str.indexOf("\"", beginIndex + 1);
                            }
                            if (endIndex != -1) {
                                constraintName = str.substring(beginIndex+1,endIndex);
                                str =  str.substring(endIndex+1 ,str.length()).trim();
                            }
                        }
                    }
                    if(str.toUpperCase(Locale.ROOT).startsWith("FOREIGN KEY")) {
                        if(str.endsWith(",")) {
                            str = str.substring(0,str.length()-1);
                        }
                        int fkIndex = str.indexOf("FOREIGN KEY");
                        if(fkIndex !=  -1 ) {
                            int startIndex = fkIndex + "FOREIGN KEY".length();
                            int refIndex = str.toUpperCase(Locale.ROOT).indexOf("REFERENCES",startIndex);
                            if (refIndex != -1 ) {
                                int paramOpenIndex = str.indexOf("(",startIndex);
                                int paramCloseIndex = str.indexOf(")",paramOpenIndex);
                                if (paramCloseIndex ==-1 || paramOpenIndex == -1)  {
                                    throw new SQLException("Parsing REFERENCES failed !");
                                }
                                columnName = str.substring(paramOpenIndex+1,paramCloseIndex);
                                int afterRefIndex = refIndex+"REFERENCES".length();
                                int referenceParamOpenIndex = str.indexOf("(",afterRefIndex);
                                if (referenceParamOpenIndex != -1 ) {
                                    refTableName = str.substring(afterRefIndex,referenceParamOpenIndex);
                                    int referenceParamCloseIndex = str.indexOf(")",referenceParamOpenIndex);
                                    refColumnName = str.substring(referenceParamOpenIndex+1,referenceParamCloseIndex);
                                    int catalogEndIndex = refTableName.indexOf(".");
                                    if (catalogEndIndex != -1) {
                                         referencedCatalog = refTableName.substring(0,catalogEndIndex);
                                         referencedTable  = refTableName.substring(catalogEndIndex+1);
                                    }
                                }
                            }
                        }
                        if (!firstTime) {
                            sb.append("comment; ");
                        } else {
                            firstTime = false;
                        }
                        if(constraintName != null){
                            sb.append(constraintName);
                        } else {
                            sb.append("not_availabl");
                        }
                        sb.append("(");
                        sb.append(columnName);
                        sb.append(") REFER");
                        sb.append(referencedCatalog);
                        sb.append("/");
                        sb.append(referencedTable);
                        sb.append("(");
                        sb.append(refColumnName);
                        sb.append(")");
                        int lastParenIndex = str.lastIndexOf(")");

                        if (lastParenIndex != (str.length() - 1)) {
                            String cascadeOptions = str.substring(lastParenIndex + 1);
                            sb.append(" ");
                            sb.append(cascadeOptions);
                        }
                    }

                }
                data[2] = sb.toString();
            }
            list.add(data);
        }
        String[][] val = new String[list.size()][];
        for (int j = 0; j < list.size(); j++) {
            val[j] = list.get(j);
        }
        return SelectResultSet.createResultSet(columnNames,columnTypes,val,this.protocol);
    }

    /**
     * GetImportedKeysUsingInformationSchema.
     *
     * @param catalog catalog
     * @param table   table
     * @return resultset
     * @throws SQLException exception
     */
    public ResultSet getImportedKeysUsingInformationSchema(String catalog, String table)
                                                                                        throws SQLException {
        if (table == null) {
            throw new SQLException("'table' parameter in getImportedKeys cannot be null");
        }
        String sql = "SELECT KCU.REFERENCED_TABLE_SCHEMA PKTABLE_CAT, NULL PKTABLE_SCHEM,  KCU.REFERENCED_TABLE_NAME PKTABLE_NAME,"
                     + " KCU.REFERENCED_COLUMN_NAME PKCOLUMN_NAME, KCU.TABLE_SCHEMA FKTABLE_CAT, NULL FKTABLE_SCHEM, "
                     + " KCU.TABLE_NAME FKTABLE_NAME, KCU.COLUMN_NAME FKCOLUMN_NAME, KCU.POSITION_IN_UNIQUE_CONSTRAINT KEY_SEQ,"
                     + " CASE update_rule "
                     + "   WHEN 'RESTRICT' THEN 1"
                     + "   WHEN 'NO ACTION' THEN 3"
                     + "   WHEN 'CASCADE' THEN 0"
                     + "   WHEN 'SET NULL' THEN 2"
                     + "   WHEN 'SET DEFAULT' THEN 4"
                     + " END UPDATE_RULE,"
                     + " CASE DELETE_RULE"
                     + "  WHEN 'RESTRICT' THEN 1"
                     + "  WHEN 'NO ACTION' THEN 3"
                     + "  WHEN 'CASCADE' THEN 0"
                     + "  WHEN 'SET NULL' THEN 2"
                     + "  WHEN 'SET DEFAULT' THEN 4"
                     + " END DELETE_RULE,"
                     + " RC.CONSTRAINT_NAME FK_NAME,"
                     + " NULL PK_NAME,"
                     + importedKeyNotDeferrable
                     + " DEFERRABILITY"
                     + " FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE KCU"
                     + " INNER JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS RC"
                     + " ON KCU.CONSTRAINT_SCHEMA = RC.CONSTRAINT_SCHEMA"
                     + " AND KCU.CONSTRAINT_NAME = RC.CONSTRAINT_NAME"
                     + " WHERE "
                     + catalogCond("KCU.TABLE_SCHEMA", catalog)
                     + " AND "
                     + " KCU.TABLE_NAME = "
                     + escapeQuote(table)
                     + " ORDER BY PKTABLE_CAT, PKTABLE_SCHEM, PKTABLE_NAME, KEY_SEQ";

        return executeQuery(sql);
    }

    /**
     * GetImportedKeysUsingShowCreateTable.
     *
     * @param catalog catalog
     * @param table   table
     * @return resultset
     * @throws Exception exception
     */
    public ResultSet getImportedKeysUsingShowCreateTable(String catalog, String table)
                                                                                      throws Exception {

        if (catalog == null || catalog.isEmpty()) {
            throw new IllegalArgumentException("catalog");
        }

        if (table == null || table.isEmpty()) {
            throw new IllegalArgumentException("table");
        }

        ResultSet rs = connection.createStatement().executeQuery(
            "SHOW CREATE TABLE " + OceanBaseConnection.quoteIdentifier(catalog) + "."
                    + OceanBaseConnection.quoteIdentifier(table));
        if (rs.next()) {
            String tableDef = rs.getString(2);
            return JDBC4DatabaseMetaData.getImportedKeys(tableDef, table, catalog, connection);
        }
        throw new SQLException("Fail to retrieve table information using SHOW CREATE TABLE");
    }

    /**
     * Retrieves a description of a table's optimal set of columns that uniquely identifies a row.
     * They are ordered by SCOPE.
     *
     * <p>Each column description has the following columns:
     *
     * <OL>
     * <LI><B>SCOPE</B> short {@code =>} actual scope of result
     * <UL>
     * <LI>bestRowTemporary - very temporary, while using row
     * <LI>bestRowTransaction - valid for remainder of current transaction
     * <LI>bestRowSession - valid for remainder of current session
     * </UL>
     * <LI><B>COLUMN_NAME</B> String {@code =>} column name
     * <LI><B>DATA_TYPE</B> int {@code =>} SQL data type from java.sql.Types
     * <LI><B>TYPE_NAME</B> String {@code =>} Data source dependent type name, for a UDT the type
     * name is fully qualified
     * <LI><B>COLUMN_SIZE</B> int {@code =>} precision
     * <LI><B>BUFFER_LENGTH</B> int {@code =>} not used
     * <LI><B>DECIMAL_DIGITS</B> short {@code =>} scale - Null is returned for data types where
     * DECIMAL_DIGITS is not applicable.
     * <LI><B>PSEUDO_COLUMN</B> short {@code =>} is this a pseudo column like an Oracle ROWID
     * <UL>
     * <LI>bestRowUnknown - may or may not be pseudo column
     * <LI>bestRowNotPseudo - is NOT a pseudo column
     * <LI>bestRowPseudo - is a pseudo column
     * </UL>
     * </OL>
     *
     * <p>The COLUMN_SIZE column represents the specified column size for the given column. For
     * numeric data, this is the maximum precision. For character data, this is the length in
     * characters. For datetime datatypes, this is the length in characters of the String
     * representation (assuming the maximum allowed precision of the fractional seconds component).
     * For binary data, this is the length in bytes. For the ROWID datatype, this is the length in
     * bytes. Null is returned for data types where the column size is not applicable.
     *
     * @param catalog  a catalog name; must match the catalog name as it is stored in the database; ""
     *                 retrieves those without a catalog; <code>null</code> means that the catalog name should not
     *                 be used to narrow the search
     * @param schema   a schema name; must match the schema name as it is stored in the database; ""
     *                 retrieves those without a schema; <code>null</code> means that the schema name should not
     *                 be used to narrow the search
     * @param table    a table name; must match the table name as it is stored in the database
     * @param scope    the scope of interest; use same values as SCOPE
     * @param nullable include columns that are nullable.
     * @return <code>ResultSet</code> - each row is a column description
     * @throws SQLException if a database access error occurs
     */
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope,
                                          final boolean nullable) throws SQLException {

        if (table == null) {
            throw new SQLException("'table' parameter cannot be null in getBestRowIdentifier()");
        }
        if (this.connection.getProtocol().isOracleMode()) {
            return super.getBestRowIdentifier(catalog, schema, table, scope, nullable);
        }
        String sql = "SELECT "
                     + bestRowUnknown
                     + " SCOPE, COLUMN_NAME,"
                     + dataTypeClause("COLUMN_TYPE")
                     + " DATA_TYPE, DATA_TYPE TYPE_NAME,"
                     + " IF(NUMERIC_PRECISION IS NULL, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION) COLUMN_SIZE, 0 BUFFER_LENGTH,"
                     + " NUMERIC_SCALE DECIMAL_DIGITS,"
                     + " 1 PSEUDO_COLUMN"
                     + " FROM INFORMATION_SCHEMA.COLUMNS"
                     + " WHERE COLUMN_KEY IN('PRI', 'MUL', 'UNI')"
                     + " AND "
                     + catalogCond("TABLE_SCHEMA", catalog == null ? this.connection.getProtocol()
                         .getCatalog() : catalog) + " AND TABLE_NAME = " + escapeQuote(table);
        return executeQuery(sql);
    }

    public boolean generatedKeyAlwaysReturned() {
        return true;
    }

    /**
     * Retrieves a description of the pseudo or hidden columns available in a given table within the
     * specified catalog and schema. Pseudo or hidden columns may not always be stored within a table
     * and are not visible in a ResultSet unless they are specified in the query's outermost SELECT
     * list. Pseudo or hidden columns may not necessarily be able to be modified. If there are no
     * pseudo or hidden columns, an empty ResultSet is returned.
     *
     * <p>Only column descriptions matching the catalog, schema, table and column name criteria are
     * returned. They are ordered by <code>TABLE_CAT</code>,<code>TABLE_SCHEM</code>, <code>TABLE_NAME
     * </code> and <code>COLUMN_NAME</code>.
     *
     * <p>Each column description has the following columns:
     *
     * <OL>
     * <LI><B>TABLE_CAT</B> String {@code =>} table catalog (may be <code>null</code>)
     * <LI><B>TABLE_SCHEM</B> String {@code =>} table schema (may be <code>null</code>)
     * <LI><B>TABLE_NAME</B> String {@code =>} table name
     * <LI><B>COLUMN_NAME</B> String {@code =>} column name
     * <LI><B>DATA_TYPE</B> int {@code =>} SQL type from java.sql.Types
     * <LI><B>COLUMN_SIZE</B> int {@code =>} column size.
     * <LI><B>DECIMAL_DIGITS</B> int {@code =>} the number of fractional digits. Null is returned
     * for data types where DECIMAL_DIGITS is not applicable.
     * <LI><B>NUM_PREC_RADIX</B> int {@code =>} Radix (typically either 10 or 2)
     * <LI><B>COLUMN_USAGE</B> String {@code =>} The allowed usage for the column. The value
     * returned will correspond to the enum name returned by PseudoColumnUsage.name()
     * <LI><B>REMARKS</B> String {@code =>} comment describing column (may be <code>null</code>)
     * <LI><B>CHAR_OCTET_LENGTH</B> int {@code =>} for char types the maximum number of bytes in the
     * column
     * <LI><B>IS_NULLABLE</B> String {@code =>} ISO rules are used to determine the nullability for
     * a column.
     * <UL>
     * <LI>YES --- if the column can include NULLs
     * <LI>NO --- if the column cannot include NULLs
     * <LI>empty string --- if the nullability for the column is unknown
     * </UL>
     * </OL>
     *
     * <p>The COLUMN_SIZE column specifies the column size for the given column. For numeric data,
     * this is the maximum precision. For character data, this is the length in characters. For
     * datetime datatypes, this is the length in characters of the String representation (assuming the
     * maximum allowed precision of the fractional seconds component). For binary data, this is the
     * length in bytes. For the ROWID datatype, this is the length in bytes. Null is returned for data
     * types where the column size is not applicable.
     *
     * @param catalog           a catalog name; must match the catalog name as it is stored in the database; ""
     *                          retrieves those without a catalog; <code>null</code> means that the catalog name should not
     *                          be used to narrow the search
     * @param schemaPattern     a schema name pattern; must match the schema name as it is stored in the
     *                          database; "" retrieves those without a schema; <code>null</code> means that the schema name
     *                          should not be used to narrow the search
     * @param tableNamePattern  a table name pattern; must match the table name as it is stored in the
     *                          database
     * @param columnNamePattern a column name pattern; must match the column name as it is stored in
     *                          the database
     * @return <code>ResultSet</code> - each row is a column description
     * @throws SQLException if a database access error occurs
     * @see PseudoColumnUsage
     * @since 1.7
     */
    public ResultSet getPseudoColumns(String catalog, String schemaPattern,
                                      String tableNamePattern, String columnNamePattern)
                                                                                        throws SQLException {
        return connection
            .createStatement()
            .executeQuery(
                "SELECT ' ' TABLE_CAT, ' ' TABLE_SCHEM,"
                        + "' ' TABLE_NAME, ' ' COLUMN_NAME, 0 DATA_TYPE, 0 COLUMN_SIZE, 0 DECIMAL_DIGITS,"
                        + "10 NUM_PREC_RADIX, ' ' COLUMN_USAGE,  ' ' REMARKS, 0 CHAR_OCTET_LENGTH, 'YES' IS_NULLABLE FROM DUAL "
                        + "WHERE 1=0");
    }

    public boolean allProceduresAreCallable() {
        if (this.connection.getProtocol().isOracleMode()) {
            return super.allProceduresAreCallable();
        }
        return true;
    }

    public boolean allTablesAreSelectable() {
        if (this.connection.getProtocol().isOracleMode()) {
            return super.allTablesAreSelectable();
        }
        return true;
    }

    @Override
    public String getURL() {
        return urlParser.getInitialUrl();
    }

    public String getUserName() throws SQLException {
        if (this.connection.getProtocol().isOracleMode()) {
            return super.getUserName();
        }
        return urlParser.getUsername();
    }

    public boolean isReadOnly() {
        return false;
    }

    public boolean nullsAreSortedHigh() {
        return false;
    }

    public boolean nullsAreSortedLow() {
        return !nullsAreSortedHigh();
    }

    public boolean nullsAreSortedAtStart() {
        return false;
    }

    public boolean nullsAreSortedAtEnd() {
        if (this.connection.getProtocol().isOracleMode()) {
            return super.nullsAreSortedAtEnd();
        }
        return !nullsAreSortedAtStart();
    }

    /**
     * Return Server type. MySQL or MariaDB. MySQL can be forced for compatibility with option
     * "useMysqlMetadata"
     *
     * @return server type
     * @throws SQLException in case of socket error.
     */
    public String getDatabaseProductName() throws SQLException {
        if (connection.getProtocol().isOracleMode()) {
            return "Oracle";
        } else {
            return "MySQL";
        }
    }

    public String getDatabaseProductVersion() {
        return connection.getProtocol().getServerVersion();
    }

    public int getDatabaseMajorVersion() {
        return connection.getProtocol().getMajorServerVersion();
    }

    public int getDatabaseMinorVersion() {
        return connection.getProtocol().getMinorServerVersion();
    }

    public String getOceanBaseServerVersion() {
        return connection.getProtocol().getObServerVersion();
    }

    public String getDriverName() {
        return DRIVER_NAME;
    }

    public String getDriverVersion() {
        return Version.version;
    }

    public int getDriverMajorVersion() {
        return Version.majorVersion;
    }

    public int getDriverMinorVersion() {
        return Version.minorVersion;
    }

    @Override
    public int getJDBCMajorVersion() {
        return 4;
    }

    @Override
    public int getJDBCMinorVersion() {
        return 2;
    }

    public boolean usesLocalFiles() {
        return false;
    }

    public boolean usesLocalFilePerTable() {
        return false;
    }

    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        if (this.connection.getProtocol().isOracleMode()) {
            return super.supportsMixedCaseIdentifiers();
        }
        return (connection.getLowercaseTableNames() == 0);
    }

    public boolean storesUpperCaseIdentifiers() {
        if (this.connection.getProtocol().isOracleMode()) {
            return super.storesUpperCaseIdentifiers();
        }
        return false;
    }

    public boolean storesLowerCaseIdentifiers() throws SQLException {
        if (this.connection.getProtocol().isOracleMode()) {
            return super.storesLowerCaseIdentifiers();
        }
        return (connection.getLowercaseTableNames() == 1);
    }

    public boolean storesMixedCaseIdentifiers() throws SQLException {
        if (this.connection.getProtocol().isOracleMode()) {
            return super.storesMixedCaseIdentifiers();
        }
        return (connection.getLowercaseTableNames() == 2);
    }

    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        return supportsMixedCaseIdentifiers();
    }

    public boolean storesUpperCaseQuotedIdentifiers() {
        return storesUpperCaseIdentifiers();
    }

    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        return storesLowerCaseIdentifiers();
    }

    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        return storesMixedCaseIdentifiers();
    }

    public String getIdentifierQuoteString() {
        if (protocol.isOracleMode()) {
            return "\"";
        }
        return "`";
    }

    /**
     * Retrieves a comma-separated list of all of this database's SQL keywords that are NOT also
     * SQL:2003 keywords.
     *
     * @return the list of this database's keywords that are not also SQL:2003 keywords
     */
    @Override
    public String getSQLKeywords() {
        if (protocol.isOracleMode()) {
            return "ACCESS, ADD, ALTER, AUDIT, CLUSTER, COLUMN, COMMENT, COMPRESS, CONNECT, DATE, DROP, EXCLUSIVE, FILE, IDENTIFIED, IMMEDIATE, INCREMENT, INDEX, INITIAL, INTERSECT, LEVEL, LOCK, LONG, MAXEXTENTS, MINUS, MODE, NOAUDIT, NOCOMPRESS, NOWAIT, NUMBER, OFFLINE, ONLINE, PCTFREE, PRIOR, all_PL_SQL_reserved_ words";
        }
        return "ACCESSIBLE,ANALYZE,ASENSITIVE,BEFORE,BIGINT,BINARY,BLOB,CALL,CHANGE,CONDITION,DATABASE,DATABASES,"
               + "DAY_HOUR,DAY_MICROSECOND,DAY_MINUTE,DAY_SECOND,DELAYED,DETERMINISTIC,DISTINCTROW,DIV,DUAL,EACH,"
               + "ELSEIF,ENCLOSED,ESCAPED,EXIT,EXPLAIN,FLOAT4,FLOAT8,FORCE,FULLTEXT,GENERAL,HIGH_PRIORITY,"
               + "HOUR_MICROSECOND,HOUR_MINUTE,HOUR_SECOND,IF,IGNORE,IGNORE_SERVER_IDS,INDEX,INFILE,INOUT,INT1,INT2,"
               + "INT3,INT4,INT8,ITERATE,KEY,KEYS,KILL,LEAVE,LIMIT,LINEAR,LINES,LOAD,LOCALTIME,LOCALTIMESTAMP,LOCK,"
               + "LONG,LONGBLOB,LONGTEXT,LOOP,LOW_PRIORITY,MASTER_HEARTBEAT_PERIOD,MASTER_SSL_VERIFY_SERVER_CERT,"
               + "MAXVALUE,MEDIUMBLOB,MEDIUMINT,MEDIUMTEXT,MIDDLEINT,MINUTE_MICROSECOND,MINUTE_SECOND,MOD,MODIFIES,"
               + "NO_WRITE_TO_BINLOG,OPTIMIZE,OPTIONALLY,OUT,OUTFILE,PURGE,RANGE,READ_WRITE,READS,REGEXP,RELEASE,"
               + "RENAME,REPEAT,REPLACE,REQUIRE,RESIGNAL,RESTRICT,RETURN,RLIKE,SCHEMAS,SECOND_MICROSECOND,SENSITIVE,"
               + "SEPARATOR,SHOW,SIGNAL,SLOW,SPATIAL,SPECIFIC,SQL_BIG_RESULT,SQL_CALC_FOUND_ROWS,SQL_SMALL_RESULT,"
               + "SQLEXCEPTION,SSL,STARTING,STRAIGHT_JOIN,TERMINATED,TINYBLOB,TINYINT,TINYTEXT,TRIGGER,UNDO,UNLOCK,"
               + "UNSIGNED,USE,UTC_DATE,UTC_TIME,UTC_TIMESTAMP,VARBINARY,VARCHARACTER,WHILE,XOR,YEAR_MONTH,ZEROFILL";
    }

    /**
     * List of numeric functions.
     *
     * @return List of numeric functions.
     */
    @Override
    public String getNumericFunctions() {
        if (protocol.isOracleMode()) {
            return "ABS,ACOS,ASIN,ATAN,ATAN2,CEILING,COS,EXP,FLOOR,LOG,LOG10,MOD,PI,POWER,ROUND,SIGN,SIN,SQRT,TAN,TRUNCATE";
        }
        return "DIV,ABS,ACOS,ASIN,ATAN,ATAN2,CEIL,CEILING,CONV,COS,COT,CRC32,DEGREES,EXP,FLOOR,GREATEST,LEAST,LN,LOG,"
               + "LOG10,LOG2,MOD,OCT,PI,POW,POWER,RADIANS,RAND,ROUND,SIGN,SIN,SQRT,TAN,TRUNCATE";
    }

    /**
     * List of string functions.
     *
     * @return List of string functions.
     */
    @Override
    public String getStringFunctions() {
        if (protocol.isOracleMode()) {
            return "ASCII,CHAR,CHAR_LENGTH,CHARACTER_LENGTH,CONCAT,LCASE,LENGTH,LTRIM,OCTET_LENGTH,REPLACE,RTRIM,SOUNDEX,SUBSTRING,UCASE";
        }
        return "ASCII,BIN,BIT_LENGTH,CAST,CHARACTER_LENGTH,CHAR_LENGTH,CONCAT,CONCAT_WS,CONVERT,ELT,EXPORT_SET,"
               + "EXTRACTVALUE,FIELD,FIND_IN_SET,FORMAT,FROM_BASE64,HEX,INSTR,LCASE,LEFT,LENGTH,LIKE,LOAD_FILE,LOCATE,"
               + "LOWER,LPAD,LTRIM,MAKE_SET,MATCH AGAINST,MID,NOT LIKE,NOT REGEXP,OCTET_LENGTH,ORD,POSITION,QUOTE,"
               + "REPEAT,REPLACE,REVERSE,RIGHT,RPAD,RTRIM,SOUNDEX,SOUNDS LIKE,SPACE,STRCMP,SUBSTR,SUBSTRING,"
               + "SUBSTRING_INDEX,TO_BASE64,TRIM,UCASE,UNHEX,UPDATEXML,UPPER,WEIGHT_STRING";
    }

    /**
     * List of system functions.
     *
     * @return List of system functions.
     */
    @Override
    public String getSystemFunctions() {
        if (protocol.isOracleMode()) {
            return "USER";
        }
        return "DATABASE,USER,SYSTEM_USER,SESSION_USER,LAST_INSERT_ID,VERSION";
    }

    /**
     * List of time and date functions.
     *
     * @return List of time and date functions.
     */
    @Override
    public String getTimeDateFunctions() {
        if (protocol.isOracleMode()) {
            return "CURRENT_DATE,CURRENT_TIMESTAMP,CURDATE,EXTRACT,HOUR,MINUTE,MONTH,SECOND,YEAR";
        }
        return "ADDDATE,ADDTIME,CONVERT_TZ,CURDATE,CURRENT_DATE,CURRENT_TIME,CURRENT_TIMESTAMP,CURTIME,DATEDIFF,"
               + "DATE_ADD,DATE_FORMAT,DATE_SUB,DAY,DAYNAME,DAYOFMONTH,DAYOFWEEK,DAYOFYEAR,EXTRACT,FROM_DAYS,"
               + "FROM_UNIXTIME,GET_FORMAT,HOUR,LAST_DAY,LOCALTIME,LOCALTIMESTAMP,MAKEDATE,MAKETIME,MICROSECOND,"
               + "MINUTE,MONTH,MONTHNAME,NOW,PERIOD_ADD,PERIOD_DIFF,QUARTER,SECOND,SEC_TO_TIME,STR_TO_DATE,SUBDATE,"
               + "SUBTIME,SYSDATE,TIMEDIFF,TIMESTAMPADD,TIMESTAMPDIFF,TIME_FORMAT,TIME_TO_SEC,TO_DAYS,TO_SECONDS,"
               + "UNIX_TIMESTAMP,UTC_DATE,UTC_TIME,UTC_TIMESTAMP,WEEK,WEEKDAY,WEEKOFYEAR,YEAR,YEARWEEK";
    }

    public String getSearchStringEscape() {
        if (protocol.isOracleMode()) {
            return "/";
        }
        return "\\";
    }

    public String getExtraNameCharacters() {
        if (protocol.isOracleMode()) {
            return "$#";
        }
        return "#@";
    }

    public boolean supportsAlterTableWithAddColumn() {
        return true;
    }

    public boolean supportsAlterTableWithDropColumn() {
        return true;
    }

    public boolean supportsColumnAliasing() {
        return true;
    }

    public boolean nullPlusNonNullIsNull() {
        return true;
    }

    public boolean supportsConvert() {
        if (this.connection.getProtocol().isOracleMode()) {
            return super.supportsConvert();
        }
        return true;
    }

    /**
     * Retrieves whether this database supports the JDBC scalar function CONVERT for conversions
     * between the JDBC types fromType and toType. The JDBC types are the generic SQL data types
     * defined in java.sql.Types.
     *
     * @param fromType the type to convert from; one of the type codes from the class java.sql.Types
     * @param toType   the type to convert to; one of the type codes from the class java.sql.Types
     * @return true if so; false otherwise
     */
    public boolean supportsConvert(int fromType, int toType) {
        switch (fromType) {
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.INTEGER:
            case Types.BIGINT:
            case Types.REAL:
            case Types.FLOAT:
            case Types.DECIMAL:
            case Types.NUMERIC:
            case Types.DOUBLE:
            case Types.BIT:
            case Types.BOOLEAN:
                switch (toType) {
                    case Types.TINYINT:
                    case Types.SMALLINT:
                    case Types.INTEGER:
                    case Types.BIGINT:
                    case Types.REAL:
                    case Types.FLOAT:
                    case Types.DECIMAL:
                    case Types.NUMERIC:
                    case Types.DOUBLE:
                    case Types.BIT:
                    case Types.BOOLEAN:
                    case Types.CHAR:
                    case Types.VARCHAR:
                    case Types.LONGVARCHAR:
                    case Types.BINARY:
                    case Types.VARBINARY:
                    case Types.LONGVARBINARY:
                        return true;
                    default:
                        return false;
                }

            case Types.BLOB:
                switch (toType) {
                    case Types.BINARY:
                    case Types.VARBINARY:
                    case Types.LONGVARBINARY:
                    case Types.CHAR:
                    case Types.VARCHAR:
                    case Types.LONGVARCHAR:
                    case Types.TINYINT:
                    case Types.SMALLINT:
                    case Types.INTEGER:
                    case Types.BIGINT:
                    case Types.REAL:
                    case Types.FLOAT:
                    case Types.DECIMAL:
                    case Types.NUMERIC:
                    case Types.DOUBLE:
                    case Types.BIT:
                    case Types.BOOLEAN:
                        return true;
                    default:
                        return false;
                }

            case Types.CHAR:
            case Types.CLOB:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                switch (toType) {
                    case Types.BIT:
                    case Types.TINYINT:
                    case Types.SMALLINT:
                    case Types.INTEGER:
                    case Types.BIGINT:
                    case Types.FLOAT:
                    case Types.REAL:
                    case Types.DOUBLE:
                    case Types.NUMERIC:
                    case Types.DECIMAL:
                    case Types.CHAR:
                    case Types.VARCHAR:
                    case Types.LONGVARCHAR:
                    case Types.BINARY:
                    case Types.VARBINARY:
                    case Types.LONGVARBINARY:
                    case Types.DATE:
                    case Types.TIME:
                    case Types.TIMESTAMP:
                    case Types.BLOB:
                    case Types.CLOB:
                    case Types.BOOLEAN:
                    case Types.NCHAR:
                    case Types.LONGNVARCHAR:
                    case Types.NCLOB:
                        return true;
                    default:
                        return false;
                }

            case Types.DATE:
                switch (toType) {
                    case Types.DATE:
                    case Types.CHAR:
                    case Types.VARCHAR:
                    case Types.LONGVARCHAR:
                    case Types.BINARY:
                    case Types.VARBINARY:
                    case Types.LONGVARBINARY:
                        return true;

                    default:
                        return false;
                }

            case Types.TIME:
                switch (toType) {
                    case Types.TIME:
                    case Types.CHAR:
                    case Types.VARCHAR:
                    case Types.LONGVARCHAR:
                    case Types.BINARY:
                    case Types.VARBINARY:
                    case Types.LONGVARBINARY:
                        return true;
                    default:
                        return false;
                }

            case Types.TIMESTAMP:
                switch (toType) {
                    case Types.TIMESTAMP:
                    case Types.CHAR:
                    case Types.VARCHAR:
                    case Types.LONGVARCHAR:
                    case Types.BINARY:
                    case Types.VARBINARY:
                    case Types.LONGVARBINARY:
                    case Types.TIME:
                    case Types.DATE:
                        return true;
                    default:
                        return false;
                }
            default:
                return false;
        }
    }

    public boolean supportsTableCorrelationNames() {
        return true;
    }

    public boolean supportsDifferentTableCorrelationNames() {
        return true;
    }

    public boolean supportsExpressionsInOrderBy() {
        return true;
    }

    public boolean supportsOrderByUnrelated() {
        return true;
    }

    public boolean supportsGroupBy() {
        return true;
    }

    public boolean supportsGroupByUnrelated() {
        return true;
    }

    public boolean supportsGroupByBeyondSelect() {
        return true;
    }

    public boolean supportsLikeEscapeClause() {
        return true;
    }

    public boolean supportsMultipleResultSets() {
        return true;
    }

    public boolean supportsMultipleTransactions() {
        return true;
    }

    public boolean supportsNonNullableColumns() {
        return true;
    }

    @Override
    public boolean supportsMinimumSQLGrammar() {
        return true;
    }

    @Override
    public boolean supportsCoreSQLGrammar() {
        return true;
    }

    @Override
    public boolean supportsExtendedSQLGrammar() {
        return false;
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL() {
        return true;
    }

    @Override
    public boolean supportsANSI92IntermediateSQL() {
        if (this.connection.getProtocol().isOracleMode()) {
            return super.supportsANSI92IntermediateSQL();
        }
        return true;
    }

    @Override
    public boolean supportsANSI92FullSQL() {
        if (this.connection.getProtocol().isOracleMode()) {
            return super.supportsANSI92FullSQL();
        }
        return true;
    }

    public boolean supportsIntegrityEnhancementFacility() {
        return true;
    }

    public boolean supportsOuterJoins() {
        return true;
    }

    public boolean supportsFullOuterJoins() {
        return true;
    }

    public boolean supportsLimitedOuterJoins() {
        return true;
    }

    public String getSchemaTerm() {
        return "schema";
    }

    public String getProcedureTerm() {
        return "procedure";
    }

    public String getCatalogTerm() {
        return "database";
    }

    public boolean isCatalogAtStart() {
        return true;
    }

    public String getCatalogSeparator() {
        return ".";
    }

    public boolean supportsSchemasInDataManipulation() {
        return false;
    }

    public boolean supportsSchemasInProcedureCalls() {
        return false;
    }

    public boolean supportsSchemasInTableDefinitions() {
        if (this.connection.getProtocol().isOracleMode()) {
            return super.supportsSchemasInTableDefinitions();
        }
        return false;
    }

    public boolean supportsSchemasInIndexDefinitions() {
        return false;
    }

    public boolean supportsSchemasInPrivilegeDefinitions() {
        return true;
    }

    public boolean supportsCatalogsInDataManipulation() {
        return true;
    }

    public boolean supportsCatalogsInProcedureCalls() {
        return true;
    }

    public boolean supportsCatalogsInTableDefinitions() {
        if (this.connection.getProtocol().isOracleMode()) {
            return super.supportsCatalogsInTableDefinitions();
        }
        return true;
    }

    public boolean supportsCatalogsInIndexDefinitions() {
        return true;
    }

    public boolean supportsCatalogsInPrivilegeDefinitions() {
        return true;
    }

    public boolean supportsPositionedDelete() {
        return false;
    }

    public boolean supportsPositionedUpdate() {
        return false;
    }

    public boolean supportsSelectForUpdate() {
        return true;
    }

    public boolean supportsStoredProcedures() {
        return true;
    }

    public boolean supportsSubqueriesInComparisons() {
        return true;
    }

    public boolean supportsSubqueriesInExists() {
        return true;
    }

    public boolean supportsSubqueriesInIns() {
        return true;
    }

    public boolean supportsSubqueriesInQuantifieds() {
        return true;
    }

    public boolean supportsCorrelatedSubqueries() {
        return true;
    }

    public boolean supportsUnion() {
        return true;
    }

    public boolean supportsUnionAll() {
        return true;
    }

    public boolean supportsOpenCursorsAcrossCommit() {
        if (this.connection.getProtocol().isOracleMode()) {
            return super.supportsOpenCursorsAcrossCommit();
        }
        return true;
    }

    public boolean supportsOpenCursorsAcrossRollback() {
        if (this.connection.getProtocol().isOracleMode()) {
            return super.supportsOpenCursorsAcrossRollback();
        }
        return true;
    }

    public boolean supportsOpenStatementsAcrossCommit() {
        if (this.connection.getProtocol().isOracleMode()) {
            return super.supportsOpenStatementsAcrossCommit();
        }
        return true;
    }

    public boolean supportsOpenStatementsAcrossRollback() {
        if (this.connection.getProtocol().isOracleMode()) {
            return super.supportsOpenStatementsAcrossRollback();
        }
        return true;
    }

    public int getMaxBinaryLiteralLength() {
        return 16777208;
    }

    public int getMaxCharLiteralLength() {
        return 16777208;
    }

    public int getMaxColumnNameLength() {
        return 64;
    }

    public int getMaxColumnsInGroupBy() {
        return 64;
    }

    public int getMaxColumnsInIndex() {
        return 16;
    }

    public int getMaxColumnsInOrderBy() {
        return 64;
    }

    public int getMaxColumnsInSelect() {
        return 256;
    }

    public int getMaxColumnsInTable() {
        return 0;
    }

    public int getMaxConnections() {
        return 0;
    }

    public int getMaxCursorNameLength() {
        return 0;
    }

    public int getMaxIndexLength() {
        return 256;
    }

    public int getMaxSchemaNameLength() {
        return 32;
    }

    public int getMaxProcedureNameLength() {
        return 256;
    }

    public int getMaxCatalogNameLength() {
        return 0;
    }

    public int getMaxRowSize() {
        return 0;
    }

    public boolean doesMaxRowSizeIncludeBlobs() {
        if (this.connection.getProtocol().isOracleMode()) {
            return super.doesMaxRowSizeIncludeBlobs();
        }
        return false;
    }

    public int getMaxStatementLength() {
        return 0;
    }

    public int getMaxStatements() {
        return 0;
    }

    public int getMaxTableNameLength() {
        return 64;
    }

    public int getMaxTablesInSelect() {
        return 256;
    }

    public int getMaxUserNameLength() {
        return 16;
    }

    public int getDefaultTransactionIsolation() {
        return Connection.TRANSACTION_REPEATABLE_READ;
    }

    /**
     * Retrieves whether this database supports transactions. If not, invoking the method <code>commit
     * </code> is a noop, and the isolation level is <code>TRANSACTION_NONE</code>.
     *
     * @return <code>true</code> if transactions are supported; <code>false</code> otherwise
     */
    public boolean supportsTransactions() {
        return true;
    }

    /* Helper to generate  information schema with "equality" condition (typically on catalog name)
     */

    /**
     * Retrieves whether this database supports the given transaction isolation level.
     *
     * @param level one of the transaction isolation levels defined in <code>java.sql.Connection
     *              </code>
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @see Connection
     */
    public boolean supportsTransactionIsolationLevel(int level) {
        switch (level) {
            case Connection.TRANSACTION_READ_UNCOMMITTED:
            case Connection.TRANSACTION_READ_COMMITTED:
            case Connection.TRANSACTION_REPEATABLE_READ:
            case Connection.TRANSACTION_SERIALIZABLE:
                return true;
            default:
                return false;
        }
    }

    //todo  there return false in 1.X_OracleModel , return true in oracle-jdbc
    public boolean supportsDataDefinitionAndDataManipulationTransactions() {
        return true;
    }

    public boolean supportsDataManipulationTransactionsOnly() {
        return false;
    }

    public boolean dataDefinitionCausesTransactionCommit() {
        return true;
    }

    public boolean dataDefinitionIgnoredInTransactions() {
        return false;
    }

    /**
     * Retrieves a description of the stored procedures available in the given catalog. Only procedure
     * descriptions matching the schema and procedure name criteria are returned. They are ordered by
     * <code>PROCEDURE_CAT</code>, <code>PROCEDURE_SCHEM</code>, <code>PROCEDURE_NAME</code> and
     * <code>SPECIFIC_ NAME</code>.
     *
     * <p>Each procedure description has the the following columns:
     *
     * <OL>
     * <LI><B>PROCEDURE_CAT</B> String {@code =>} procedure catalog (may be <code>null</code>)
     * <LI><B>PROCEDURE_SCHEM</B> String {@code =>} procedure schema (may be <code>null</code>)
     * <LI><B>PROCEDURE_NAME</B> String {@code =>} procedure name
     * <LI>reserved for future use
     * <LI>reserved for future use
     * <LI>reserved for future use
     * <LI><B>REMARKS</B> String {@code =>} explanatory comment on the procedure
     * <LI><B>PROCEDURE_TYPE</B> short {@code =>} kind of procedure:
     * <UL>
     * <LI>procedureResultUnknown - Cannot determine if a return value will be returned
     * <LI>procedureNoResult - Does not return a return value
     * <LI>procedureReturnsResult - Returns a return value
     * </UL>
     * <LI><B>SPECIFIC_NAME</B> String {@code =>} The name which uniquely identifies this procedure
     * within its schema.
     * </OL>
     *
     * <p>A user may not have permissions to execute any of the procedures that are returned by <code>
     * getProcedures</code>
     *
     * @param catalog              a catalog name; must match the catalog name as it is stored in the database; ""
     *                             retrieves those without a catalog; <code>null</code> means that the catalog name should not
     *                             be used to narrow the search
     * @param schemaPattern        a schema name pattern; must match the schema name as it is stored in the
     *                             database; "" retrieves those without a schema; <code>null</code> means that the schema name
     *                             should not be used to narrow the search
     * @param procedureNamePattern a procedure name pattern; must match the procedure name as it is
     *                             stored in the database
     * @return <code>ResultSet</code> - each row is a procedure description
     * @throws SQLException if a database access error occurs
     * @see #getSearchStringEscape
     */
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern)
                                                                                                     throws SQLException {

        if (connection.getProtocol().isOracleMode()) {
            return super.getProcedures(catalog, schemaPattern, procedureNamePattern);
        }
        String sql = "SELECT ROUTINE_SCHEMA PROCEDURE_CAT,NULL PROCEDURE_SCHEM, ROUTINE_NAME PROCEDURE_NAME,"
                     + " NULL RESERVED1, NULL RESERVED2, NULL RESERVED3,"
                     + " CASE ROUTINE_TYPE "
                     + "  WHEN 'FUNCTION' THEN "
                     + procedureReturnsResult
                     + "  WHEN 'PROCEDURE' THEN "
                     + procedureNoResult
                     + "  ELSE "
                     + procedureResultUnknown
                     + " END PROCEDURE_TYPE,"
                     + " ROUTINE_COMMENT REMARKS, SPECIFIC_NAME "
                     + " FROM INFORMATION_SCHEMA.ROUTINES "
                     + " WHERE "
                     + catalogCond("ROUTINE_SCHEMA", catalog)
                     + patternCond("ROUTINE_NAME", procedureNamePattern)
                     + "/* AND ROUTINE_TYPE='PROCEDURE' */";
        return executeQuery(sql);
    }

    /* Is INFORMATION_SCHEMA.PARAMETERS available ?*/
    private boolean haveInformationSchemaParameters() {
        //        return connection.getProtocol().versionGreaterOrEqual(5, 5, 3);
        return false;
    }

    /**
     * Retrieves a description of the given catalog's stored procedure parameter and result columns.
     *
     * <p>Only descriptions matching the schema, procedure and parameter name criteria are returned.
     * They are ordered by PROCEDURE_CAT, PROCEDURE_SCHEM, PROCEDURE_NAME and SPECIFIC_NAME. Within
     * this, the return value, if any, is first. Next are the parameter descriptions in call order.
     * The column descriptions follow in column number order.
     *
     * <p>Each row in the <code>ResultSet</code> is a parameter description or column description with
     * the following fields:
     *
     * <OL>
     * <LI><B>PROCEDURE_CAT</B> String {@code =>} procedure catalog (may be <code>null</code>)
     * <LI><B>PROCEDURE_SCHEM</B> String {@code =>} procedure schema (may be <code>null</code>)
     * <LI><B>PROCEDURE_NAME</B> String {@code =>} procedure name
     * <LI><B>COLUMN_NAME</B> String {@code =>} column/parameter name
     * <LI><B>COLUMN_TYPE</B> Short {@code =>} kind of column/parameter:
     * <UL>
     * <LI>procedureColumnUnknown - nobody knows
     * <LI>procedureColumnIn - IN parameter
     * <LI>procedureColumnInOut - INOUT parameter
     * <LI>procedureColumnOut - OUT parameter
     * <LI>procedureColumnReturn - procedure return value
     * <LI>procedureColumnResult - result column in <code>ResultSet</code>
     * </UL>
     * <LI><B>DATA_TYPE</B> int {@code =>} SQL type from java.sql.Types
     * <LI><B>TYPE_NAME</B> String {@code =>} SQL type name, for a UDT type the type name is fully
     * qualified
     * <LI><B>PRECISION</B> int {@code =>} precision
     * <LI><B>LENGTH</B> int {@code =>} length in bytes of data
     * <LI><B>SCALE</B> short {@code =>} scale - null is returned for data types where SCALE is not
     * applicable.
     * <LI><B>RADIX</B> short {@code =>} radix
     * <LI><B>NULLABLE</B> short {@code =>} can it contain NULL.
     * <UL>
     * <LI>procedureNoNulls - does not allow NULL values
     * <LI>procedureNullable - allows NULL values
     * <LI>procedureNullableUnknown - nullability unknown
     * </UL>
     * <LI><B>REMARKS</B> String {@code =>} comment describing parameter/column
     * <LI><B>COLUMN_DEF</B> String {@code =>} default value for the column, which should be
     * interpreted as a string when the value is enclosed in single quotes (may be <code>null
     * </code>)
     * <UL>
     * <LI>The string NULL (not enclosed in quotes) - if NULL was specified as the default
     * value
     * <LI>TRUNCATE (not enclosed in quotes) - if the specified default value cannot be
     * represented without truncation
     * <LI>NULL - if a default value was not specified
     * </UL>
     * <LI><B>SQL_DATA_TYPE</B> int {@code =>} reserved for future use
     * <LI><B>SQL_DATETIME_SUB</B> int {@code =>} reserved for future use
     * <LI><B>CHAR_OCTET_LENGTH</B> int {@code =>} the maximum length of binary and character based
     * columns. For any other datatype the returned value is a NULL
     * <LI><B>ORDINAL_POSITION</B> int {@code =>} the ordinal position, starting from 1, for the
     * input and output parameters for a procedure. A value of 0 is returned if this row
     * describes the procedure's return value. For result set columns, it is the ordinal
     * position of the column in the result set starting from 1. If there are multiple result
     * sets, the column ordinal positions are implementation defined.
     * <LI><B>IS_NULLABLE</B> String {@code =>} ISO rules are used to determine the nullability for
     * a column.
     * <UL>
     * <LI>YES --- if the column can include NULLs
     * <LI>NO --- if the column cannot include NULLs
     * <LI>empty string --- if the nullability for the column is unknown
     * </UL>
     * <LI><B>SPECIFIC_NAME</B> String {@code =>} the name which uniquely identifies this procedure
     * within its schema.
     * </OL>
     *
     * <p><B>Note:</B> Some databases may not return the column descriptions for a procedure.
     *
     * <p>The PRECISION column represents the specified column size for the given column. For numeric
     * data, this is the maximum precision. For character data, this is the length in characters. For
     * datetime datatypes, this is the length in characters of the String representation (assuming the
     * maximum allowed precision of the fractional seconds component). For binary data, this is the
     * length in bytes. For the ROWID datatype, this is the length in bytes. Null is returned for data
     * types where the column size is not applicable.
     *
     * @param catalog              a catalog name; must match the catalog name as it is stored in the database; ""
     *                             retrieves those without a catalog; <code>null</code> means that the catalog name should not
     *                             be used to narrow the search
     * @param schemaPattern        a schema name pattern; must match the schema name as it is stored in the
     *                             database; "" retrieves those without a schema; <code>null</code> means that the schema name
     *                             should not be used to narrow the search
     * @param procedureNamePattern a procedure name pattern; must match the procedure name as it is
     *                             stored in the database
     * @param columnNamePattern    a column name pattern; must match the column name as it is stored in
     *                             the database
     * @return <code>ResultSet</code> - each row describes a stored procedure parameter or column
     * @throws SQLException if a database access error occurs
     * @see #getSearchStringEscape
     */
    public ResultSet getProcedureColumns(String catalog, String schemaPattern,
                                         String procedureNamePattern, String columnNamePattern)
                                                                                               throws SQLException {
        String sql;

        if (haveInformationSchemaParameters()) {
            /*
             *  Get info from information_schema.parameters
             */
            sql = "SELECT SPECIFIC_SCHEMA PROCEDURE_CAT, NULL PROCEDURE_SCHEM, SPECIFIC_NAME PROCEDURE_NAME,"
                  + " PARAMETER_NAME COLUMN_NAME, " + " CASE PARAMETER_MODE " + "  WHEN 'IN' THEN "
                  + procedureColumnIn
                  + "  WHEN 'OUT' THEN "
                  + procedureColumnOut
                  + "  WHEN 'INOUT' THEN "
                  + procedureColumnInOut
                  + "  ELSE IF(PARAMETER_MODE IS NULL,"
                  + procedureColumnReturn
                  + ","
                  + procedureColumnUnknown
                  + ")"
                  + " END COLUMN_TYPE,"
                  + dataTypeClause("DTD_IDENTIFIER")
                  + " DATA_TYPE,"
                  + "DATA_TYPE TYPE_NAME,"
                  + " CASE DATA_TYPE"
                  + "  WHEN 'time' THEN "
                  + (datePrecisionColumnExist ? "IF(DATETIME_PRECISION = 0, 10, CAST(11 + DATETIME_PRECISION as signed integer))"
                      : "10")
                  + "  WHEN 'date' THEN 10"
                  + "  WHEN 'datetime' THEN "
                  + (datePrecisionColumnExist ? "IF(DATETIME_PRECISION = 0, 19, CAST(20 + DATETIME_PRECISION as signed integer))"
                      : "19")
                  + "  WHEN 'timestamp' THEN "
                  + (datePrecisionColumnExist ? "IF(DATETIME_PRECISION = 0, 19, CAST(20 + DATETIME_PRECISION as signed integer))"
                      : "19")
                  + "  ELSE "
                  + "  IF(NUMERIC_PRECISION IS NULL, LEAST(CHARACTER_MAXIMUM_LENGTH,"
                  + Integer.MAX_VALUE
                  + "), NUMERIC_PRECISION) "
                  + " END `PRECISION`,"
                  + " CASE DATA_TYPE"
                  + "  WHEN 'time' THEN "
                  + (datePrecisionColumnExist ? "IF(DATETIME_PRECISION = 0, 10, CAST(11 + DATETIME_PRECISION as signed integer))"
                      : "10")
                  + "  WHEN 'date' THEN 10"
                  + "  WHEN 'datetime' THEN "
                  + (datePrecisionColumnExist ? "IF(DATETIME_PRECISION = 0, 19, CAST(20 + DATETIME_PRECISION as signed integer))"
                      : "19")
                  + "  WHEN 'timestamp' THEN "
                  + (datePrecisionColumnExist ? "IF(DATETIME_PRECISION = 0, 19, CAST(20 + DATETIME_PRECISION as signed integer))"
                      : "19")
                  + "  ELSE "
                  + "  IF(NUMERIC_PRECISION IS NULL, LEAST(CHARACTER_MAXIMUM_LENGTH,"
                  + Integer.MAX_VALUE
                  + "), NUMERIC_PRECISION) "
                  + " END `LENGTH`,"
                  + (datePrecisionColumnExist ? " CASE DATA_TYPE"
                                                + "  WHEN 'time' THEN CAST(DATETIME_PRECISION as signed integer)"
                                                + "  WHEN 'datetime' THEN CAST(DATETIME_PRECISION as signed integer)"
                                                + "  WHEN 'timestamp' THEN CAST(DATETIME_PRECISION as signed integer)"
                                                + "  ELSE NUMERIC_SCALE " + " END `SCALE`,"
                      : " NUMERIC_SCALE `SCALE`,")
                  + "10 RADIX,"
                  + procedureNullableUnknown
                  + " NULLABLE,NULL REMARKS,NULL COLUMN_DEF,0 SQL_DATA_TYPE,0 SQL_DATETIME_SUB,"
                  + "CHARACTER_OCTET_LENGTH CHAR_OCTET_LENGTH ,ORDINAL_POSITION, '' IS_NULLABLE, SPECIFIC_NAME "
                  + " FROM INFORMATION_SCHEMA.PARAMETERS "
                  + " WHERE "
                  + catalogCond("SPECIFIC_SCHEMA", catalog)
                  + patternCond("SPECIFIC_NAME", procedureNamePattern)
                  + patternCond("PARAMETER_NAME", columnNamePattern)
                  + " /* AND ROUTINE_TYPE='PROCEDURE' */ "
                  + " ORDER BY SPECIFIC_SCHEMA, SPECIFIC_NAME, ORDINAL_POSITION";
        } else {

            /* No information_schema.parameters
             * TODO : figure out what to do with older versions (get info via mysql.proc)
             * For now, just a dummy result set is returned.
             */
            try {
                String[] columnNames = { "PROCEDURE_CAT", "PROCEDURE_SCHEM", "PROCEDURE_NAME",
                        "COLUMN_NAME", "COLUMN_TYPE", "DATA_TYPE", "TYPE_NAME", "PRECISION",
                        "LENGTH", "SCALE", "RADIX", "NULLABLE", "REMARKS", "COLUMN_DEF",
                        "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH",
                        "ORDINAL_POSITION", "IS_NULLABLE", "SPECIFIC_NAME" };
                ColumnType[] columnTypes = { ColumnType.VARCHAR, ColumnType.VARCHAR,
                        ColumnType.VARCHAR, ColumnType.VARCHAR, ColumnType.NUMBER,
                        ColumnType.SMALLINT, ColumnType.VARCHAR,

                        ColumnType.INTEGER, ColumnType.INTEGER, ColumnType.SMALLINT,
                        ColumnType.SMALLINT, ColumnType.SMALLINT, ColumnType.VARCHAR,
                        ColumnType.VARCHAR,

                        ColumnType.INTEGER, ColumnType.INTEGER, ColumnType.INTEGER,
                        ColumnType.INTEGER, ColumnType.VARCHAR, ColumnType.VARCHAR };
                ResultSet procsAndOrFuncsRs = getProcAndFuncs(catalog, procedureNamePattern, true,
                    false, columnNames, columnTypes, false);
                return procsAndOrFuncsRs;
            } catch (SQLException throwables) {
                throw throwables;
            }
        }
        return null;
    }

    ResultSet getProcAndFuncs(String catalog, String procedureNamePattern, boolean isProcedure,
                              boolean isFunction, String[] columnNames, ColumnType[] columnTypes,
                              boolean forGetFunctionColumns) throws SQLException {

        String db = catalog;

        ResultSet results = null;

        StringBuilder procSql = new StringBuilder();

        if (this.protocol.isOracleMode()) {
            procSql
                .append("select object_name as name,object_type as type  from  all_objects where ");
            if (isProcedure && !isFunction) {
                procSql.append("object_type = 'PROCEDURE' AND ");
            } else if (!isProcedure && isFunction) {
                procSql.append("object_type = 'FUNCTION' AND ");
            }
            procSql.append("object_name LIKE ? AND owner = ? ORDER BY object_name, object_type");
        } else {
            procSql.append("SELECT name, type, comment  FROM mysql.proc WHERE ");
            if (isProcedure && !isFunction) {
                procSql.append("type = 'PROCEDURE' AND ");
            } else if (!isProcedure && isFunction) {
                procSql.append("type = 'FUNCTION' AND ");
            }
            procSql.append("name LIKE ? AND db <=> ? ORDER BY name, type");
        }

        try {
            PreparedStatement proceduresStmt = this.connection.clientPrepareStatement(procSql
                .toString());
            if (procedureNamePattern == null || procedureNamePattern.length() == 0) {
                procedureNamePattern = "%";
            }
            proceduresStmt.setString(1, procedureNamePattern);

            if (db != null || this.protocol.getDatabase() != null) {
                db = (db == null) ? this.protocol.getDatabase() : db;
                proceduresStmt.setString(2, db);
                catalog = db;
            } else {
                proceduresStmt.setNull(2, Types.VARCHAR);
            }
            results = proceduresStmt.executeQuery();
        } catch (SQLException e) {
            PreparedStatement proceduresStmt = null;
            if (isFunction) {
                proceduresStmt = this.connection
                    .clientPrepareStatement("SHOW FUNCTION STATUS LIKE ?");
            } else if (isProcedure) {
                proceduresStmt = this.connection
                    .clientPrepareStatement("SHOW PROCEDURE STATUS LIKE ?");
            }
            if (procedureNamePattern == null || procedureNamePattern.length() == 0) {
                procedureNamePattern = "%";
            }
            proceduresStmt.setString(1, procedureNamePattern);
            results = proceduresStmt.executeQuery();
        }
        return getCallStmtParameterTypes(catalog, results, columnNames, columnTypes,
            forGetFunctionColumns);
    }

    class TypeInfo {
        int    bufferLength;
        int    columnSize;
        String decimalDigits = null;
        String isNullable;
        int    nullability;
        int    numPrecRadix  = 10;
        String typeName;
        int    sqlType;

        TypeInfo(String fullTypeName) {

            bufferLength = 65535;
            boolean isUnsigned = false;
            this.nullability = columnNullable;
            this.isNullable = "YES";
            String mysqlType;
            if (fullTypeName.indexOf("(") != -1) {
                mysqlType = fullTypeName.substring(0, fullTypeName.indexOf("(")).trim();
            } else {
                mysqlType = fullTypeName;
            }
            int indexOfUnsignedInMysqlType = mysqlType.toLowerCase((Locale.ROOT)).indexOf(
                "unsigned");

            if (indexOfUnsignedInMysqlType != -1) {
                mysqlType = mysqlType.substring(0, (indexOfUnsignedInMysqlType - 1));
            }
            this.typeName = mysqlType;
            sqlType = mapMariaDbTypeToJdbc(mysqlType);

            int indexUnsigned = fullTypeName.toLowerCase(Locale.ROOT).indexOf("unsigned");
            if (indexUnsigned != -1) {
                fullTypeName = fullTypeName.substring(0, indexUnsigned - 1);
                isUnsigned = true;
            }

            if (fullTypeName.toLowerCase(Locale.ROOT).startsWith("enum")) {
                String sub = fullTypeName.substring(fullTypeName.indexOf("("),
                    fullTypeName.indexOf(")"));
                StringTokenizer tokenizer = new StringTokenizer(sub, ",");
                int length = 0;
                while (tokenizer.hasMoreTokens()) {
                    length = Math.max(length, (tokenizer.nextToken().length() - 2));
                }
                this.columnSize = length;
            }
            if (fullTypeName.toLowerCase(Locale.ROOT).startsWith("set")) {
                String sub = fullTypeName.substring(fullTypeName.indexOf("("),
                    fullTypeName.indexOf(")"));
                StringTokenizer tokenizer = new StringTokenizer(sub, ",");
                int length = 0;
                this.columnSize = 0;
                int num = tokenizer.countTokens();
                if (num > 0) {
                    length += (num - 1);
                }
                while (tokenizer.hasMoreTokens()) {
                    String setMember = tokenizer.nextToken().trim();

                    if (setMember.startsWith("'") && setMember.endsWith("'")) {
                        length += setMember.length() - 2;
                    } else {
                        length += setMember.length();
                    }
                }
                this.columnSize = length;
            } else if (fullTypeName.indexOf(",") != -1) {
                this.columnSize = Integer.valueOf(fullTypeName.substring(
                    fullTypeName.indexOf("(") + 1, fullTypeName.indexOf(",")));
                this.decimalDigits = fullTypeName.substring(fullTypeName.indexOf(",") + 1,
                    fullTypeName.indexOf(")"));

            } else {
                if ((fullTypeName.toLowerCase(Locale.ROOT).indexOf("char") != -1
                     || fullTypeName.toLowerCase(Locale.ROOT).indexOf("text") != -1
                     || fullTypeName.toLowerCase(Locale.ROOT).indexOf("binary") != -1
                     || fullTypeName.toLowerCase(Locale.ROOT).indexOf("blob") != -1 || fullTypeName
                    .toLowerCase(Locale.ROOT).indexOf("bit") != -1)
                    && fullTypeName.indexOf("(") != -1) {
                    int endIndex = fullTypeName.indexOf(")");
                    if (endIndex == -1) {
                        endIndex = fullTypeName.length();
                    }
                    this.columnSize = Integer.valueOf(fullTypeName.substring(
                        fullTypeName.indexOf("(") + 1, endIndex));
                    // Adjust for pseudo-boolean  todo
                } else if (fullTypeName.toLowerCase(Locale.ROOT).startsWith("tinyint")) {
                    // Adjust for pseudo-boolean  todo
                    this.columnSize = 5;
                    this.decimalDigits = "0";
                } else if (fullTypeName.toLowerCase(Locale.ROOT).startsWith("smallint")) {
                    this.columnSize = 5;
                    this.decimalDigits = "0";
                } else if (fullTypeName.toLowerCase(Locale.ROOT).startsWith("mediumint")) {
                    this.columnSize = isUnsigned ? 8 : 7;
                    this.decimalDigits = "0";
                } else if (fullTypeName.toLowerCase(Locale.ROOT).startsWith("int")) {
                    this.columnSize = 10;
                    this.decimalDigits = "0";
                } else if (fullTypeName.toLowerCase(Locale.ROOT).startsWith("integer")) {
                    this.columnSize = 10;
                    this.decimalDigits = "0";
                } else if (fullTypeName.toLowerCase(Locale.ROOT).startsWith("bigint")) {
                    this.columnSize = isUnsigned ? 20 : 19;
                    this.decimalDigits = "0";
                } else if (fullTypeName.toLowerCase(Locale.ROOT).startsWith("int24")) {
                    this.columnSize = 19;
                    this.decimalDigits = "0";
                } else if (fullTypeName.toLowerCase(Locale.ROOT).startsWith("real")) {
                    this.columnSize = 12;
                } else if (fullTypeName.toLowerCase(Locale.ROOT).startsWith("float")) {
                    this.columnSize = 12;
                } else if (fullTypeName.toLowerCase(Locale.ROOT).startsWith("decimal")) {
                    this.columnSize = 12;
                } else if (fullTypeName.toLowerCase(Locale.ROOT).startsWith("numeric")) {
                    this.columnSize = 12;
                } else if (fullTypeName.toLowerCase(Locale.ROOT).startsWith("double")) {
                    this.columnSize = 12;
                } else if (fullTypeName.toLowerCase(Locale.ROOT).startsWith("char")) {
                    this.columnSize = 1;
                } else if (fullTypeName.toLowerCase(Locale.ROOT).startsWith("varchar")) {
                    this.columnSize = 255;
                } else if (fullTypeName.toLowerCase(Locale.ROOT).startsWith("timestamp")) {
                    this.columnSize = 19;
                } else if (fullTypeName.toLowerCase(Locale.ROOT).startsWith("datetime")) {
                    this.columnSize = 19;
                } else if (fullTypeName.toLowerCase(Locale.ROOT).startsWith("date")) {
                    this.columnSize = 10;
                } else if (fullTypeName.toLowerCase(Locale.ROOT).startsWith("time")) {
                    this.columnSize = 8;
                } else if (fullTypeName.toLowerCase(Locale.ROOT).startsWith("tinyblob")) {
                    this.columnSize = 255;
                } else if (fullTypeName.toLowerCase(Locale.ROOT).startsWith("blob")) {
                    this.columnSize = 65535;
                } else if (fullTypeName.toLowerCase(Locale.ROOT).startsWith("mediumblob")) {
                    this.columnSize = 16777215;
                } else if (fullTypeName.toLowerCase(Locale.ROOT).startsWith("longblob")) {
                    this.columnSize = Integer.MAX_VALUE;
                } else if (fullTypeName.toLowerCase(Locale.ROOT).startsWith("tinytext")) {
                    this.columnSize = 255;
                } else if (fullTypeName.toLowerCase(Locale.ROOT).startsWith("text")) {
                    this.columnSize = 65535;
                } else if (fullTypeName.toLowerCase(Locale.ROOT).startsWith("mediumtext")) {
                    this.columnSize = 16777215;
                } else if (fullTypeName.toLowerCase(Locale.ROOT).startsWith("longtext")) {
                    this.columnSize = Integer.MAX_VALUE;
                } else if (fullTypeName.toLowerCase(Locale.ROOT).startsWith("enum")) {
                    this.columnSize = 255;
                } else if (fullTypeName.toLowerCase(Locale.ROOT).startsWith("set")) {
                    this.columnSize = 255;
                }
            }
        }
    }

    private int mapMariaDbTypeToJdbc(String str) {
        switch (str.toUpperCase(Locale.ROOT)) {
            case "BIT":
                return Types.BIT;
            case "TINYINT":
                return Types.TINYINT;
            case "SMALLINT":
                return Types.SMALLINT;
            case "MEDIUMINT":
                return Types.INTEGER;
            case "INT":
                return Types.INTEGER;
            case "INTEGER":
                return Types.INTEGER;
            case "LONG":
                return Types.INTEGER;
            case "BIGINT":
                return Types.BIGINT;
            case "INT24":
                return Types.INTEGER;
            case "REAL":
                return Types.DOUBLE;
            case "FLOAT":
                return Types.FLOAT;
            case "DECIMAL":
                return Types.DECIMAL;
            case "NUMERIC":
                return Types.NUMERIC;
            case "DOUBLE":
                return Types.DOUBLE;
            case "CHAR":
                return Types.CHAR;
            case "VARCHAR":
                return Types.VARCHAR;
            case "DATE":
                return Types.DATE;
            case "TIME":
                return Types.TIME;
            case "YEAR":
                return Types.SMALLINT;
            case "TIMESTAMP":
                return Types.TIMESTAMP;
            case "DATETIME":
                return Types.TIMESTAMP;
            case "TINYBLOB":
                return Types.BINARY;
            case "BLOB":
                return Types.LONGVARBINARY;
            case "MEDIUMBLOB":
                return Types.LONGVARBINARY;
            case "LONGBLOB":
                return Types.LONGVARBINARY;
            case "TINYTEXT":
                return Types.VARCHAR;
            case "TEXT":
                return Types.LONGVARCHAR;
            case "MEDIUMTEXT":
                return Types.LONGVARCHAR;
            case "LONGTEXT":
                return Types.LONGVARCHAR;
            case "ENUM":
                return Types.VARCHAR;
            case "SET":
                return Types.VARCHAR;
            case "GEOMETRY":
                return Types.LONGVARBINARY;
            case "VARBINARY":
                return Types.VARBINARY;
            default:
                return Types.OTHER;
        }
    }

    protected int getColumnType(boolean isOutParam, boolean isInParam, boolean isReturnParam,
                                boolean forGetFunctionColumns) {

        if (isInParam && isOutParam) {
            return forGetFunctionColumns ? functionColumnInOut : procedureColumnInOut;
        } else if (isInParam) {
            return forGetFunctionColumns ? functionColumnIn : procedureColumnIn;
        } else if (isOutParam) {
            return forGetFunctionColumns ? functionColumnOut : procedureColumnOut;
        } else if (isReturnParam) {
            return forGetFunctionColumns ? functionReturn : procedureColumnReturn;
        } else {
            return forGetFunctionColumns ? functionColumnUnknown : procedureColumnUnknown;
        }
    }

    private ResultSet parseParamListProc(boolean isFunction, ArrayList<String> paramListArray,String catalog,ArrayList<String> nameArray,String[] columnNames,ColumnType[] columnTypes,boolean forGetFunctionColumns) throws SQLException {
        List<String[]> list = new ArrayList<>();
        int origin = 1;
        for (int i = 0;i<paramListArray.size();i++) {
            String paramList = paramListArray.get(i);
            String procName = nameArray.get(i);
            Matcher matcher2;
            Matcher matcher1;
            if (protocol.isOracleMode()) {
                matcher1 = ORALCLE_PARAMETER_PATTERN.matcher(paramList);
            } else {
                matcher1 = PARAMETER_PATTERN.matcher(paramList);
            }
            TypeInfo typeInfo = null;
            boolean isInParam = false;
            boolean isOutParam = false;
            boolean isReturnParam = false;
            String[] data = new String[20];
            if ( paramList != null && paramList.indexOf("(") != 0 && !matcher1.find()) {
                String[] tokens = new String[] { "LANGUAGE", "NOT", "DETERMINISTIC", "CONTAINS", "NO",
                        "READ", "MODIFIES", "SQL", "COMMENT", "BEGIN", "RETURN" };
                int startIndex = 0;
                int endIndex = -1;
                for(int j = 0 ;j<tokens.length;j++) {
                    endIndex = paramList.substring(startIndex).toUpperCase(Locale.ROOT).indexOf(tokens[j]);
                    if(endIndex != -1) {
                        startIndex = endIndex +1;
                    }
                }
                if(endIndex != -1) {
                    paramList = paramList.substring(0,endIndex);
                }
                isInParam = false;
                isOutParam = false;
                isReturnParam = true;
                typeInfo = new TypeInfo(paramList.toUpperCase(Locale.ROOT));
                String[] tmp = getRowData(catalog,"",procName,forGetFunctionColumns,isInParam,isOutParam,isReturnParam,typeInfo,0);
                list.add(tmp);

            } else {
                if (protocol.isOracleMode()) {
                    matcher2 = ORALCLE_PARAMETER_PATTERN.matcher(paramList);
                } else {
                    matcher2 = PARAMETER_PATTERN.matcher(paramList);
                }
                while (matcher2.find()) {
                    String direction = matcher2.group(1);
                    if (direction != null) {
                        direction = direction.trim();
                    }
                    isInParam = false;
                    isOutParam = false;
                    isReturnParam = false;
                    if (direction == null || direction.equalsIgnoreCase("IN")) {
                        isInParam = true;
                    } else if (direction.equalsIgnoreCase("OUT")) {
                        isOutParam = true;
                    } else if (direction.equalsIgnoreCase("INOUT")) {
                        isInParam = true;
                        isOutParam = true;
                    } else {
                        throw new SQLException(
                                "unknown parameter direction " + direction + "for "); // todo
                    }
                    String paramName = matcher2.group(2).trim();
                    if (this.protocol.isOracleMode()) {
                        typeInfo = new TypeInfo(matcher2.group(4).trim().toUpperCase(Locale.ROOT));
                    } else {
                        if(matcher2.group(5) != null) {
                            typeInfo = new TypeInfo(matcher2.group(4).trim().toUpperCase(Locale.ROOT) + matcher2.group(5).trim().toUpperCase(Locale.ROOT));
                        } else {
                            typeInfo = new TypeInfo(matcher2.group(4).trim().toUpperCase(Locale.ROOT));
                        }

                    }
                    if (paramName.startsWith("`") && paramName.endsWith("`")) {
                        paramName = paramName.substring(1, paramName.length() - 1);
                    } else if (paramName.startsWith("") && paramName.endsWith("")) {
                        paramName = paramName.substring(1, paramName.length() - 1);
                    }
                    String[] tmp = getRowData(catalog,paramName,procName,forGetFunctionColumns,isInParam,isOutParam,isReturnParam,typeInfo,origin++);
                    list.add(tmp);
                }
            }

        }
        String[][] val = new String[list.size()][];
        for (int j = 0; j < list.size(); j++) {
            val[j] = list.get(j);
        }
        return  JDBC4ResultSet.createResultSet(columnNames, columnTypes, val, this.protocol);

    }

    String[] getRowData(String catalog, String paramName, String procName,
                        boolean forGetFunctionColumns, boolean isInParam, boolean isOutParam,
                        boolean isReturnParam, TypeInfo typeInfo, int origin) throws SQLException {
        String[] data = new String[20];
        if (paramName.startsWith("`") && paramName.endsWith("`")) {
            paramName = paramName.substring(1, paramName.length() - 1);
        } else if (paramName.startsWith("'") && paramName.endsWith("'")) {
            paramName = paramName.substring(1, paramName.length() - 1);
        }
        data[0] = catalog;
        data[1] = null;
        data[2] = procName;
        data[3] = paramName;
        data[4] = String.valueOf(getColumnType(isOutParam, isInParam, isReturnParam,
            forGetFunctionColumns));
        data[5] = Integer.toString(typeInfo.sqlType);
        data[6] = typeInfo.typeName;
        data[7] = String.valueOf(typeInfo.columnSize);
        data[8] = data[7];
        data[9] = typeInfo.decimalDigits;
        data[10] = String.valueOf(typeInfo.numPrecRadix);
        switch (typeInfo.nullability) {
            case columnNoNulls:
                data[11] = String.valueOf(procedureNoNulls);
                break;

            case columnNullable:
                data[11] = String.valueOf(procedureNullable); //NULLABLE
                break;

            case columnNullableUnknown:
                data[11] = String.valueOf(procedureNullableUnknown); //NULLABLE
                break;

            default:
                throw new SQLException(
                    "Internal error while parsing callable statement metadata (unknown nullability value fount");
        }
        data[12] = null;
        if (forGetFunctionColumns) {
            data[13] = null;
            data[14] = Integer.toString(origin);
            data[15] = typeInfo.isNullable; // todo
            data[16] = procName;
        } else {
            data[13] = null;
            data[14] = null;
            data[15] = null;
            data[16] = null;
            data[17] = Integer.toString(origin);
            data[18] = typeInfo.isNullable;
            data[19] = procName;
        }
        String[] tmp = new String[20];
        System.arraycopy(data, 0, tmp, 0, data.length);
        return tmp;

    }

    ResultSet getCallStmtParameterTypes(String catalog, ResultSet procsAndOrFuncsRs,
                                        String[] columnNames, ColumnType[] columnTypes, boolean forGetFunctionColumns)
            throws SQLException {
        ArrayList<String> paramListArray = new ArrayList<>();
        ArrayList<String> nameArray = new ArrayList<>();
        while(procsAndOrFuncsRs.next()) {
            String name = procsAndOrFuncsRs.getString("name");

            StringBuilder procNameBuf = new StringBuilder();
            String type = procsAndOrFuncsRs.getString("type");
            if (this.protocol.isOracleMode()) {
                procNameBuf.append("\"");
                procNameBuf.append(name);
                procNameBuf.append("\"");
            } else {
                procNameBuf.append("`");
                procNameBuf.append(catalog);
                procNameBuf.append("`.`");
                procNameBuf.append(name);
                procNameBuf.append("`");
            }
            if("FUNCTION".equals(type)) {
                ResultSet paramRetrievalRs = this.connection.createStatement().executeQuery(
                        "SHOW CREATE FUNCTION " + procNameBuf.toString());
                paramRetrievalRs.next();
                String procedureDDl = paramRetrievalRs.getString("Create Function");
                int startIndex = procedureDDl.indexOf("(");
                int returnIndex = procedureDDl.indexOf("RETURNS");
                if (returnIndex != -1 && returnIndex > startIndex) {
                    String returnList = procedureDDl.substring(returnIndex + "RESTURNS".length()
                    );
                    paramListArray.add(returnList);
                    nameArray.add(name);
                    String paramList  = procedureDDl.substring(startIndex - 1,
                            returnIndex-1);
                    paramListArray.add(paramList);
                    nameArray.add(name);
                } else {
                    String paramList = procedureDDl.substring(procedureDDl.indexOf("(") - 1
                    );
                    paramListArray.add(paramList);
                    nameArray.add(name);
                }
            } else {
                ResultSet paramRetrievalRs = this.connection.createStatement().executeQuery(
                        "SHOW CREATE PROCEDURE " + procNameBuf.toString());
                paramRetrievalRs.next();

                String procedureDDl = paramRetrievalRs.getString("Create Procedure");
                int startIndex = procedureDDl.indexOf("(");
                int endIdex = procedureDDl.toUpperCase().indexOf("BEGIN");
                String paramList = null;
                if (endIdex != -1 && endIdex > startIndex) {
                    paramList  = procedureDDl.substring(startIndex - 1,
                            endIdex -1);
                } else {
                    paramList = procedureDDl.substring(procedureDDl.indexOf("(") - 1);
                }
                paramListArray.add(paramList);
                nameArray.add(name);
            }
        }

        return parseParamListProc(false, paramListArray, catalog, nameArray, columnNames, columnTypes,forGetFunctionColumns);
    }

    /**
     * Retrieves a description of the given catalog's system or user function parameters and return
     * type.
     *
     * <p>Only descriptions matching the schema, function and parameter name criteria are returned.
     * They are ordered by <code>FUNCTION_CAT</code>, <code>FUNCTION_SCHEM</code>, <code>FUNCTION_NAME
     * </code> and <code>SPECIFIC_ NAME</code>. Within this, the return value, if any, is first. Next
     * are the parameter descriptions in call order. The column descriptions follow in column number
     * order.
     *
     * <p>Each row in the <code>ResultSet</code> is a parameter description, column description or
     * return type description with the following fields:
     *
     * <OL>
     * <LI><B>FUNCTION_CAT</B> String {@code =>} function catalog (may be <code>null</code>)
     * <LI><B>FUNCTION_SCHEM</B> String {@code =>} function schema (may be <code>null</code>)
     * <LI><B>FUNCTION_NAME</B> String {@code =>} function name. This is the name used to invoke the
     * function
     * <LI><B>COLUMN_NAME</B> String {@code =>} column/parameter name
     * <LI><B>COLUMN_TYPE</B> Short {@code =>} kind of column/parameter:
     * <UL>
     * <LI>functionColumnUnknown - nobody knows
     * <LI>functionColumnIn - IN parameter
     * <LI>functionColumnInOut - INOUT parameter
     * <LI>functionColumnOut - OUT parameter
     * <LI>functionColumnReturn - function return value
     * <LI>functionColumnResult - Indicates that the parameter or column is a column in the
     * <code>ResultSet</code>
     * </UL>
     * <LI><B>DATA_TYPE</B> int {@code =>} SQL type from java.sql.Types
     * <LI><B>TYPE_NAME</B> String {@code =>} SQL type name, for a UDT type the type name is fully
     * qualified
     * <LI><B>PRECISION</B> int {@code =>} precision
     * <LI><B>LENGTH</B> int {@code =>} length in bytes of data
     * <LI><B>SCALE</B> short {@code =>} scale - null is returned for data types where SCALE is not
     * applicable.
     * <LI><B>RADIX</B> short {@code =>} radix
     * <LI><B>NULLABLE</B> short {@code =>} can it contain NULL.
     * <UL>
     * <LI>functionNoNulls - does not allow NULL values
     * <LI>functionNullable - allows NULL values
     * <LI>functionNullableUnknown - nullability unknown
     * </UL>
     * <LI><B>REMARKS</B> String {@code =>} comment describing column/parameter
     * <LI><B>CHAR_OCTET_LENGTH</B> int {@code =>} the maximum length of binary and character based
     * parameters or columns. For any other datatype the returned value is a NULL
     * <LI><B>ORDINAL_POSITION</B> int {@code =>} the ordinal position, starting from 1, for the
     * input and output parameters. A value of 0 is returned if this row describes the
     * function's return value. For result set columns, it is the ordinal position of the column
     * in the result set starting from 1.
     * <LI><B>IS_NULLABLE</B> String {@code =>} ISO rules are used to determine the nullability for
     * a parameter or column.
     * <UL>
     * <LI>YES --- if the parameter or column can include NULLs
     * <LI>NO --- if the parameter or column cannot include NULLs
     * <LI>empty string --- if the nullability for the parameter or column is unknown
     * </UL>
     * <LI><B>SPECIFIC_NAME</B> String {@code =>} the name which uniquely identifies this function
     * within its schema. This is a user specified, or DBMS generated, name that may be
     * different then the <code>FUNCTION_NAME</code> for example with overload functions
     * </OL>
     *
     * <p>The PRECISION column represents the specified column size for the given parameter or column.
     * For numeric data, this is the maximum precision. For character data, this is the length in
     * characters. For datetime datatypes, this is the length in characters of the String
     * representation (assuming the maximum allowed precision of the fractional seconds component).
     * For binary data, this is the length in bytes. For the ROWID datatype, this is the length in
     * bytes. Null is returned for data types where the column size is not applicable.
     *
     * @param catalog             a catalog name; must match the catalog name as it is stored in the database; ""
     *                            retrieves those without a catalog; <code>null</code> means that the catalog name should not
     *                            be used to narrow the search
     * @param schemaPattern       a schema name pattern; must match the schema name as it is stored in the
     *                            database; "" retrieves those without a schema; <code>null</code> means that the schema name
     *                            should not be used to narrow the search
     * @param functionNamePattern a procedure name pattern; must match the function name as it is
     *                            stored in the database
     * @param columnNamePattern   a parameter name pattern; must match the parameter or column name as
     *                            it is stored in the database
     * @return <code>ResultSet</code> - each row describes a user function parameter, column or return
     * type
     * @throws SQLException if a database access error occurs
     * @see #getSearchStringEscape
     * @since 1.6
     */
    public ResultSet getFunctionColumns(String catalog, String schemaPattern,
                                        String functionNamePattern, String columnNamePattern)
                                                                                             throws SQLException {
        //todo unfinished
        //        if (this.connection.getProtocol().isOracleMode()){
        //            super.getFunctionColumns(catalog,schemaPattern,functionNamePattern,columnNamePattern);
        //        }
        String sql;
        if (haveInformationSchemaParameters()) {

            sql = "SELECT SPECIFIC_SCHEMA `FUNCTION_CAT`, NULL `FUNCTION_SCHEM`, SPECIFIC_NAME FUNCTION_NAME,"
                  + " PARAMETER_NAME COLUMN_NAME, "
                  + " CASE PARAMETER_MODE "
                  + "  WHEN 'IN' THEN "
                  + functionColumnIn
                  + "  WHEN 'OUT' THEN "
                  + functionColumnOut
                  + "  WHEN 'INOUT' THEN "
                  + functionColumnInOut
                  + "  ELSE "
                  + functionReturn
                  + " END COLUMN_TYPE,"
                  + dataTypeClause("DTD_IDENTIFIER")
                  + " DATA_TYPE,"
                  + "DATA_TYPE TYPE_NAME,NUMERIC_PRECISION `PRECISION`,CHARACTER_MAXIMUM_LENGTH LENGTH,NUMERIC_SCALE SCALE,10 RADIX,"
                  + procedureNullableUnknown
                  + " NULLABLE,NULL REMARKS,"
                  + "CHARACTER_OCTET_LENGTH CHAR_OCTET_LENGTH ,ORDINAL_POSITION, '' IS_NULLABLE, SPECIFIC_NAME "
                  + " FROM INFORMATION_SCHEMA.PARAMETERS "
                  + " WHERE "
                  + catalogCond("SPECIFIC_SCHEMA", catalog)
                  + patternCond("SPECIFIC_NAME", functionNamePattern)
                  + patternCond("PARAMETER_NAME", columnNamePattern)
                  + " AND ROUTINE_TYPE='FUNCTION'"
                  + " ORDER BY FUNCTION_CAT, SPECIFIC_NAME, ORDINAL_POSITION";
        } else {
            /*
             * No information_schema.parameters
             * TODO : figure out what to do with older versions (get info via mysql.proc)
             * For now, just a dummy result set is returned.
             */

        }
        String[] columnNames = { "FUNCTION_CAT", "FUNCTION_SCHEM", "FUNCTION_NAME", "COLUMN_NAME",
                "COLUMN_TYPE", "DATA_TYPE", "TYPE_NAME", "PRECISION", "LENGTH", "SCALE", "RADIX",
                "NULLABLE", "REMARKS", "CHAR_OCTET_LENGTH", "ORDINAL_POSITION", "IS_NULLABLE",
                "SPECIFIC_NAME" };
        ColumnType[] columnTypes = { ColumnType.VARCHAR, ColumnType.VARCHAR, ColumnType.VARCHAR,
                ColumnType.VARCHAR, ColumnType.NUMBER, ColumnType.SMALLINT, ColumnType.VARCHAR,

                ColumnType.INTEGER, ColumnType.INTEGER, ColumnType.SMALLINT, ColumnType.SMALLINT,
                ColumnType.SMALLINT, ColumnType.VARCHAR, ColumnType.NUMBER, ColumnType.NUMBER,
                ColumnType.VARCHAR, ColumnType.VARCHAR };
        ResultSet procsAndOrFuncsRs = getProcAndFuncs(catalog, functionNamePattern, false, true,
            columnNames, columnTypes, true);
        return procsAndOrFuncsRs;
    }

    public ResultSet getSchemas() throws SQLException {
        if (connection.getProtocol().isOracleMode()) {
            return super.getSchemas();
        }
        return executeQuery("SELECT '' TABLE_SCHEM, '' TABLE_catalog  FROM DUAL WHERE 1=0");
    }

    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        if (connection.getProtocol().isOracleMode()) {
            return super.getSchemas(catalog, schemaPattern);
        }
        return executeQuery("SELECT  ' ' table_schem, ' ' table_catalog FROM DUAL WHERE 1=0");
    }

    public ResultSet getCatalogs() throws SQLException {
        if (connection.getProtocol().isOracleMode()) {
            return super.getCatalogs();
        }
        return executeQuery("SELECT SCHEMA_NAME  TABLE_CAT FROM INFORMATION_SCHEMA.SCHEMATA ORDER BY 1");
    }

    public ResultSet getTableTypes() throws SQLException {
        if (this.connection.getProtocol().isOracleMode()) {
            return super.getTableTypes();
        }
        return executeQuery("SELECT 'TABLE' TABLE_TYPE UNION SELECT 'SYSTEM VIEW' TABLE_TYPE UNION SELECT 'VIEW' TABLE_TYPE");
    }

    /**
     * Retrieves a description of the access rights for a table's columns.
     *
     * <p>Only privileges matching the column name criteria are returned. They are ordered by
     * COLUMN_NAME and PRIVILEGE.
     *
     * <p>Each privilege description has the following columns:
     *
     * <OL>
     * <LI><B>TABLE_CAT</B> String {@code =>} table catalog (may be <code>null</code>)
     * <LI><B>TABLE_SCHEM</B> String {@code =>} table schema (may be <code>null</code>)
     * <LI><B>TABLE_NAME</B> String {@code =>} table name
     * <LI><B>COLUMN_NAME</B> String {@code =>} column name
     * <LI><B>GRANTOR</B> String {@code =>} grantor of access (may be <code>null</code>)
     * <LI><B>GRANTEE</B> String {@code =>} grantee of access
     * <LI><B>PRIVILEGE</B> String {@code =>} name of access (SELECT, INSERT, UPDATE, REFRENCES,
     * ...)
     * <LI><B>IS_GRANTABLE</B> String {@code =>} "YES" if grantee is permitted to grant to others;
     * "NO" if not; <code>null</code> if unknown
     * </OL>
     *
     * @param catalog           a catalog name; must match the catalog name as it is stored in the database; ""
     *                          retrieves those without a catalog; <code>null</code> means that the catalog name should not
     *                          be used to narrow the search
     * @param schema            a schema name; must match the schema name as it is stored in the database; ""
     *                          retrieves those without a schema; <code>null</code> means that the schema name should not
     *                          be used to narrow the search
     * @param table             a table name; must match the table name as it is stored in the database
     * @param columnNamePattern a column name pattern; must match the column name as it is stored in
     *                          the database
     * @return <code>ResultSet</code> - each row is a column privilege description
     * @throws SQLException if a database access error occurs
     * @see #getSearchStringEscape
     */
    public ResultSet getColumnPrivileges(String catalog, String schema, String table,
                                         String columnNamePattern) throws SQLException {

        if (table == null) {
            throw new SQLException("'table' parameter must not be null");
        }

        if (this.connection.getProtocol().isOracleMode()) {
            return super.getColumnPrivileges(catalog, schema, table, columnNamePattern);
        }
        String sql = "SELECT TABLE_SCHEMA TABLE_CAT, NULL TABLE_SCHEM, TABLE_NAME,"
                     + " COLUMN_NAME, NULL AS GRANTOR, GRANTEE, PRIVILEGE_TYPE AS PRIVILEGE, IS_GRANTABLE FROM "
                     + " INFORMATION_SCHEMA.COLUMN_PRIVILEGES WHERE "
                     + catalogCond("TABLE_SCHEMA", catalog) + " AND " + " TABLE_NAME = "
                     + escapeQuote(table) + patternCond("COLUMN_NAME", columnNamePattern)
                     + " ORDER BY COLUMN_NAME, PRIVILEGE_TYPE";

        return executeQuery(sql);
    }

    /**
     * Retrieves a description of the access rights for each table available in a catalog. Note that a
     * table privilege applies to one or more columns in the table. It would be wrong to assume that
     * this privilege applies to all columns (this may be true for some systems but is not true for
     * all.)
     *
     * <p>Only privileges matching the schema and table name criteria are returned. They are ordered
     * by <code>TABLE_CAT</code>, <code>TABLE_SCHEM</code>, <code>TABLE_NAME</code>, and <code>
     * PRIVILEGE</code>.
     *
     * <p>Each privilege description has the following columns:
     *
     * <OL>
     * <LI><B>TABLE_CAT</B> String {@code =>} table catalog (may be <code>null</code>)
     * <LI><B>TABLE_SCHEM</B> String {@code =>} table schema (may be <code>null</code>)
     * <LI><B>TABLE_NAME</B> String {@code =>} table name
     * <LI><B>GRANTOR</B> String {@code =>} grantor of access (may be <code>null</code>)
     * <LI><B>GRANTEE</B> String {@code =>} grantee of access
     * <LI><B>PRIVILEGE</B> String {@code =>} name of access (SELECT, INSERT, UPDATE, REFRENCES,
     * ...)
     * <LI><B>IS_GRANTABLE</B> String {@code =>} "YES" if grantee is permitted to grant to others;
     * "NO" if not; <code>null</code> if unknown
     * </OL>
     *
     * @param catalog          a catalog name; must match the catalog name as it is stored in the database; ""
     *                         retrieves those without a catalog; <code>null</code> means that the catalog name should not
     *                         be used to narrow the search
     * @param schemaPattern    a schema name pattern; must match the schema name as it is stored in the
     *                         database; "" retrieves those without a schema; <code>null</code> means that the schema name
     *                         should not be used to narrow the search
     * @param tableNamePattern a table name pattern; must match the table name as it is stored in the
     *                         database
     * @return <code>ResultSet</code> - each row is a table privilege description
     * @throws SQLException if a database access error occurs
     * @see #getSearchStringEscape
     */
    public ResultSet getTablePrivileges(String catalog, String schemaPattern,
                                        String tableNamePattern) throws SQLException {
        if (this.connection.getProtocol().isOracleMode()) {
            return super.getTablePrivileges(catalog, schemaPattern, tableNamePattern);
        }

        String sql = "SELECT TABLE_SCHEMA TABLE_CAT,NULL  TABLE_SCHEM, TABLE_NAME, NULL GRANTOR,"
                     + "GRANTEE, PRIVILEGE_TYPE  PRIVILEGE, IS_GRANTABLE  FROM INFORMATION_SCHEMA.TABLE_PRIVILEGES "
                     + " WHERE " + catalogCond("TABLE_SCHEMA", catalog)
                     + patternCond("TABLE_NAME", tableNamePattern)
                     + "ORDER BY TABLE_SCHEMA, TABLE_NAME,  PRIVILEGE_TYPE ";

        return executeQuery(sql);
    }

    /**
     * Retrieves a description of a table's columns that are automatically updated when any value in a
     * row is updated. They are unordered.
     *
     * <p>Each column description has the following columns:
     *
     * <OL>
     * <LI><B>SCOPE</B> short {@code =>} is not used
     * <LI><B>COLUMN_NAME</B> String {@code =>} column name
     * <LI><B>DATA_TYPE</B> int {@code =>} SQL data type from <code>java.sql.Types</code>
     * <LI><B>TYPE_NAME</B> String {@code =>} Data source-dependent type name
     * <LI><B>COLUMN_SIZE</B> int {@code =>} precision
     * <LI><B>BUFFER_LENGTH</B> int {@code =>} length of column value in bytes
     * <LI><B>DECIMAL_DIGITS</B> short {@code =>} scale - Null is returned for data types where
     * DECIMAL_DIGITS is not applicable.
     * <LI><B>PSEUDO_COLUMN</B> short {@code =>} whether this is pseudo column like an Oracle ROWID
     * <UL>
     * <LI>versionColumnUnknown - may or may not be pseudo column
     * <LI>versionColumnNotPseudo - is NOT a pseudo column
     * <LI>versionColumnPseudo - is a pseudo column
     * </UL>
     * </OL>
     *
     * <p>The COLUMN_SIZE column represents the specified column size for the given column. For
     * numeric data, this is the maximum precision. For character data, this is the length in
     * characters. For datetime datatypes, this is the length in characters of the String
     * representation (assuming the maximum allowed precision of the fractional seconds component).
     * For binary data, this is the length in bytes. For the ROWID datatype, this is the length in
     * bytes. Null is returned for data types where the column size is not applicable.
     *
     * @param catalog a catalog name; must match the catalog name as it is stored in the database; ""
     *                retrieves those without a catalog;<code>null</code> means that the catalog name should not
     *                be used to narrow the search
     * @param schema  a schema name; must match the schema name as it is stored in the database; ""
     *                retrieves those without a schema; <code>null</code> means that the schema name should not
     *                be used to narrow the search
     * @param table   a table name; must match the table name as it is stored in the database
     * @return a <code>ResultSet</code> object in which each row is a column description
     * @throws SQLException if a database access error occurs
     */
    public ResultSet getVersionColumns(String catalog, String schema, String table)
                                                                                   throws SQLException {
        String sql = "SELECT 0 SCOPE, ' ' COLUMN_NAME, 0 DATA_TYPE,"
                     + " ' ' TYPE_NAME, 0 COLUMN_SIZE, 0 BUFFER_LENGTH,"
                     + " 0 DECIMAL_DIGITS, 0 PSEUDO_COLUMN " + " FROM DUAL WHERE 1 = 0";
        return executeQuery(sql);
    }

    /**
     * Retrieves a description of the foreign key columns in the given foreign key table that
     * reference the primary key or the columns representing a unique constraint of the parent table
     * (could be the same or a different table). The number of columns returned from the parent table
     * must match the number of columns that make up the foreign key. They are ordered by FKTABLE_CAT,
     * FKTABLE_SCHEM, FKTABLE_NAME, and KEY_SEQ.
     *
     * <p>Each foreign key column description has the following columns:
     *
     * <OL>
     * <LI><B>PKTABLE_CAT</B> String {@code =>} parent key table catalog (may be <code>null</code>)
     * <LI><B>PKTABLE_SCHEM</B> String {@code =>} parent key table schema (may be <code>null</code>)
     * <LI><B>PKTABLE_NAME</B> String {@code =>} parent key table name
     * <LI><B>PKCOLUMN_NAME</B> String {@code =>} parent key column name
     * <LI><B>FKTABLE_CAT</B> String {@code =>} foreign key table catalog (may be <code>null</code>)
     * being exported (may be <code>null</code>)
     * <LI><B>FKTABLE_SCHEM</B> String {@code =>} foreign key table schema (may be <code>null</code>
     * ) being exported (may be <code>null</code>)
     * <LI><B>FKTABLE_NAME</B> String {@code =>} foreign key table name being exported
     * <LI><B>FKCOLUMN_NAME</B> String {@code =>} foreign key column name being exported
     * <LI><B>KEY_SEQ</B> short {@code =>} sequence number within foreign key( a value of 1
     * represents the first column of the foreign key, a value of 2 would represent the second
     * column within the foreign key).
     * <LI><B>UPDATE_RULE</B> short {@code =>} What happens to foreign key when parent key is
     * updated:
     * <UL>
     * <LI>importedNoAction - do not allow update of parent key if it has been imported
     * <LI>importedKeyCascade - change imported key to agree with parent key update
     * <LI>importedKeySetNull - change imported key to <code>NULL</code> if its parent key has
     * been updated
     * <LI>importedKeySetDefault - change imported key to default values if its parent key has
     * been updated
     * <LI>importedKeyRestrict - same as importedKeyNoAction (for ODBC 2.x compatibility)
     * </UL>
     * <LI><B>DELETE_RULE</B> short {@code =>} What happens to the foreign key when parent key is
     * deleted.
     * <UL>
     * <LI>importedKeyNoAction - do not allow delete of parent key if it has been imported
     * <LI>importedKeyCascade - delete rows that import a deleted key
     * <LI>importedKeySetNull - change imported key to <code>NULL</code> if its primary key
     * has been deleted
     * <LI>importedKeyRestrict - same as importedKeyNoAction (for ODBC 2.x compatibility)
     * <LI>importedKeySetDefault - change imported key to default if its parent key has been
     * deleted
     * </UL>
     * <LI><B>FK_NAME</B> String {@code =>} foreign key name (may be <code>null</code>)
     * <LI><B>PK_NAME</B> String {@code =>} parent key name (may be <code>null</code>)
     * <LI><B>DEFERRABILITY</B> short {@code =>} can the evaluation of foreign key constraints be
     * deferred until commit
     * <UL>
     * <LI>importedKeyInitiallyDeferred - see SQL92 for definition
     * <LI>importedKeyInitiallyImmediate - see SQL92 for definition
     * <LI>importedKeyNotDeferrable - see SQL92 for definition
     * </UL>
     * </OL>
     *
     * @param parentCatalog  a catalog name; must match the catalog name as it is stored in the
     *                       database; "" retrieves those without a catalog; <code>null</code> means drop catalog name
     *                       from the selection criteria
     * @param parentSchema   a schema name; must match the schema name as it is stored in the database;
     *                       "" retrieves those without a schema; <code>null</code> means drop schema name from the
     *                       selection criteria
     * @param parentTable    the name of the table that exports the key; must match the table name as it
     *                       is stored in the database
     * @param foreignCatalog a catalog name; must match the catalog name as it is stored in the
     *                       database; "" retrieves those without a catalog; <code>null</code> means drop catalog name
     *                       from the selection criteria
     * @param foreignSchema  a schema name; must match the schema name as it is stored in the database;
     *                       "" retrieves those without a schema; <code>null</code> means drop schema name from the
     *                       selection criteria
     * @param foreignTable   the name of the table that imports the key; must match the table name as it
     *                       is stored in the database
     * @return <code>ResultSet</code> - each row is a foreign key column description
     * @throws SQLException if a database access error occurs
     * @see #getImportedKeys
     */
    public ResultSet getCrossReference(String parentCatalog, String parentSchema,
                                       String parentTable, String foreignCatalog,
                                       String foreignSchema, String foreignTable)
                                                                                 throws SQLException {
        if (this.connection.getProtocol().isOracleMode()) {
            return this.keysQuery(parentSchema, parentTable, foreignSchema, foreignTable,
                "ORDER BY fktable_schem, fktable_name, key_seq");
        }
        String sql = "SELECT KCU.REFERENCED_TABLE_SCHEMA PKTABLE_CAT, NULL PKTABLE_SCHEM,  KCU.REFERENCED_TABLE_NAME PKTABLE_NAME,"
                     + " KCU.REFERENCED_COLUMN_NAME PKCOLUMN_NAME, KCU.TABLE_SCHEMA FKTABLE_CAT, NULL FKTABLE_SCHEM, "
                     + " KCU.TABLE_NAME FKTABLE_NAME, KCU.COLUMN_NAME FKCOLUMN_NAME, KCU.POSITION_IN_UNIQUE_CONSTRAINT KEY_SEQ,"
                     + " CASE update_rule "
                     + "   WHEN 'RESTRICT' THEN 1"
                     + "   WHEN 'NO ACTION' THEN 3"
                     + "   WHEN 'CASCADE' THEN 0"
                     + "   WHEN 'SET NULL' THEN 2"
                     + "   WHEN 'SET DEFAULT' THEN 4"
                     + " END UPDATE_RULE,"
                     + " CASE DELETE_RULE"
                     + "  WHEN 'RESTRICT' THEN 1"
                     + "  WHEN 'NO ACTION' THEN 3"
                     + "  WHEN 'CASCADE' THEN 0"
                     + "  WHEN 'SET NULL' THEN 2"
                     + "  WHEN 'SET DEFAULT' THEN 4"
                     + " END DELETE_RULE,"
                     + " RC.CONSTRAINT_NAME FK_NAME,"
                     + " NULL PK_NAME,"
                     + importedKeyNotDeferrable
                     + " DEFERRABILITY"
                     + " FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE KCU"
                     + " INNER JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS RC"
                     + " ON KCU.CONSTRAINT_SCHEMA = RC.CONSTRAINT_SCHEMA"
                     + " AND KCU.CONSTRAINT_NAME = RC.CONSTRAINT_NAME"
                     + " WHERE "
                     + catalogCond("KCU.REFERENCED_TABLE_SCHEMA", parentCatalog)
                     + " AND "
                     + catalogCond("KCU.TABLE_SCHEMA", foreignCatalog)
                     + " AND "
                     + " KCU.REFERENCED_TABLE_NAME = "
                     + escapeQuote(parentTable)
                     + " AND "
                     + " KCU.TABLE_NAME = "
                     + escapeQuote(foreignTable)
                     + " ORDER BY FKTABLE_CAT, FKTABLE_SCHEM, FKTABLE_NAME, KEY_SEQ";
        ArrayList<String> tableNameList;
        try {
            tableNameList = getALlTableNames(parentCatalog);
            ResultSet allTableRS = getAllTablesResultSet(tableNameList, parentCatalog);
            ArrayList<String[]> list = new ArrayList<>();
            while(allTableRS.next()) {
                String tableType = allTableRS.getString("Type");
                if (tableType!= null && (tableType.toUpperCase(Locale.ROOT).equals("INNODB") || tableType.toUpperCase(Locale.ROOT).equals("SUPPORTS_FK"))) {
                    String tableComment  = allTableRS.getString("Comment").trim();
                    String fkTableName = allTableRS.getString("Name");
                    if(tableComment != null) {
                        StringTokenizer stringTokenizer = new StringTokenizer(tableComment,";",false);
                        if (stringTokenizer.hasMoreTokens()) {
                            String dummy = stringTokenizer.nextToken();
                            // Skip InnoDB comment
                        }
                        while(stringTokenizer.hasMoreTokens()) {
                            String keys = stringTokenizer.nextToken();
                            int fkParamOpenIndex = keys.indexOf("(");
                            if (fkParamOpenIndex == -1) {
                                throw  new SQLException("Error parsing foreign keys definition, couldn't find start of local columns list.");
                            }
                            String constraintName = keys.substring(1,fkParamOpenIndex);
                            keys = keys.substring(fkParamOpenIndex);
                            int fkParamCloseIndex = keys.indexOf(")");
                            if (fkParamCloseIndex == -1) {
                                throw  new SQLException("Error parsing foreign keys definition, couldn't find start of local columns list.");
                            }
                            String fkParamNames =  keys.substring(1,fkParamCloseIndex);
                            int refIndex = keys.indexOf("REFER");
                            if (refIndex == -1) {
                                throw new SQLException("Error parsing foreign keys definition, couldn't find start of referenced tables list");
                            }
                            int refParamOpenIndex = keys.indexOf("(",refIndex);
                            if (refParamOpenIndex == -1) {
                                throw new SQLException("Error parsing foreign keys definition, couldn't find start of referenced columns list");
                            }
                            String refCatalogAndTable = keys.substring(refIndex + "REFER".length() + 1,refParamOpenIndex);
                            int slashIndex = refCatalogAndTable.indexOf("/");
                            if(slashIndex == -1) {
                                throw  new SQLException("Error parsing foreign keys definition, couldn't find name of referenced catalog.");
                            }
                            String refCatalog = refCatalogAndTable.substring(0,slashIndex);
                            refCatalog = removeQuoted(refCatalog);
                            String refTableName = refCatalogAndTable.substring(slashIndex + 1);
                            refTableName = removeQuoted(refTableName);
                            if(fkTableName.compareTo(foreignTable) != 0)  {
                                continue;
                            }
                            int refParamCloseIndex = keys.indexOf(")",refParamOpenIndex);
                            if (refParamCloseIndex == -1) {
                                throw new SQLException("Error parsing foreign keys definition, couldn't find end of referenced columns list.");
                            }
                            String refParamNames = keys.substring(refParamOpenIndex + 1,refParamCloseIndex );
                            String[] refParamList = refParamNames.split(",");
                            String[] fkParamList = fkParamNames.split(",");
                            int fkCur = 0;
                            int refCur = 0;
                            int keySeq = 0;
                            while(fkCur < fkParamList.length) {
                                String lColumnName = removeQuoted(fkParamList[fkCur++]);
                                String rColumnName = null;
                                if (refCur < refParamList.length) {
                                    rColumnName = removeQuoted(refParamList[refCur++]);
                                }
                                if(refTableName.compareTo(parentTable) !=  0) {
                                    continue;
                                }
                                int updateRuleAction  = importedKeyNoAction;
                                int deleteRuleAction  = importedKeyNoAction;
                                int lastIndex = keys.lastIndexOf(")");
                                if (lastIndex != keys.length() - 1) {
                                    String options = keys.substring(lastIndex + 1);
                                    String optionsForUpdate = options.substring(options.indexOf("ON UPDATE"));
                                    if (optionsForUpdate.startsWith("ON UPDATE CASCADE")){
                                        updateRuleAction = importedKeyCascade;
                                    } else if (optionsForUpdate.startsWith("ON UPDATE SET NUL")) {
                                        updateRuleAction = importedKeySetNull;
                                    } else if (optionsForUpdate.startsWith("ON UPDATE RESTRICT")) {
                                        updateRuleAction = importedKeyRestrict;
                                    } else if (optionsForUpdate.startsWith("ON UPDATE NO ACTION")) {
                                        updateRuleAction = importedKeyNoAction;
                                    }
                                    String optionsForDelete = options.substring(options.indexOf("ON DELETE"));
                                    if (optionsForDelete.startsWith("ON DELETE CASCADE")) {
                                        deleteRuleAction = importedKeyCascade;
                                    } else if (optionsForDelete.startsWith("ON DELETE SET NUL")) {
                                        deleteRuleAction = importedKeySetNull;
                                    } else if (optionsForDelete.startsWith("ON DELETE RESTRICT")) {
                                        deleteRuleAction = importedKeyRestrict;
                                    } else if (optionsForDelete.startsWith("ON DELETE NO ACTIO")) {
                                        deleteRuleAction = importedKeyNoAction;
                                    }
                                }
                                String[] data = new String[14];
                                data[0] = parentCatalog;
                                data[1] = parentSchema;
                                data[2] = refTableName;
                                data[3] = rColumnName;
                                data[4] = foreignCatalog;
                                data[5] = foreignSchema;
                                data[6] = fkTableName;
                                data[7] = lColumnName;
                                data[8] = Integer.toString(keySeq++);
                                data[9] = Integer.toString(updateRuleAction);
                                data[10] = Integer.toString(deleteRuleAction);
                                data[11] = null;
                                data[12] = null;
                                data[13] = Integer.toString(importedKeyNotDeferrable);
                                list.add(data);
                            }
                        }
                    }
                }

            }
            String[][] val = new String[list.size()][];
            for (int j = 0; j < list.size(); j++) {
                val[j] = list.get(j);
            }
            return SelectResultSet.createResultSet(exportKeysColumnNames,exportKeysColumnTypes,val,this.protocol);
        } catch (SQLException e) {
            throw e;
        }
    }

    /**
     * Retrieves a description of all the data types supported by this database. They are ordered by
     * DATA_TYPE and then by how closely the data type maps to the corresponding JDBC SQL type.
     *
     * <p>If the database supports SQL distinct types, then getTypeInfo() will return a single row
     * with a TYPE_NAME of DISTINCT and a DATA_TYPE of Types.DISTINCT. If the database supports SQL
     * structured types, then getTypeInfo() will return a single row with a TYPE_NAME of STRUCT and a
     * DATA_TYPE of Types.STRUCT.
     *
     * <p>If SQL distinct or structured types are supported, then information on the individual types
     * may be obtained from the getUDTs() method.
     *
     * <p>Each type description has the following columns:
     *
     * <OL>
     * <LI><B>TYPE_NAME</B> String {@code =>} Type name
     * <LI><B>DATA_TYPE</B> int {@code =>} SQL data type from java.sql.Types
     * <LI><B>PRECISION</B> int {@code =>} maximum precision
     * <LI><B>LITERAL_PREFIX</B> String {@code =>} prefix used to quote a literal (may be <code>null
     * </code>)
     * <LI><B>LITERAL_SUFFIX</B> String {@code =>} suffix used to quote a literal (may be <code>null
     * </code>)
     * <LI><B>CREATE_PARAMS</B> String {@code =>} parameters used in creating the type (may be
     * <code>null</code>)
     * <LI><B>NULLABLE</B> short {@code =>} can you use NULL for this type.
     * <UL>
     * <LI>typeNoNulls - does not allow NULL values
     * <LI>typeNullable - allows NULL values
     * <LI>typeNullableUnknown - nullability unknown
     * </UL>
     * <LI><B>CASE_SENSITIVE</B> boolean{@code =>} is it case sensitive.
     * <LI><B>SEARCHABLE</B> short {@code =>} can you use "WHERE" based on this type:
     * <UL>
     * <LI>typePredNone - No support
     * <LI>typePredChar - Only supported with WHERE .. LIKE
     * <LI>typePredBasic - Supported except for WHERE .. LIKE
     * <LI>typeSearchable - Supported for all WHERE ..
     * </UL>
     * <LI><B>UNSIGNED_ATTRIBUTE</B> boolean {@code =>} is it unsigned.
     * <LI><B>FIXED_PREC_SCALE</B> boolean {@code =>} can it be a money value.
     * <LI><B>AUTO_INCREMENT</B> boolean {@code =>} can it be used for an auto-increment value.
     * <LI><B>LOCAL_TYPE_NAME</B> String {@code =>} localized version of type name (may be <code>
     * null</code>)
     * <LI><B>MINIMUM_SCALE</B> short {@code =>} minimum scale supported
     * <LI><B>MAXIMUM_SCALE</B> short {@code =>} maximum scale supported
     * <LI><B>SQL_DATA_TYPE</B> int {@code =>} unused
     * <LI><B>SQL_DATETIME_SUB</B> int {@code =>} unused
     * <LI><B>NUM_PREC_RADIX</B> int {@code =>} usually 2 or 10
     * </OL>
     *
     * <p>The PRECISION column represents the maximum column size that the server supports for the
     * given datatype. For numeric data, this is the maximum precision. For character data, this is
     * the length in characters. For datetime datatypes, this is the length in characters of the
     * String representation (assuming the maximum allowed precision of the fractional seconds
     * component). For binary data, this is the length in bytes. For the ROWID datatype, this is the
     * length in bytes. Null is returned for data types where the column size is not applicable.
     *
     * @return a <code>ResultSet</code> object in which each row is an SQL type description
     */
    public ResultSet getTypeInfo() {
        String[] columnNames = { "TYPE_NAME", "DATA_TYPE", "PRECISION", "LITERAL_PREFIX",
                "LITERAL_SUFFIX", "CREATE_PARAMS", "NULLABLE", "CASE_SENSITIVE", "SEARCHABLE",
                "UNSIGNED_ATTRIBUTE", "FIXED_PREC_SCALE", "AUTO_INCREMENT", "LOCAL_TYPE_NAME",
                "MINIMUM_SCALE", "MAXIMUM_SCALE", "SQL_DATA_TYPE", "SQL_DATETIME_SUB",
                "NUM_PREC_RADIX" };
        ColumnType[] columnTypes = { ColumnType.VARCHAR, ColumnType.INTEGER, ColumnType.INTEGER,
                ColumnType.VARCHAR, ColumnType.VARCHAR, ColumnType.VARCHAR, ColumnType.INTEGER,
                ColumnType.BIT, ColumnType.SMALLINT, ColumnType.BIT, ColumnType.BIT,
                ColumnType.BIT, ColumnType.VARCHAR, ColumnType.SMALLINT, ColumnType.SMALLINT,
                ColumnType.INTEGER, ColumnType.INTEGER, ColumnType.INTEGER };

        String[][] data = {
                { "BIT", "-7", "1", "", "", "", "1", "1", "3", "0", "0", "0", "BIT", "0", "0", "0",
                        "0", "10" },
                { "BOOL", "-7", "1", "", "", "", "1", "1", "3", "0", "0", "0", "BOOL", "0", "0",
                        "0", "0", "10" },
                { "TINYINT", "-6", "3", "", "", "[(M)] [UNSIGNED] [ZEROFILL]", "1", "0", "3", "1",
                        "0", "1", "TINYINT", "0", "0", "0", "0", "10" },
                { "TINYINT UNSIGNED", "-6", "3", "", "", "[(M)] [UNSIGNED] [ZEROFILL]", "1", "0",
                        "3", "1", "0", "1", "TINYINT UNSIGNED", "0", "0", "0", "0", "10" },
                { "BIGINT", "-5", "19", "", "", "[(M)] [UNSIGNED] [ZEROFILL]", "1", "0", "3", "1",
                        "0", "1", "BIGINT", "0", "0", "0", "0", "10" },
                { "BIGINT UNSIGNED", "-5", "20", "", "", "[(M)] [ZEROFILL]", "1", "0", "3", "1",
                        "0", "1", "BIGINT UNSIGNED", "0", "0", "0", "0", "10" },
                { "LONG VARBINARY", "-4", "16777215", "'", "'", "", "1", "1", "3", "0", "0", "0",
                        "LONG VARBINARY", "0", "0", "0", "0", "10" },
                { "MEDIUMBLOB", "-4", "16777215", "'", "'", "", "1", "1", "3", "0", "0", "0",
                        "MEDIUMBLOB", "0", "0", "0", "0", "10" },
                { "LONGBLOB", "-4", "2147483647", "'", "'", "", "1", "1", "3", "0", "0", "0",
                        "LONGBLOB", "0", "0", "0", "0", "10" },
                { "BLOB", "-4", "65535", "'", "'", "", "1", "1", "3", "0", "0", "0", "BLOB", "0",
                        "0", "0", "0", "10" },
                { "TINYBLOB", "-4", "255", "'", "'", "", "1", "1", "3", "0", "0", "0", "TINYBLOB",
                        "0", "0", "0", "0", "10" },
                { "VARBINARY", "-3", "255", "'", "'", "(M)", "1", "1", "3", "0", "0", "0",
                        "VARBINARY", "0", "0", "0", "0", "10" },
                { "BINARY", "-2", "255", "'", "'", "(M)", "1", "1", "3", "0", "0", "0", "BINARY",
                        "0", "0", "0", "0", "10" },
                { "LONG VARCHAR", "-1", "16777215", "'", "'", "", "1", "0", "3", "0", "0", "0",
                        "LONG VARCHAR", "0", "0", "0", "0", "10" },
                { "MEDIUMTEXT", "-1", "16777215", "'", "'", "", "1", "0", "3", "0", "0", "0",
                        "MEDIUMTEXT", "0", "0", "0", "0", "10" },
                { "LONGTEXT", "-1", "2147483647", "'", "'", "", "1", "0", "3", "0", "0", "0",
                        "LONGTEXT", "0", "0", "0", "0", "10" },
                { "TEXT", "-1", "65535", "'", "'", "", "1", "0", "3", "0", "0", "0", "TEXT", "0",
                        "0", "0", "0", "10" },
                { "TINYTEXT", "-1", "255", "'", "'", "", "1", "0", "3", "0", "0", "0", "TINYTEXT",
                        "0", "0", "0", "0", "10" },
                { "CHAR", "1", "255", "'", "'", "(M)", "1", "0", "3", "0", "0", "0", "CHAR", "0",
                        "0", "0", "0", "10" },
                { "NUMERIC", "2", "65", "", "", "[(M,D])] [ZEROFILL]", "1", "0", "3", "0", "0",
                        "1", "NUMERIC", "-308", "308", "0", "0", "10" },
                { "DECIMAL", "3", "65", "", "", "[(M,D])] [ZEROFILL]", "1", "0", "3", "0", "0",
                        "1", "DECIMAL", "-308", "308", "0", "0", "10" },
                { "INTEGER", "4", "10", "", "", "[(M)] [UNSIGNED] [ZEROFILL]", "1", "0", "3", "1",
                        "0", "1", "INTEGER", "0", "0", "0", "0", "10" },
                { "INTEGER UNSIGNED", "4", "10", "", "", "[(M)] [ZEROFILL]", "1", "0", "3", "1",
                        "0", "1", "INTEGER UNSIGNED", "0", "0", "0", "0", "10" },
                { "INT", "4", "10", "", "", "[(M)] [UNSIGNED] [ZEROFILL]", "1", "0", "3", "1", "0",
                        "1", "INT", "0", "0", "0", "0", "10" },
                { "INT UNSIGNED", "4", "10", "", "", "[(M)] [ZEROFILL]", "1", "0", "3", "1", "0",
                        "1", "INT UNSIGNED", "0", "0", "0", "0", "10" },
                { "MEDIUMINT", "4", "7", "", "", "[(M)] [UNSIGNED] [ZEROFILL]", "1", "0", "3", "1",
                        "0", "1", "MEDIUMINT", "0", "0", "0", "0", "10" },
                { "MEDIUMINT UNSIGNED", "4", "8", "", "", "[(M)] [ZEROFILL]", "1", "0", "3", "1",
                        "0", "1", "MEDIUMINT UNSIGNED", "0", "0", "0", "0", "10" },
                { "SMALLINT", "5", "5", "", "", "[(M)] [UNSIGNED] [ZEROFILL]", "1", "0", "3", "1",
                        "0", "1", "SMALLINT", "0", "0", "0", "0", "10" },
                { "SMALLINT UNSIGNED", "5", "5", "", "", "[(M)] [ZEROFILL]", "1", "0", "3", "1",
                        "0", "1", "SMALLINT UNSIGNED", "0", "0", "0", "0", "10" },
                { "FLOAT", "7", "10", "", "", "[(M|D)] [ZEROFILL]", "1", "0", "3", "0", "0", "1",
                        "FLOAT", "-38", "38", "0", "0", "10" },
                { "DOUBLE", "8", "17", "", "", "[(M|D)] [ZEROFILL]", "1", "0", "3", "0", "0", "1",
                        "DOUBLE", "-308", "308", "0", "0", "10" },
                { "DOUBLE PRECISION", "8", "17", "", "", "[(M,D)] [ZEROFILL]", "1", "0", "3", "0",
                        "0", "1", "DOUBLE PRECISION", "-308", "308", "0", "0", "10" },
                { "REAL", "8", "17", "", "", "[(M,D)] [ZEROFILL]", "1", "0", "3", "0", "0", "1",
                        "REAL", "-308", "308", "0", "0", "10" },
                { "VARCHAR", "12", "255", "'", "'", "(M)", "1", "0", "3", "0", "0", "0", "VARCHAR",
                        "0", "0", "0", "0", "10" },
                { "ENUM", "12", "65535", "'", "'", "", "1", "0", "3", "0", "0", "0", "ENUM", "0",
                        "0", "0", "0", "10" },
                { "SET", "12", "64", "'", "'", "", "1", "0", "3", "0", "0", "0", "SET", "0", "0",
                        "0", "0", "10" },
                { "DATE", "91", "10", "'", "'", "", "1", "0", "3", "0", "0", "0", "DATE", "0", "0",
                        "0", "0", "10" },
                { "TIME", "92", "18", "'", "'", "[(M)]", "1", "0", "3", "0", "0", "0", "TIME", "0",
                        "0", "0", "0", "10" },
                { "DATETIME", "93", "27", "'", "'", "[(M)]", "1", "0", "3", "0", "0", "0",
                        "DATETIME", "0", "0", "0", "0", "10" },
                { "TIMESTAMP", "93", "27", "'", "'", "[(M)]", "1", "0", "3", "0", "0", "0",
                        "TIMESTAMP", "0", "0", "0", "0", "10" } };

        return SelectResultSet.createResultSet(columnNames, columnTypes, data,
            connection.getProtocol());
    }

    /**
     * Retrieves a description of the given table's indices and statistics. They are ordered by
     * NON_UNIQUE, TYPE, INDEX_NAME, and ORDINAL_POSITION.
     *
     * <p>Each index column description has the following columns:
     *
     * <ol>
     *   <li><B>TABLE_CAT</B> String {@code =>} table catalog (may be <code>null</code>)
     *   <li><B>TABLE_SCHEM</B> String {@code =>} table schema (may be <code>null</code>)
     *   <li><B>TABLE_NAME</B> String {@code =>} table name
     *   <li><B>NON_UNIQUE</B> boolean {@code =>} Can index values be non-unique. false when TYPE is
     *       tableIndexStatistic
     *   <li><B>INDEX_QUALIFIER</B> String {@code =>} index catalog (may be <code>null</code>); <code>
     *       null</code> when TYPE is tableIndexStatistic
     *   <li><B>INDEX_NAME</B> String {@code =>} index name; <code>null</code> when TYPE is
     *       tableIndexStatistic
     *   <li><B>TYPE</B> short {@code =>} index type:
     *       <ul>
     *         <li>tableIndexStatistic - this identifies table statistics that are returned in
     *             conjuction with a table's index descriptions
     *         <li>tableIndexClustered - this is a clustered index
     *         <li>tableIndexHashed - this is a hashed index
     *         <li>tableIndexOther - this is some other style of index
     *       </ul>
     *   <li><B>ORDINAL_POSITION</B> short {@code =>} column sequence number within index; zero when
     *       TYPE is tableIndexStatistic
     *   <li><B>COLUMN_NAME</B> String {@code =>} column name; <code>null</code> when TYPE is
     *       tableIndexStatistic
     *   <li><B>ASC_OR_DESC</B> String {@code =>} column sort sequence, "A" {@code =>} ascending, "D"
     *       {@code =>} descending, may be <code>null</code> if sort sequence is not supported; <code>
     *       null</code> when TYPE is tableIndexStatistic
     *   <li><B>CARDINALITY</B> long {@code =>} When TYPE is tableIndexStatistic, then this is the
     *       number of rows in the table; otherwise, it is the number of unique values in the index.
     *   <li><B>PAGES</B> long {@code =>} When TYPE is tableIndexStatisic then this is the number of
     *       pages used for the table, otherwise it is the number of pages used for the current index.
     *   <li><B>FILTER_CONDITION</B> String {@code =>} Filter condition, if any. (may be <code>null
     *       </code>)
     * </ol>
     *
     * @param catalog     a catalog name; must match the catalog name as it is stored in this database; ""
     *                    retrieves those without a catalog; <code>null</code> means that the catalog name should not
     *                    be used to narrow the search
     * @param schema      a schema name; must match the schema name as it is stored in this database; ""
     *                    retrieves those without a schema; <code>null</code> means that the schema name should not
     *                    be used to narrow the search
     * @param table       a table name; must match the table name as it is stored in this database
     * @param unique      when true, return only indices for unique values; when false, return indices
     *                    regardless of whether unique or not
     * @param approximate when true, result is allowed to reflect approximate or out of data values;
     *                    when false, results are requested to be accurate
     * @return <code>ResultSet</code> - each row is an index column description
     * @throws SQLException if a database access error occurs
     */
    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique,
                                  boolean approximate) throws SQLException {

        if (table == null || table.length() == 0) {
            throw new SQLException();
        }
        if (this.connection.getProtocol().isOracleMode()) {
            return super.getIndexInfo(catalog, schema, table, unique, approximate);
        }

        String sql = "SELECT TABLE_SCHEMA TABLE_CAT, NULL TABLE_SCHEM, TABLE_NAME, NON_UNIQUE, "
                     + " TABLE_SCHEMA INDEX_QUALIFIER, INDEX_NAME, 3 TYPE,"
                     + " SEQ_IN_INDEX ORDINAL_POSITION, COLUMN_NAME, COLLATION ASC_OR_DESC,"
                     + " CARDINALITY, NULL PAGES, NULL FILTER_CONDITION"
                     + " FROM INFORMATION_SCHEMA.STATISTICS" + " WHERE TABLE_NAME = "
                     + escapeQuote(table) + " AND " + catalogCond("TABLE_SCHEMA", catalog)
                     + ((unique) ? " AND NON_UNIQUE = 0" : "")
                     + " ORDER BY NON_UNIQUE, TYPE, INDEX_NAME, ORDINAL_POSITION";

        return executeQuery(sql);
    }

    /**
     * Retrieves whether this database supports the given result set type. ResultSet.TYPE_FORWARD_ONLY
     * and ResultSet.TYPE_SCROLL_INSENSITIVE are supported.
     *
     * @param type one of the following <code>ResultSet</code> constants:
     *             <ul>
     *               <li><code>ResultSet.TYPE_FORWARD_ONLY</code>
     *               <li><code>ResultSet.TYPE_SCROLL_INSENSITIVE</code>
     *               <li><code>ResultSet.TYPE_SCROLL_SENSITIVE</code>
     *             </ul>
     * @return true if supported
     */
    public boolean supportsResultSetType(int type) {
        return (type == ResultSet.TYPE_SCROLL_INSENSITIVE || type == ResultSet.TYPE_FORWARD_ONLY);
    }

    /**
     * Retrieves whether this database supports the given concurrency type in combination with the
     * given result set type. All are supported, but combination that use
     * ResultSet.TYPE_SCROLL_INSENSITIVE.
     *
     * @param type        one of the following <code>ResultSet</code> constants:
     *                    <ul>
     *                      <li><code>ResultSet.TYPE_FORWARD_ONLY</code>
     *                      <li><code>ResultSet.TYPE_SCROLL_INSENSITIVE</code>
     *                      <li><code>ResultSet.TYPE_SCROLL_SENSITIVE</code>
     *                    </ul>
     * @param concurrency one of the following <code>ResultSet</code> constants:
     *                    <ul>
     *                      <li><code>ResultSet.CONCUR_READ_ONLY</code>
     *                      <li><code>ResultSet.CONCUR_UPDATABLE</code>
     *                    </ul>
     * @return true if supported
     */
    public boolean supportsResultSetConcurrency(int type, int concurrency) {
        // Support all concurrency (ResultSet.CONCUR_READ_ONLY and ResultSet.CONCUR_UPDATABLE)
        // so just return scroll type
        return type == ResultSet.TYPE_SCROLL_INSENSITIVE || type == ResultSet.TYPE_FORWARD_ONLY;
    }

    public boolean ownUpdatesAreVisible(int type) {
        return supportsResultSetType(type);
    }

    public boolean ownDeletesAreVisible(int type) {
        return supportsResultSetType(type);
    }

    public boolean ownInsertsAreVisible(int type) {
        return supportsResultSetType(type);
    }

    public boolean othersUpdatesAreVisible(int type) {
        return false;
    }

    public boolean othersDeletesAreVisible(int type) {
        return false;
    }

    public boolean othersInsertsAreVisible(int type) {
        return false;
    }

    public boolean updatesAreDetected(int type) {
        return false;
    }

    public boolean deletesAreDetected(int type) {
        return false;
    }

    public boolean insertsAreDetected(int type) {
        return false;
    }

    public boolean supportsBatchUpdates() {
        return true;
    }

    /**
     * Retrieves a description of the user-defined types (UDTs) defined in a particular schema.
     * Schema-specific UDTs may have type <code>JAVA_OBJECT</code>, <code>STRUCT</code>, or <code>
     * DISTINCT</code>.
     *
     * <p>Only types matching the catalog, schema, type name and type criteria are returned. They are
     * ordered by <code>DATA_TYPE</code>, <code>TYPE_CAT</code>, <code>TYPE_SCHEM</code> and <code>
     * TYPE_NAME</code>. The type name parameter may be a fully-qualified name. In this case, the
     * catalog and schemaPattern parameters are ignored.
     *
     * <p>Each type description has the following columns:
     *
     * <ol>
     *   <li><B>TYPE_CAT</B> String {@code =>} the type's catalog (may be <code>null</code>)
     *   <li><B>TYPE_SCHEM</B> String {@code =>} type's schema (may be <code>null</code>)
     *   <li><B>TYPE_NAME</B> String {@code =>} type name
     *   <li><B>CLASS_NAME</B> String {@code =>} Java class name
     *   <li><B>DATA_TYPE</B> int {@code =>} type value defined in java.sql.Types. One of JAVA_OBJECT,
     *       STRUCT, or DISTINCT
     *   <li><B>REMARKS</B> String {@code =>} explanatory comment on the type
     *   <li><B>BASE_TYPE</B> short {@code =>} type code of the source type of a DISTINCT type or the
     *       type that implements the user-generated reference type of the SELF_REFERENCING_COLUMN of
     *       a structured type as defined in java.sql.Types (<code>null</code> if DATA_TYPE is not
     *       DISTINCT or not STRUCT with REFERENCE_GENERATION = USER_DEFINED)
     * </ol>
     *
     * <p><B>Note:</B> If the driver does not support UDTs, an empty result set is returned.
     *
     * @param catalog         a catalog name; must match the catalog name as it is stored in the database; ""
     *                        retrieves those without a catalog; <code>null</code> means that the catalog name should not
     *                        be used to narrow the search
     * @param schemaPattern   a schema pattern name; must match the schema name as it is stored in the
     *                        database; "" retrieves those without a schema; <code>null</code> means that the schema name
     *                        should not be used to narrow the search
     * @param typeNamePattern a type name pattern; must match the type name as it is stored in the
     *                        database; may be a fully qualified name
     * @param types           a list of user-defined types (JAVA_OBJECT, STRUCT, or DISTINCT) to include; <code>
     *                        null</code> returns all types
     * @return <code>ResultSet</code> object in which each row describes a UDT
     * @throws SQLException if a database access error occurs
     * @see #getSearchStringEscape
     * @since 1.2
     */
    @Override
    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern,
                             int[] types) throws SQLException {
        String sql = "SELECT ' ' TYPE_CAT, NULL TYPE_SCHEM, ' ' TYPE_NAME, ' ' CLASS_NAME, 0 DATA_TYPE, ' ' REMARKS, 0 BASE_TYPE"
                     + " FROM DUAL WHERE 1=0";

        return executeQuery(sql);
    }

    public Connection getConnection() {
        return connection;
    }

    public boolean supportsSavepoints() {
        return true;
    }

    public boolean supportsNamedParameters() {
        return connection.getProtocol().isOracleMode();
    }

    //todo  there return true in 1.X_OracleModel , return false in oracle-jdbc
    public boolean supportsMultipleOpenResults() {
        return false;
    }

    public boolean supportsGetGeneratedKeys() {
        return true;
    }

    /**
     * Retrieves a description of the user-defined type (UDT) hierarchies defined in a particular
     * schema in this database. Only the immediate super type/ sub type relationship is modeled. Only
     * supertype information for UDTs matching the catalog, schema, and type name is returned. The
     * type name parameter may be a fully-qualified name. When the UDT name supplied is a
     * fully-qualified name, the catalog and schemaPattern parameters are ignored. If a UDT does not
     * have a direct super type, it is not listed here. A row of the <code>ResultSet</code> object
     * returned by this method describes the designated UDT and a direct supertype. A row has the
     * following columns:
     *
     * <OL>
     * <li><B>TYPE_CAT</B> String {@code =>} the UDT's catalog (may be <code>null</code>)
     * <li><B>TYPE_SCHEM</B> String {@code =>} UDT's schema (may be <code>null</code>)
     * <li><B>TYPE_NAME</B> String {@code =>} type name of the UDT
     * <li><B>SUPERTYPE_CAT</B> String {@code =>} the direct super type's catalog (may be <code>null
     * </code>)
     * <li><B>SUPERTYPE_SCHEM</B> String {@code =>} the direct super type's schema (may be <code>
     * null</code>)
     * <li><B>SUPERTYPE_NAME</B> String {@code =>} the direct super type's name
     * </OL>
     *
     * <p><B>Note:</B> If the driver does not support type hierarchies, an empty result set is
     * returned.
     *
     * @param catalog         a catalog name; "" retrieves those without a catalog; <code>null</code> means
     *                        drop catalog name from the selection criteria
     * @param schemaPattern   a schema name pattern; "" retrieves those without a schema
     * @param typeNamePattern a UDT name pattern; may be a fully-qualified name
     * @return a <code>ResultSet</code> object in which a row gives information about the designated
     * UDT
     * @throws SQLException if a database access error occurs
     * @see #getSearchStringEscape
     * @since 1.4
     */
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern)
                                                                                                throws SQLException {
        String sql = "SELECT  ' ' TYPE_CAT, NULL TYPE_SCHEM, ' ' TYPE_NAME, ' ' SUPERTYPE_CAT, ' ' SUPERTYPE_SCHEM, ' '  SUPERTYPE_NAME"
                     + " FROM DUAL WHERE 1=0";

        return executeQuery(sql);
    }

    /**
     * Retrieves a description of the table hierarchies defined in a particular schema in this
     * database.
     *
     * <p>Only supertable information for tables matching the catalog, schema and table name are
     * returned. The table name parameter may be a fully-qualified name, in which case, the catalog
     * and schemaPattern parameters are ignored. If a table does not have a super table, it is not
     * listed here. Supertables have to be defined in the same catalog and schema as the sub tables.
     * Therefore, the type description does not need to include this information for the supertable.
     *
     * <p>Each type description has the following columns:
     *
     * <OL>
     * <li><B>TABLE_CAT</B> String {@code =>} the type's catalog (may be <code>null</code>)
     * <li><B>TABLE_SCHEM</B> String {@code =>} type's schema (may be <code>null</code>)
     * <li><B>TABLE_NAME</B> String {@code =>} type name
     * <li><B>SUPERTABLE_NAME</B> String {@code =>} the direct super type's name
     * </OL>
     *
     * <p><B>Note:</B> If the driver does not support type hierarchies, an empty result set is
     * returned.
     *
     * @param catalog          a catalog name; "" retrieves those without a catalog; <code>null</code> means
     *                         drop catalog name from the selection criteria
     * @param schemaPattern    a schema name pattern; "" retrieves those without a schema
     * @param tableNamePattern a table name pattern; may be a fully-qualified name
     * @return a <code>ResultSet</code> object in which each row is a type description
     * @throws SQLException if a database access error occurs
     * @see #getSearchStringEscape
     * @since 1.4
     */
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern)
                                                                                                  throws SQLException {
        String sql = "SELECT  ' ' TABLE_CAT, ' ' TABLE_SCHEM, ' ' TABLE_NAME, ' ' SUPERTABLE_NAME FROM DUAL WHERE 1=0";
        return executeQuery(sql);
    }

    /**
     * Retrieves a description of the given attribute of the given type for a user-defined type (UDT)
     * that is available in the given schema and catalog. Descriptions are returned only for
     * attributes of UDTs matching the catalog, schema, type, and attribute name criteria. They are
     * ordered by <code>TYPE_CAT</code>, <code>TYPE_SCHEM</code>, <code>TYPE_NAME</code> and <code>
     * ORDINAL_POSITION</code>. This description does not contain inherited attributes. The <code>
     * ResultSet</code> object that is returned has the following columns:
     *
     * <OL>
     * <li><B>TYPE_CAT</B> String {@code =>} type catalog (may be <code>null</code>)
     * <li><B>TYPE_SCHEM</B> String {@code =>} type schema (may be <code>null</code>)
     * <li><B>TYPE_NAME</B> String {@code =>} type name
     * <li><B>ATTR_NAME</B> String {@code =>} attribute name
     * <li><B>DATA_TYPE</B> int {@code =>} attribute type SQL type from java.sql.Types
     * <li><B>ATTR_TYPE_NAME</B> String {@code =>} Data source dependent type name. For a UDT, the
     * type name is fully qualified. For a REF, the type name is fully qualified and represents
     * the target type of the reference type.
     * <li><B>ATTR_SIZE</B> int {@code =>} column size. For char or date types this is the maximum
     * number of characters; for numeric or decimal types this is precision.
     * <li><B>DECIMAL_DIGITS</B> int {@code =>} the number of fractional digits. Null is returned
     * for data types where DECIMAL_DIGITS is not applicable.
     * <li><B>NUM_PREC_RADIX</B> int {@code =>} Radix (typically either 10 or 2)
     * <li><B>NULLABLE</B> int {@code =>} whether NULL is allowed
     * <UL>
     * <li>attributeNoNulls - might not allow NULL values
     * <li>attributeNullable - definitely allows NULL values
     * <li>attributeNullableUnknown - nullability unknown
     * </UL>
     * <li><B>REMARKS</B> String {@code =>} comment describing column (may be <code>null</code>)
     * <li><B>ATTR_DEF</B> String {@code =>} default value (may be<code>null</code>)
     * <li><B>SQL_DATA_TYPE</B> int {@code =>} unused
     * <li><B>SQL_DATETIME_SUB</B> int {@code =>} unused
     * <li><B>CHAR_OCTET_LENGTH</B> int {@code =>} for char types the maximum number of bytes in the
     * column
     * <li><B>ORDINAL_POSITION</B> int {@code =>} index of the attribute in the UDT (starting at 1)
     * <li><B>IS_NULLABLE</B> String {@code =>} ISO rules are used to determine the nullability for
     * a attribute.
     * <UL>
     * <li>YES --- if the attribute can include NULLs
     * <li>NO --- if the attribute cannot include NULLs
     * <li>empty string --- if the nullability for the attribute is unknown
     * </UL>
     * <li><B>SCOPE_CATALOG</B> String {@code =>} catalog of table that is the scope of a reference
     * attribute (<code>null</code> if DATA_TYPE isn't REF)
     * <li><B>SCOPE_SCHEMA</B> String {@code =>} schema of table that is the scope of a reference
     * attribute (<code>null</code> if DATA_TYPE isn't REF)
     * <li><B>SCOPE_TABLE</B> String {@code =>} table name that is the scope of a reference
     * attribute (<code>null</code> if the DATA_TYPE isn't REF)
     * <li><B>SOURCE_DATA_TYPE</B> short {@code =>} source type of a distinct type or user-generated
     * Ref type,SQL type from java.sql.Types (<code>null</code> if DATA_TYPE isn't DISTINCT or
     * user-generated REF)
     * </OL>
     *
     * @param catalog              a catalog name; must match the catalog name as it is stored in the database; ""
     *                             retrieves those without a catalog; <code>null</code> means that the catalog name should not
     *                             be used to narrow the search
     * @param schemaPattern        a schema name pattern; must match the schema name as it is stored in the
     *                             database; "" retrieves those without a schema; <code>null</code> means that the schema name
     *                             should not be used to narrow the search
     * @param typeNamePattern      a type name pattern; must match the type name as it is stored in the
     *                             database
     * @param attributeNamePattern an attribute name pattern; must match the attribute name as it is
     *                             declared in the database
     * @return a <code>ResultSet</code> object in which each row is an attribute description
     * @throws SQLException if a database access error occurs
     * @see #getSearchStringEscape
     * @since 1.4
     */
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern,
                                   String attributeNamePattern) throws SQLException {

        if (this.connection.getProtocol().isOracleMode()) {
            throw new SQLFeatureNotSupportedException("Oracle model Unsupported features");
        }

        String sql = "SELECT ' ' TYPE_CAT, ' ' TYPE_SCHEM, ' ' TYPE_NAME, ' ' ATTR_NAME, 0 DATA_TYPE,"
                     + " ' ' ATTR_TYPE_NAME, 0 ATTR_SIZE, 0 DECIMAL_DIGITS, 0 NUM_PREC_RADIX, 0 NULLABLE,"
                     + " ' ' REMARKS, ' ' ATTR_DEF,  0 SQL_DATA_TYPE, 0 SQL_DATETIME_SUB, 0 CHAR_OCTET_LENGTH,"
                     + " 0 ORDINAL_POSITION, ' ' IS_NULLABLE, ' ' SCOPE_CATALOG, ' ' SCOPE_SCHEMA, ' ' SCOPE_TABLE,"
                     + " 0 SOURCE_DATA_TYPE" + " FROM DUAL " + " WHERE 1=0";

        return executeQuery(sql);
    }

    public boolean supportsResultSetHoldability(int holdability) {
        return holdability == ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    public int getResultSetHoldability() {
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public int getSQLStateType() {
        return sqlStateSQL;
    }

    public boolean locatorsUpdateCopy() {
        if (this.connection.getProtocol().isOracleMode()) {
            return super.locatorsUpdateCopy();
        }
        return false;
    }

    public boolean supportsStatementPooling() {
        return false;
    }

    public RowIdLifetime getRowIdLifetime() {
        return RowIdLifetime.ROWID_UNSUPPORTED;
    }

    public boolean supportsStoredFunctionsUsingCallSyntax() {
        return true;
    }

    public boolean autoCommitFailureClosesAllResultSets() {
        return false;
    }

    /**
     * Retrieves a list of the client info properties that the driver supports. The result set
     * contains the following columns
     *
     * <ol>
     *   <li>NAME String : The name of the client info property
     *   <li>MAX_LEN int : The maximum length of the value for the property
     *   <li>DEFAULT_VALUE String : The default value of the property
     *   <li>DESCRIPTION String : A description of the property. This will typically contain
     *       information as to where this property is stored in the database.
     * </ol>
     *
     * <p>The ResultSet is sorted by the NAME column
     *
     * @return A ResultSet object; each row is a supported client info property
     */
    public ResultSet getClientInfoProperties() {
        ColumnDefinition[] columns = new ColumnDefinition[4];
        boolean isOracleMode = this.protocol.isOracleMode();
        columns[0] = ColumnDefinition.create("NAME", ColumnType.STRING, isOracleMode,this.protocol.getOptions().characterEncoding);
        columns[1] = ColumnDefinition.create("MAX_LEN", ColumnType.INTEGER, isOracleMode,this.protocol.getOptions().characterEncoding);
        columns[2] = ColumnDefinition.create("DEFAULT_VALUE", ColumnType.STRING, isOracleMode,this.protocol.getOptions().characterEncoding);
        columns[3] = ColumnDefinition.create("DESCRIPTION", ColumnType.STRING, isOracleMode,this.protocol.getOptions().characterEncoding);

        byte[] sixteenMb =
                new byte[]{
                        (byte) 49, (byte) 54, (byte) 55, (byte) 55, (byte) 55, (byte) 50, (byte) 49, (byte) 53
                };
        byte[] empty = new byte[0];

        ColumnType[] types =
                new ColumnType[]{
                        ColumnType.STRING, ColumnType.INTEGER, ColumnType.STRING, ColumnType.STRING
                };
        List<byte[]> rows = new ArrayList<>(3);

        rows.add(
                StandardPacketInputStream.create(
                        new byte[][]{
                                "ApplicationName".getBytes(),
                                sixteenMb,
                                empty,
                                "The name of the application currently utilizing the connection".getBytes()
                        },
                        types));

        rows.add(
                StandardPacketInputStream.create(
                        new byte[][]{
                                "ClientUser".getBytes(),
                                sixteenMb,
                                empty,
                                ("The name of the user that the application using the connection is performing work for. "
                                        + "This may not be the same as the user name that was used in establishing the connection.")
                                        .getBytes()
                        },
                        types));

        rows.add(
                StandardPacketInputStream.create(
                        new byte[][]{
                                "ClientHostname".getBytes(),
                                sixteenMb,
                                empty,
                                "The hostname of the computer the application using the connection is running on"
                                        .getBytes()
                        },
                        types));

        return new SelectResultSet(
                columns, rows, connection.getProtocol(), ResultSet.TYPE_SCROLL_INSENSITIVE);
    }

    /**
     * Retrieves a description of the system and user functions available in the given catalog. Only
     * system and user function descriptions matching the schema and function name criteria are
     * returned. They are ordered by <code>FUNCTION_CAT</code>, <code>FUNCTION_SCHEM</code>, <code>
     * FUNCTION_NAME</code> and <code>SPECIFIC_ NAME</code>.
     *
     * <p>Each function description has the the following columns:
     *
     * <OL>
     * <li><B>FUNCTION_CAT</B> String {@code =>} function catalog (may be <code>null</code>)
     * <li><B>FUNCTION_SCHEM</B> String {@code =>} function schema (may be <code>null</code>)
     * <li><B>FUNCTION_NAME</B> String {@code =>} function name. This is the name used to invoke the
     * function
     * <li><B>REMARKS</B> String {@code =>} explanatory comment on the function
     * <li><B>FUNCTION_TYPE</B> short {@code =>} kind of function:
     * <UL>
     * <li>functionResultUnknown - Cannot determine if a return value or table will be
     * returned
     * <li>functionNoTable- Does not return a table
     * <li>functionReturnsTable - Returns a table
     * </UL>
     * <li><B>SPECIFIC_NAME</B> String {@code =>} the name which uniquely identifies this function
     * within its schema. This is a user specified, or DBMS generated, name that may be
     * different then the <code>FUNCTION_NAME</code> for example with overload functions
     * </OL>
     *
     * <p>A user may not have permission to execute any of the functions that are returned by <code>
     * getFunctions</code>
     *
     * @param catalog             a catalog name; must match the catalog name as it is stored in the database; ""
     *                            retrieves those without a catalog; <code>null</code> means that the catalog name should not
     *                            be used to narrow the search
     * @param schemaPattern       a schema name pattern; must match the schema name as it is stored in the
     *                            database; "" retrieves those without a schema; <code>null</code> means that the schema name
     *                            should not be used to narrow the search
     * @param functionNamePattern a function name pattern; must match the function name as it is
     *                            stored in the database
     * @return <code>ResultSet</code> - each row is a function description
     * @throws SQLException if a database access error occurs
     * @see #getSearchStringEscape
     * @since 1.6
     */
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern)
                                                                                                   throws SQLException {
        if (connection.getProtocol().isOracleMode()) {
            return super.getFunctions(catalog, schemaPattern, functionNamePattern);
        }
        String sql = "SELECT ROUTINE_SCHEMA FUNCTION_CAT,NULL FUNCTION_SCHEM, ROUTINE_NAME FUNCTION_NAME,"
                     + " ROUTINE_COMMENT REMARKS,"
                     + functionNoTable
                     + " FUNCTION_TYPE, SPECIFIC_NAME "
                     + " FROM INFORMATION_SCHEMA.ROUTINES "
                     + " WHERE "
                     + catalogCond("ROUTINE_SCHEMA", catalog)
                     + patternCond("ROUTINE_NAME", functionNamePattern)
                     + " AND ROUTINE_TYPE='FUNCTION'";

        return executeQuery(sql);
    }

    public <T> T unwrap(final Class<T> iface) {
        return null;
    }

    public boolean isWrapperFor(final Class<?> iface) {
        return false;
    }

    @Override
    public long getMaxLogicalLobSize() {
        return 4294967295L;
    }

    @Override
    public boolean supportsRefCursors() {
        return false;
    }
}
