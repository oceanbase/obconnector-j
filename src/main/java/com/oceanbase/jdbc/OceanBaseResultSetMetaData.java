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
 *
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

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

import com.oceanbase.jdbc.internal.ColumnType;
import com.oceanbase.jdbc.internal.ObOracleDefs;
import com.oceanbase.jdbc.internal.com.read.resultset.ColumnDefinition;
import com.oceanbase.jdbc.internal.util.constant.ColumnFlags;
import com.oceanbase.jdbc.internal.util.exceptions.ExceptionFactory;
import com.oceanbase.jdbc.util.Options;

public class OceanBaseResultSetMetaData implements ResultSetMetaData {

    private final ColumnDefinition[] fieldPackets;
    private final Options            options;
    private final boolean            forceAlias;
    private final boolean            isOracleMode;
    private final int                columnIndexOffset;

    /**
     * Constructor.
     *
     * @param fieldPackets column informations
     * @param options connection options
     * @param forceAlias force table and column name alias as original data
     */
    public OceanBaseResultSetMetaData(final ColumnDefinition[] fieldPackets, final Options options,
                                      final boolean forceAlias) {
        this.fieldPackets = fieldPackets;
        this.options = options;
        this.forceAlias = forceAlias;
        this.isOracleMode = false;
        this.columnIndexOffset = 0;
    }

    public OceanBaseResultSetMetaData(final ColumnDefinition[] fieldPackets, final Options options,
                                      final boolean forceAlias, boolean isOracleMode,
                                      final int columnIndexOffset) {
        this.fieldPackets = fieldPackets;
        this.options = options;
        this.forceAlias = forceAlias;
        this.isOracleMode = isOracleMode;
        this.columnIndexOffset = columnIndexOffset;
    }

    /**
     * Returns the number of columns in this <code>ResultSet</code> object.
     *
     * @return the number of columns
     */
    public int getColumnCount() {
        return fieldPackets.length - columnIndexOffset;
    }

    /**
     * Indicates whether the designated column is automatically numbered.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @throws SQLException if a database access error occurs
     */
    public boolean isAutoIncrement(final int column) throws SQLException {
        return (getColumnInformation(column).getFlags() & ColumnFlags.AUTO_INCREMENT) != 0;
    }

    /**
     * Indicates whether a column's case matters.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @throws SQLException if a database access error occurs
     */
    public boolean isCaseSensitive(final int column) throws SQLException {
        //        return (getColumnInformation(column).getFlags() & ColumnFlags.BINARY_COLLATION) != 0;
        ColumnDefinition columnInformation = getColumnInformation(column);

        if (isOracleMode) {
            switch (columnInformation.getColumnType().getSqlType()) {
                case Types.CHAR:
                case Types.VARCHAR:
                case Types.LONGVARCHAR:
                case Types.NCHAR:
                case Types.NVARCHAR:
                    return true;
                default:
                    return false;
            }
        } else {
            switch (columnInformation.getColumnType().getSqlType()) {
                case Types.BIT:
                case Types.TINYINT:
                case Types.SMALLINT:
                case Types.INTEGER:
                case Types.BIGINT:
                case Types.FLOAT:
                case Types.REAL:
                case Types.DOUBLE:
                case Types.DATE:
                case Types.TIME:
                case Types.TIMESTAMP:
                    return false;

                case Types.CHAR:
                case Types.VARCHAR:
                case Types.LONGVARCHAR:
                case Types.CLOB:
                    if (columnInformation.isBinary()) {
                        return true;
                    }
                    return true;
                    // fixme collation
                    //                    String collationName = columnInformation.();
                    //
                    //                    return ((collationName != null) && !collationName.endsWith("_ci"));
                default:
                    return true;
            }
        }
    }

    /**
     * Indicates whether the designated column can be used in a where clause.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return <code>true</code> if so; <code>false</code> otherwise
     */
    public boolean isSearchable(final int column) {
        return true;
    }

    /**
     * Indicates whether the designated column is a cash value.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return <code>true</code> if so; <code>false</code> otherwise
     */
    public boolean isCurrency(final int column) {
        return false;
    }

    /**
     * Indicates the nullability of values in the designated column.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return the nullability status of the given column; one of <code>columnNoNulls</code>, <code>
     *     columnNullable</code> or <code>columnNullableUnknown</code>
     * @throws SQLException if a database access error occurs
     */
    public int isNullable(final int column) throws SQLException {
        if ((getColumnInformation(column).getFlags() & ColumnFlags.NOT_NULL) == 0) {
            return ResultSetMetaData.columnNullable;
        } else {
            return ResultSetMetaData.columnNoNulls;
        }
    }

    /**
     * Indicates whether values in the designated column are signed numbers.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @throws SQLException if a database access error occurs
     */
    public boolean isSigned(int column) throws SQLException {
        return getColumnInformation(column).isSigned();
    }

    /**
     * Indicates the designated column's normal maximum width in characters.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return the normal maximum number of characters allowed as the width of the designated column
     * @throws SQLException if a database access error occurs
     */
    public int getColumnDisplaySize(final int column) throws SQLException {
        return getColumnInformation(column).getDisplaySize();
    }

    /**
     * Gets the designated column's suggested title for use in printouts and displays. The suggested
     * title is usually specified by the SQL <code>AS</code> clause. If a SQL <code>AS</code> is not
     * specified, the value returned from <code>getColumnLabel</code> will be the same as the value
     * returned by the <code>getColumnName</code> method.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return the suggested column title
     * @throws SQLException if a database access error occurs
     */
    public String getColumnLabel(final int column) throws SQLException {
        return getColumnInformation(column).getName();
    }

    /**
     * Get the designated column's name.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return column name
     * @throws SQLException if a database access error occurs
     */
    public String getColumnName(final int column) throws SQLException {
        if (isOracleMode) {
            return getColumnLabel(column);
        }
        String columnName = getColumnInformation(column).getOriginalName();
        if ("".equals(columnName) || options.useOldAliasMetadataBehavior || forceAlias) {
            return getColumnLabel(column);
        }
        return columnName;
    }

    /**
     * Get the designated column's table's schema.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return schema name or "" if not applicable
     * @throws SQLException if a database access error occurs
     */
    public String getCatalogName(int column) throws SQLException {
        return getColumnInformation(column).getDatabase();
    }

    /**
     * Get the designated column's specified column size. For numeric data, this is the maximum
     * precision. For character data, this is the length in characters. For datetime datatypes, this
     * is the length in characters of the String representation (assuming the maximum allowed
     * precision of the fractional seconds component). For binary data, this is the length in bytes.
     * For the ROWID datatype, this is the length in bytes. 0 is returned for data types where the
     * column size is not applicable.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return precision
     * @throws SQLException if a database access error occurs
     */
    public int getPrecision(final int column) throws SQLException {
        return (int) getColumnInformation(column).getPrecision();
    }

    /**
     * Gets the designated column's number of digits to right of the decimal point. 0 is returned for
     * data types where the scale is not applicable.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return scale
     * @throws SQLException if a database access error occurs
     */
    public int getScale(final int column) throws SQLException {
        return getColumnInformation(column).getDecimals();
    }

    /**
     * Gets the designated column's table name.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return table name or "" if not applicable
     * @throws SQLException if a database access error occurs
     */
    public String getTableName(final int column) throws SQLException {
        if (forceAlias) {
            return getColumnInformation(column).getTable();
        }

        if (options.blankTableNameMeta) {
            return "";
        }

        if (options.useOldAliasMetadataBehavior) {
            return getColumnInformation(column).getTable();
        }
        return getColumnInformation(column).getOriginalTable();
    }

    public String getSchemaName(int column) {
        return "";
    }

    /**
     * Retrieves the designated column's SQL type.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return SQL type from java.sql.Types
     * @throws SQLException if a database access error occurs
     * @see Types
     */
    public int getColumnType(final int column) throws SQLException {
        ColumnDefinition ci = getColumnInformation(column);
        if (isOracleMode) {
            switch (ci.getColumnType()) {
                case NVARCHAR2:
                    return ObOracleDefs.FIELD_JAVA_TYPE_NVARCHAR2;
                case BINARY_FLOAT:
                    return ObOracleDefs.FIELD_JAVA_TYPE_BINARY_FLOAT;
                case BINARY_DOUBLE:
                    return ObOracleDefs.FIELD_JAVA_TYPE_BINARY_DOUBLE;
                case OBDECIMAL:
                case NUMBER_FLOAT:
                    return ObOracleDefs.FIELD_JAVA_TYPE_NUMBER;
                case ROWID:
                    return ObOracleDefs.FIELD_JAVA_TYPE_ROWID;
                case NCHAR:
                    return ObOracleDefs.FIELD_JAVA_TYPE_NCHAR;
                case INTERVALYM:
                    return ObOracleDefs.FIELD_JAVA_TYPE_INTERVALYM;
                case INTERVALDS:
                    return ObOracleDefs.FIELD_JAVA_TYPE_INTERVALDS;
            }
        }
        switch (ci.getColumnType()) {
            case BIT:
                if (options.useNewResultSetMetaData) {
                    return getColumnTypeWithNewResultSet(ci);
                }
                if (ci.getLength() == 1) {
                    return Types.BIT;
                }
                return Types.VARBINARY;
            case TINYINT:
                if (ci.getLength() == 1 && options.tinyInt1isBit) {
                    if (options.transformedBitIsBoolean) {
                        return Types.BOOLEAN;
                    }
                    return Types.BIT;
                }
                return Types.TINYINT;
            case YEAR:
                if (options.yearIsDateType) {
                    return Types.DATE;
                }
                return Types.SMALLINT;
            case BLOB:
                if (!this.isOracleMode) {
                    if (options.useNewResultSetMetaData) {
                        return getColumnTypeWithNewResultSet(ci);
                    }
                    if (ci.getLength() <= 255 ){
                        return Types.VARBINARY;
                    }
                    return Types.LONGVARBINARY;
                } else {
                    if (ci.getLength() < 0 || ci.getLength() > 16777215) {
                        return Types.LONGVARBINARY;
                    }
                    return Types.VARBINARY;
                }
            case VARCHAR:
                if (!isOracleMode
                    && (ci.getSqltype() == 252 || ci.getSqltype() == 250 || ci.getSqltype() == 249)) { // text , tinytext and  mediumText
                    return Types.LONGVARCHAR;
                }
                if (ci.getSqltype() == 251) {
                    return Types.LONGVARCHAR;
                }
                if (ci.isBinary()) {
                    return Types.VARBINARY;
                }
                return Types.VARCHAR;
            case VARSTRING:
                if (ci.isBinary()) {
                    return Types.VARBINARY;
                }
                return Types.VARCHAR;
            case STRING:
                if (ci.isBinary()) {
                    return Types.BINARY;
                }
                return Types.CHAR;
            case MEDIUMBLOB:
                if (isOracleMode) {
                    return ci.getColumnType().getSqlType();
                } else {
                    return Types.LONGVARBINARY;
                }
            case JSON:
            case GEOMETRY:
                if (options.useNewResultSetMetaData) {
                    return getColumnTypeWithNewResultSet(ci);
                }
                return ci.getColumnType().getSqlType();
            default:
                return ci.getColumnType().getSqlType();
        }
    }

    public int getColumnTypeWithNewResultSet(ColumnDefinition ci) {
        switch (ci.getColumnType()) {
            case BLOB:
                if (!this.isOracleMode) {
                    if (!ci.isBinary()) {
                        if (ci.getLength() <= 255) {
                            return Types.VARCHAR;
                        }
                        return Types.LONGVARCHAR;
                    }
                    if (ci.getLength() <= 255){
                        return Types.VARBINARY;
                    }
                    return Types.LONGVARBINARY;
                }
                break;
            case JSON:
                if (!isOracleMode && options.compatibleMysqlVersion == 8) {
                    return Types.LONGVARCHAR;
                }
                return Types.CHAR;
            case GEOMETRY:
                return Types.BINARY;
            default:
                break;
        }
        return ci.getColumnType().getSqlType();
    }

    /**
     * Retrieves the designated column's database-specific type name.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return type name used by the database. If the column type is a user-defined type, then a
     *     fully-qualified type name is returned.
     * @throws SQLException if a database access error occurs
     */
    public String getColumnTypeName(final int column) throws SQLException {
        ColumnDefinition ci = getColumnInformation(column);
        return ColumnType.getColumnTypeName(ci.getColumnType(), ci.getLength(), ci.isSigned(),
            ci.isBinary(), isOracleMode, options);
    }

    /**
     * Indicates whether the designated column is definitely not writable.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @throws SQLException if a database access error occurs or in case of wrong index
     */
    public boolean isReadOnly(final int column) throws SQLException {
        ColumnDefinition ci = getColumnInformation(column);
        return !(ci.getOriginalTable() != null && ci.getOriginalTable().length() > 0
                 && ci.getName() != null && ci.getName().length() > 0);
    }

    /**
     * Indicates whether it is possible for a write on the designated column to succeed.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @throws SQLException if a database access error occurs or in case of wrong index
     */
    public boolean isWritable(final int column) throws SQLException {
        return !isReadOnly(column);
    }

    /**
     * Indicates whether a write on the designated column will definitely succeed.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @throws SQLException if a database access error occurs or in case of wrong index
     */
    public boolean isDefinitelyWritable(final int column) throws SQLException {
        return !isReadOnly(column);
    }

    /**
     * Returns the fully-qualified name of the Java class whose instances are manufactured if the
     * method <code>ResultSet.getObject</code> is called to retrieve a value from the column. <code>
     * ResultSet.getObject</code> may return a subclass of the class returned by this method.
     *
     * @param column the first column is 1, the second is 2, ...
     * @return the fully-qualified name of the class in the Java programming language that would be
     *     used by the method <code>ResultSet.getObject</code> to retrieve the value in the specified
     *     column. This is the class name used for custom mapping.
     * @throws SQLException if a database access error occurs
     */
    public String getColumnClassName(int column) throws SQLException {
        ColumnDefinition ci = getColumnInformation(column);
        ColumnType type = ci.getColumnType();
        return ColumnType.getClassName(type, (int) ci.getLength(), ci.isSigned(), ci.isBinary(),
            options);
    }

    private ColumnDefinition getColumnInformation(int column) throws SQLException {
        int actualColumnIndex = column + columnIndexOffset;
        if (actualColumnIndex >= 1 && actualColumnIndex <= fieldPackets.length) {
            return fieldPackets[actualColumnIndex - 1];
        }
        throw ExceptionFactory.INSTANCE.create(String.format(
            "wrong column index %s. must be in [1, %s] range", column, fieldPackets.length
                                                                       - columnIndexOffset));
    }

    /**
     * Returns an object that implements the given interface to allow access to non-standard methods,
     * or standard methods not exposed by the proxy. <br>
     * If the receiver implements the interface then the result is the receiver or a proxy for the
     * receiver. If the receiver is a wrapper and the wrapped object implements the interface then the
     * result is the wrapped object or a proxy for the wrapped object. Otherwise return the the result
     * of calling <code>unwrap</code> recursively on the wrapped object or a proxy for that result. If
     * the receiver is not a wrapper and does not implement the interface, then an <code>SQLException
     * </code> is thrown.
     *
     * @param iface A Class defining an interface that the result must implement.
     * @return an object that implements the interface. May be a proxy for the actual implementing
     *     object.
     * @throws SQLException If no object found that implements the interface
     */
    public <T> T unwrap(final Class<T> iface) throws SQLException {
        try {
            if (isWrapperFor(iface)) {
                return iface.cast(this);
            } else {
                throw new SQLException("The receiver is not a wrapper for " + iface.getName());
            }
        } catch (Exception e) {
            throw new SQLException(
                "The receiver is not a wrapper and does not implement the interface " + iface.getName());
        }
    }

    /**
     * Returns true if this either implements the interface argument or is directly or indirectly a
     * wrapper for an object that does. Returns false otherwise. If this implements the interface then
     * return true, else if this is a wrapper then return the result of recursively calling <code>
     * isWrapperFor</code> on the wrapped object. If this does not implement the interface and is not
     * a wrapper, return false. This method should be implemented as a low-cost operation compared to
     * <code>unwrap</code> so that callers can use this method to avoid expensive <code>unwrap</code>
     * calls that may fail. If this method returns true then calling <code>unwrap</code> with the same
     * argument should succeed.
     *
     * @param iface a Class defining an interface.
     * @return true if this implements the interface or directly or indirectly wraps an object that
     *     does.
     * @throws SQLException if an error occurs while determining whether this is a wrapper for an
     *     object with the given interface.
     */
    public boolean isWrapperFor(final Class<?> iface) throws SQLException {
        return iface.isInstance(this);
    }
}
