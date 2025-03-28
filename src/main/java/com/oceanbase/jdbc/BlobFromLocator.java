package com.oceanbase.jdbc;

import com.oceanbase.jdbc.internal.com.read.resultset.ColumnDefinition;
import com.oceanbase.jdbc.internal.util.exceptions.ExceptionFactory;
import com.oceanbase.jdbc.util.Options;

import java.io.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class BlobFromLocator extends Blob {

    private final String blobColName;
    private final String tableName;

    private OceanBaseConnection connection;

    private List<String> primaryKeys;

    private List<String> primaryValues;

    private Options options;

    class BlobFromLocatorInputStream extends InputStream {

        private int currPos;

        private final long length;

        BlobFromLocatorInputStream() throws SQLException {
            this.length = length();
        }

        @Override
        public int read() throws IOException {
            if (currPos + 1 > length) {
                return -1;
            }
            try {
                byte[] bytes = getBytes(currPos + 1, 1);
                if (bytes == null) {
                    return -1;
                }
                return bytes[0];
            } catch (SQLException e) {
                throw new IOException(e.toString());
            }
        }

        @Override
        public int read(byte[] b) throws IOException {
            if (this.currPos + 1 > this.length) {
                return -1;
            }

            try {
                byte[] bytes = getBytes(currPos + 1, b.length);
                if (bytes == null) {
                    return -1;
                }
                System.arraycopy(bytes, 0, b, 0, bytes.length);
                this.currPos += bytes.length;
                return bytes.length;
            } catch (SQLException e) {
                throw new IOException(e.toString());
            }
        }

        @Override
        public synchronized int read(byte[] b, int off, int len) throws IOException {
            if (this.currPos + 1 > this.length) {
                return -1;
            }

            try {
                byte[] bytes = getBytes(currPos + 1, len);
                if (bytes == null) {
                    return -1;
                }
                System.arraycopy(bytes, 0, b, off, bytes.length);
                this.currPos += bytes.length;
                return bytes.length;
            } catch (SQLException e) {
                throw new IOException(e.toString());
            }
        }
    }

    BlobFromLocator(JDBC4ResultSet resultSet, int columnIndex, Connection connection, Options options) throws SQLException {
        this.connection = (OceanBaseConnection) connection;
        this.primaryKeys = new ArrayList<>();
        this.primaryValues = new ArrayList<>();
        this.options = options;

        ColumnDefinition[] columnsInformation = resultSet.getColumnsInformation();
        int columnLen = columnsInformation.length;

        for (int i = 1; i <= columnLen; i++) {
            ColumnDefinition columnDefinition = columnsInformation[i - 1];
            if (columnDefinition.isPrimaryKey()) {
                String name = columnDefinition.getOriginalName();
                if (name == null || name.isEmpty()) {
                    name = columnDefinition.getName();
                }
                primaryKeys.add(name);
                primaryValues.add(resultSet.getString(i));
            }
        }

        if (columnLen <= 1 || primaryKeys.isEmpty()) {
            throw ExceptionFactory.INSTANCE.create("Emulated BLOB locators must come from a ResultSet with only one table selected, and all primary keys selected.");
        }

        String database = columnsInformation[0].getDatabase();
        String originalTable = columnsInformation[0].getOriginalTable();
        if (database == null || database.isEmpty()) {
            throw ExceptionFactory.INSTANCE.create("Emulated BLOB locators must come from a ResultSet with only one table selected, and all primary keys selected.");
        } else {
            this.tableName = OceanBaseConnection.quoteIdentifier(database) + "." + OceanBaseConnection.quoteIdentifier(originalTable);
        }

        this.blobColName = resultSet.getString(columnIndex);
    }

    @Override
    public InputStream getBinaryStream() throws SQLException {
        return new BufferedInputStream(new BlobFromLocatorInputStream(), this.options.locatorFetchBufferSize);
    }

    @Override
    public InputStream getBinaryStream(long pos, long length) throws SQLException {
        // there is a bug in mysql-jdbc, which always throws a null pointer exception.
        throw new NullPointerException();
    }

    @Override
    public void free() {
        super.free();
        this.primaryKeys = null;
        this.primaryValues = null;
        this.connection = null;
    }

    @Override
    public byte[] getBytes(long pos, int length) throws SQLException {
        String subStringSql = getSubStringSql();
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            ps = this.connection.prepareStatement(subStringSql);
            ps.setLong(1, pos);
            ps.setInt(2, length);
            for (int i = 0; i < primaryValues.size(); i++) {
                ps.setString(i + 3, primaryValues.get(i));
            }
            rs = ps.executeQuery();
            if (!rs.next()) {
                throw ExceptionFactory.INSTANCE.create("BLOB data not found! Did primary keys change?");
            }
            byte[] bytes = rs.getBytes(1);
            rs.close();
            return bytes;
        } finally {
            if (ps != null) {
                ps.close();
            }
        }
    }

    @Override
    public long length() throws SQLException {
        String lengthSql = getLengthSql();

        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            ps = this.connection.prepareStatement(lengthSql);
            for (int i = 0; i < primaryValues.size(); i++) {
                ps.setString(i + 1, primaryValues.get(i));
            }
            rs = ps.executeQuery();
            if (!rs.next()) {
                throw ExceptionFactory.INSTANCE.create("BLOB data not found! Did primary keys change?");
            }
            long res = rs.getLong(1);
            rs.close();
            return res;
        } finally {
            if (ps != null) {
                ps.close();
            }
        }
    }

    @Override
    public long position(java.sql.Blob pattern, long start) throws SQLException {
        return position(pattern.getBytes(0, (int) pattern.length()), start);
    }

    @Override
    public long position(byte[] pattern, long start) throws SQLException {
        String locateSql = getLocateSql(start);

        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            ps = this.connection.prepareStatement(locateSql);
            ps.setBytes(1, pattern);
            for (int i = 0; i < primaryValues.size(); i++) {
                ps.setString(i + 2, primaryValues.get(i));
            }
            rs = ps.executeQuery();

            if (!rs.next()) {
                throw ExceptionFactory.INSTANCE.create("BLOB data not found! Did primary keys change?");
            }
            long res = rs.getLong(1);
            rs.close();
            return res;
        } finally {
            if (ps != null) {
                ps.close();
            }
        }
    }

    @Override
    public OutputStream setBinaryStream(long pos) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int setBytes(long pos, byte[] bytes) throws SQLException {
        return setBytes(pos, bytes, 0, bytes.length);
    }

    @Override
    public int setBytes(long pos, byte[] bytes, int offset, int len) throws SQLException {
        if (offset + len > bytes.length) {
            len = bytes.length - offset;
        }

        byte[] byteToWrite = new byte[len];
        System.arraycopy(bytes, offset, byteToWrite, 0, len);
        String updateSql = getUpdateSql(pos, len);

        PreparedStatement ps = null;
        try {
            ps = this.connection.prepareStatement(updateSql);
            ps.setBytes(1, byteToWrite);
            for (int i = 0; i < primaryValues.size(); i++) {
                ps.setString(i + 2, primaryValues.get(i));
            }

            int executeUpdate = ps.executeUpdate();
            if (executeUpdate != 1) {
                throw ExceptionFactory.INSTANCE.create("BLOB data not found! Did primary keys change?");
            }
        }
        finally {
            if (ps != null) {
                ps.close();
            }
        }

        return (int) length();
    }

    @Override
    public void truncate(long len) throws SQLException {
        String truncateSql = getTruncateSql(len);

        PreparedStatement ps = null;
        try {
            ps = this.connection.prepareStatement(truncateSql);
            for (int i = 0; i < primaryValues.size(); i++) {
                ps.setString(i + 1 , primaryValues.get(i));
            }
            int executeUpdate = ps.executeUpdate();
            if (executeUpdate != 1) {
                throw ExceptionFactory.INSTANCE.create("BLOB data not found! Did primary keys change?");
            }
        }
        finally {
            if (ps != null) {
                ps.close();
            }
        }
    }

    private String getSubStringSql() {
        StringBuilder sqlBuilder = new StringBuilder("SELECT SUBSTRING(");
        sqlBuilder.append(OceanBaseConnection.quoteIdentifier(blobColName));
        sqlBuilder.append(", ?, ?");
        sqlBuilder.append(") FROM ");
        sqlBuilder.append(tableName);
        sqlBuilder.append(" WHERE ");
        for (int i = 0; i < primaryKeys.size(); i++) {
            if (i > 0) {
                sqlBuilder.append(" AND ");
            }
            sqlBuilder.append(OceanBaseConnection.quoteIdentifier(primaryKeys.get(i)));
            sqlBuilder.append(" = ?");
        }
        return sqlBuilder.toString();
    }

    private String getLocateSql(long start) {
        StringBuilder sqlBuilder = new StringBuilder("SELECT LOCATE(");
        sqlBuilder.append("?, ");
        sqlBuilder.append(OceanBaseConnection.quoteIdentifier(blobColName));
        sqlBuilder.append(", ");
        sqlBuilder.append(start);
        sqlBuilder.append(") FROM ");
        sqlBuilder.append(this.tableName);
        sqlBuilder.append(" WHERE ");
        for (int i = 0; i < primaryKeys.size(); i++) {
            if (i > 0) {
                sqlBuilder.append(" AND ");
            }
            sqlBuilder.append(OceanBaseConnection.quoteIdentifier(primaryKeys.get(i)));
            sqlBuilder.append(" = ?");
        }
        return sqlBuilder.toString();
    }

    private String getTruncateSql(long len) {
        StringBuilder sqlBuilder = new StringBuilder("UPDATE ");
        sqlBuilder.append(this.tableName);
        sqlBuilder.append(" SET ");
        sqlBuilder.append(OceanBaseConnection.quoteIdentifier(blobColName));
        sqlBuilder.append(" = LEFT(");
        sqlBuilder.append(OceanBaseConnection.quoteIdentifier(blobColName));
        sqlBuilder.append(", ");
        sqlBuilder.append(len);
        sqlBuilder.append(") WHERE ");
        for (int i = 0; i < primaryKeys.size(); i++) {
            if (i > 0) {
                sqlBuilder.append(" AND ");
            }
            sqlBuilder.append(OceanBaseConnection.quoteIdentifier(primaryKeys.get(i)));
            sqlBuilder.append(" = ?");
        }
        return sqlBuilder.toString();
    }

    private String getUpdateSql(long pos, int len) {
        StringBuilder query = new StringBuilder("UPDATE ");
        query.append(this.tableName);
        query.append(" SET ");
        query.append(OceanBaseConnection.quoteIdentifier(blobColName));
        query.append(" = INSERT(");
        query.append(OceanBaseConnection.quoteIdentifier(blobColName));
        query.append(", ");
        query.append(pos);
        query.append(", ");
        query.append(len);
        query.append(", ?) WHERE ");
        for (int i = 0; i < primaryKeys.size(); i++) {
            if (i > 0) {
                query.append(" AND ");
            }
            query.append(OceanBaseConnection.quoteIdentifier(primaryKeys.get(i)));
            query.append(" = ?");
        }
        return query.toString();
    }

    private String getLengthSql() {
        StringBuilder sqlBuilder = new StringBuilder("SELECT LENGTH(");
        sqlBuilder.append(OceanBaseConnection.quoteIdentifier(blobColName));
        sqlBuilder.append(") FROM ");
        sqlBuilder.append(this.tableName);
        sqlBuilder.append(" WHERE ");
        for (int i = 0; i < primaryKeys.size(); i++) {
            if (i > 0) {
                sqlBuilder.append(" AND ");
            }
            sqlBuilder.append(OceanBaseConnection.quoteIdentifier(primaryKeys.get(i)));
            sqlBuilder.append(" = ?");
        }
        return sqlBuilder.toString();
    }


}
