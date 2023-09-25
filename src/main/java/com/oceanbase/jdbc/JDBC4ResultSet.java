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

import java.io.*;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.sql.Clob;
import java.sql.Date;
import java.time.*;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

import com.oceanbase.jdbc.extend.datatype.*;
import com.oceanbase.jdbc.extend.datatype.INTERVALDS;
import com.oceanbase.jdbc.extend.datatype.INTERVALYM;
import com.oceanbase.jdbc.extend.datatype.TIMESTAMP;
import com.oceanbase.jdbc.extend.datatype.TIMESTAMPLTZ;
import com.oceanbase.jdbc.extend.datatype.TIMESTAMPTZ;
import com.oceanbase.jdbc.internal.ColumnType;
import com.oceanbase.jdbc.internal.com.Packet;
import com.oceanbase.jdbc.internal.com.read.Buffer;
import com.oceanbase.jdbc.internal.com.read.ErrorPacket;
import com.oceanbase.jdbc.internal.com.read.dao.CmdInformationSingle;
import com.oceanbase.jdbc.internal.com.read.dao.ColumnLabelIndexer;
import com.oceanbase.jdbc.internal.com.read.dao.Results;
import com.oceanbase.jdbc.internal.com.read.resultset.ColumnDefinition;
import com.oceanbase.jdbc.internal.com.read.resultset.RefCursor;
import com.oceanbase.jdbc.internal.com.read.resultset.SelectResultSet;
import com.oceanbase.jdbc.internal.com.read.resultset.UpdatableColumnDefinition;
import com.oceanbase.jdbc.internal.com.read.resultset.rowprotocol.BinaryRowProtocol;
import com.oceanbase.jdbc.internal.com.read.resultset.rowprotocol.RowProtocol;
import com.oceanbase.jdbc.internal.com.read.resultset.rowprotocol.TextRowProtocol;
import com.oceanbase.jdbc.internal.com.send.parameters.*;
import com.oceanbase.jdbc.internal.io.input.PacketInputStream;
import com.oceanbase.jdbc.internal.io.input.StandardPacketInputStream;
import com.oceanbase.jdbc.internal.protocol.Protocol;
import com.oceanbase.jdbc.internal.util.ResourceStatus;
import com.oceanbase.jdbc.internal.util.constant.ServerStatus;
import com.oceanbase.jdbc.internal.util.exceptions.ExceptionFactory;
import com.oceanbase.jdbc.util.Options;

public class JDBC4ResultSet implements ResultSetImpl {

    public static final int                 TINYINT1_IS_BIT       = 1;
    public static final int                 YEAR_IS_DATE_TYPE     = 2;
    private static final String             NOT_UPDATABLE_ERROR   = "Updates are not supported when using ResultSet.CONCUR_READ_ONLY";
    private static final ColumnDefinition[] INSERT_ID_COLUMNS;
    private static final int                MAX_ARRAY_SIZE        = Integer.MAX_VALUE - 8;
    public static String                    UpdatableResultSet_12 = "refreshRow() called on row that has been deleted or had primary key changed.";

    static {
        INSERT_ID_COLUMNS = new ColumnDefinition[1];
        INSERT_ID_COLUMNS[0] = ColumnDefinition.create("insert_id", ColumnType.BIGINT, false,
            "UTF-8");
        // used for empty result set
    }

    protected TimeZone                      timeZone;
    protected Options                       options;
    protected ColumnDefinition[]            columnsInformation;
    protected int                           columnInformationLength;
    protected int                           columnIndexOffset;
    protected boolean                       noBackslashEscapes;
    protected Protocol                      protocol;
    protected PacketInputStream             reader;
    private boolean                         callableResult;
    protected OceanBaseStatement            statement;
    protected int                           fetchSize;
    protected int                           resultSetScrollType;
    protected int                           resultSetConcurType;
    protected ColumnLabelIndexer            columnLabelIndexer;
    public RowProtocol                      row;
    protected byte[][]                      data;
    /**
     * dataSize can be -1, 0, 1, ..., n.
     * -1: client hasn't got result from server, whether the result set is empty remains unknown.
     * 0: client did get result from server, but the result set is empty without any row.
     * 1~n: the number of rows cached in "data[][]"
     */
    protected int                           dataSize              = -1;
    protected long                          processedRows;
    protected int                           discardedRows;
    protected int                           rowPointer            = -1;
    protected int                           lastRowPointer        = -1;
    /**
     * Mark the absolute position in entire result for the first row in cache.
     * The absolute position of the first row in entire result is 1.
     */
    protected int                           startIndexInEntireResult;
    protected int                           endIndexInEntireResult;
    protected boolean                       isEof;
    protected boolean                       isLastRowSent;
    protected ResourceStatus                status = ResourceStatus.OPEN;
    protected boolean                       isModified;
    private boolean                         eofDeprecated;
    private ReentrantLock                   lock;
    protected boolean                       forceAlias;
    public ComplexData[]                    complexData;
    public int[]                            complexEndPos;
    private boolean                         isPsOutParameter;
    private ResultSetMetaData               metaData;
    private ResultSetClass                  rsClass;
    private long createTime;
    private long closeTime;

    public enum ResultSetClass {
        COMPLETE,
        STREAMING,
        CURSOR
    };

    /****************************** updatable  characters *******************************/
    // Since not all types of result set use these variables, there can be a way not to initial them all the time
    protected static final int          STATE_STANDARD = 0;
    protected static final int          STATE_UPDATE   = 1;
    protected static final int          STATE_UPDATED  = 2;
    protected static final int          STATE_INSERT   = 3;

    protected OceanBaseConnection       connection;
    private String                      database;
    private String                      table;

    private boolean                     canBeUpdate;
    private boolean                     canBeInserted;
    private boolean                     canBeRefresh;
    protected int                       notInsertRowPointer;

    private String                      exceptionUpdateMsg;
    private String                      exceptionInsertMsg;
    protected int                       state          = STATE_STANDARD;
    private int                         updatableColumnLength;
    private UpdatableColumnDefinition[] updatableColumns;
    private ParameterHolder[]           updatableParameterHolders;
    private List<Integer>               primaryKeyIndicies = new ArrayList<>();
    private PreparedStatement           refreshPreparedStatement;
    private ClientSidePreparedStatement insertPreparedStatement;
    private ClientSidePreparedStatement deletePreparedStatement;

    /*************************** updatable characters ending ****************************/

    /**
     * Create a complete or streaming resultSet
     *
     * @param columnDefinition column information
     * @param results          results
     * @param protocol         current protocol
     * @param reader           stream fetcher
     * @param callableResult   is it from a callableStatement ?
     * @param eofDeprecated    is EOF deprecated
     * @throws IOException  if any connection error occur
     * @throws SQLException if any connection error occur
     */
    public JDBC4ResultSet(ColumnDefinition[] columnDefinition, Results results, Protocol protocol,
                          PacketInputStream reader, boolean callableResult, boolean eofDeprecated,
                          boolean isPsOutParameter) throws IOException, SQLException {
        if (protocol != null && protocol.getTimeTrace() != null) {
            createTime = System.nanoTime();
        }

        commonConstruct(columnDefinition, results, protocol, callableResult, eofDeprecated,
            isPsOutParameter);
        this.reader = reader;

        if (resultSetScrollType == ResultSet.TYPE_FORWARD_ONLY
            && resultSetConcurType == ResultSet.CONCUR_READ_ONLY && fetchSize == Integer.MIN_VALUE) {
            // definition of streaming resultSet by MySQL
            rsClass = ResultSetClass.STREAMING;
            fetchSize = 1;
            results.setFetchSize(1);
            this.lock = protocol.getLock();
            protocol.setActiveStreamingResult(results);
            protocol.removeHasMoreResults();
            data = new byte[Math.max(10, fetchSize)][];
            nextStreamingValue();
        } else {
            rsClass = ResultSetClass.COMPLETE;
            this.data = new byte[10][];
            fetchAllResults();
        }

        handelUpdatable(results);
    }

    private void commonConstruct(ColumnDefinition[] columnDefinition, Results results,
                                 Protocol protocol, boolean callableResult, boolean eofDeprecated,
                                 boolean isPsOutParameter) {
        this.statement = results.getStatement();
        this.protocol = protocol;
        this.options = protocol.getOptions();
        this.noBackslashEscapes = protocol.noBackslashEscapes();
        this.columnsInformation = columnDefinition;
        this.columnLabelIndexer = new ColumnLabelIndexer(columnsInformation);
        if (statement != null && statement.addRowid) {
            this.columnIndexOffset = 1;
        }
        this.columnInformationLength = columnDefinition.length;
        this.complexData = new ComplexData[columnInformationLength];
        this.complexEndPos = new int[columnInformationLength];
        timeZone = protocol.getTimeZone();
        if (results.isBinaryFormat()) {
            row = new BinaryRowProtocol(columnsInformation, columnInformationLength,
                results.getMaxFieldSize(), options);
        } else {
            row = new TextRowProtocol(results.getMaxFieldSize(), options);
        }
        row.setProtocol(protocol);
        this.fetchSize = results.getStatement() != null ? results.getStatement().getFetchSize()
            : results.getFetchSize();
        this.resultSetScrollType = results.getResultSetScrollType();
        this.resultSetConcurType = results.getResultSetConcurrency();
        this.callableResult = callableResult;
        this.eofDeprecated = eofDeprecated;
        this.isPsOutParameter = isPsOutParameter;
    }

    /**
     * Create a cursor resultSet
     *
     * @param columnDefinition
     * @param results
     * @param protocol
     * @param callableResult
     * @param eofDeprecated
     * @param isPsOutParameter
     * @throws IOException
     * @throws SQLException
     */
    public JDBC4ResultSet(ColumnDefinition[] columnDefinition, Results results, Protocol protocol,
                          boolean callableResult, boolean eofDeprecated, boolean isPsOutParameter)
                                                                                                  throws IOException,
                                                                                                  SQLException {
        if (protocol != null && protocol.getTimeTrace() != null) {
            createTime = System.nanoTime();
        }

        commonConstruct(columnDefinition, results, protocol, callableResult, eofDeprecated,
            isPsOutParameter);
        this.reader = protocol.getReader();
        this.lock = protocol.getLock();

        rsClass = ResultSetClass.CURSOR;
        data = new byte[Math.max(10, fetchSize)][];

        handelUpdatable(results);
    }

    public boolean assign(JDBC4ResultSet opt) {
        if (opt.data == null || opt.dataSize == 0) {
            return false;
        }
        this.statement = opt.statement;
        this.protocol = opt.protocol;
        this.options = opt.options;
        this.noBackslashEscapes = opt.noBackslashEscapes;
        this.columnsInformation = opt.columnsInformation;
        this.columnLabelIndexer = opt.columnLabelIndexer;
        if (statement != null && statement.addRowid) {
            this.columnIndexOffset = 1;
        }
        this.columnInformationLength = opt.columnInformationLength;
        this.complexData = opt.complexData;
        this.complexEndPos = opt.complexEndPos;

        this.reader = opt.reader;
        this.isEof = opt.isEof;
        timeZone = protocol.getTimeZone();
        this.row = opt.row;
        this.fetchSize = opt.fetchSize;
        this.resultSetScrollType = opt.resultSetScrollType;
        this.resultSetConcurType = opt.resultSetConcurType;
        this.dataSize = opt.dataSize;
        this.rowPointer = opt.rowPointer;
        this.callableResult = opt.callableResult;
        this.eofDeprecated = opt.eofDeprecated;
        this.isPsOutParameter = opt.isPsOutParameter;
        this.data = opt.data;
        this.rsClass = opt.rsClass;
        return true;
    }

    public void changeRowProtocol(RowProtocol row) {
        this.row = row;
    }

    public void setProtocol(Protocol protocol) {
        this.protocol = protocol;
    }

    /**
     * Create filled result-set.
     *
     * @param columnDefinition    column information
     * @param resultSet           result-set data
     * @param protocol            current protocol
     * @param resultSetScrollType one of the following <code>ResultSet</code> constants: <code>
     *                            ResultSet.TYPE_FORWARD_ONLY</code>, <code>ResultSet.TYPE_SCROLL_INSENSITIVE</code>, or
     *                            <code>ResultSet.TYPE_SCROLL_SENSITIVE</code>
     */
    public JDBC4ResultSet(ColumnDefinition[] columnDefinition, List<byte[]> resultSet,
                          Protocol protocol, int resultSetScrollType) {
        if (protocol != null && protocol.getTimeTrace() != null) {
            createTime = System.nanoTime();
        }

        this.statement = null;
        if (protocol != null) {
            this.options = protocol.getOptions();
            this.timeZone = protocol.getTimeZone();
        } else {
            this.options = new Options();
            this.timeZone = TimeZone.getDefault();
        }

        this.row = new TextRowProtocol(0, this.options);
        this.row.setProtocol(protocol);
        this.protocol = protocol;
        this.columnsInformation = columnDefinition;
        this.columnLabelIndexer = new ColumnLabelIndexer(columnsInformation);
        if (statement != null && statement.addRowid) {
            this.columnIndexOffset = 1;
        }
        this.columnInformationLength = columnDefinition.length;
        this.isEof = true;
        this.fetchSize = 0;
        this.resultSetScrollType = resultSetScrollType;
        this.resultSetConcurType = ResultSet.CONCUR_READ_ONLY;
        this.data = resultSet.toArray(new byte[10][]);
        this.dataSize = resultSet.size();
        this.callableResult = false;
        this.rsClass = ResultSetClass.COMPLETE;
    }

    public Protocol getProtocol() {
        return protocol;
    }

    /**
     * Create a result set from given data. Useful for creating "fake" resultsets for
     * DatabaseMetaData, (one example is MariaDbDatabaseMetaData.getTypeInfo())
     *
     * @param data                 - each element of this array represents a complete row in the ResultSet. Each value
     *                             is given in its string representation, as in MariaDB text protocol, except boolean (BIT(1))
     *                             values that are represented as "1" or "0" strings
     * @param protocol             protocol
     * @param findColumnReturnsOne - special parameter, used only in generated key result sets
     * @return resultset
     */
    public static ResultSet createGeneratedData(
            long[] data, Protocol protocol, boolean findColumnReturnsOne) {
        ColumnDefinition[] columns = new ColumnDefinition[1];
        columns[0] = ColumnDefinition.create("GENERATED_KEY", ColumnType.BIGINT, protocol.isOracleMode(), protocol.getOptions().getCharacterEncoding());

        List<byte[]> rows = new ArrayList<>();
        for (long rowData : data) {
            if (rowData != 0) {
                if(rowData < 0) {
                    columns[0].setUnSigned();
                    byte[] bytes = new byte[8];
                    bytes[0] = (byte) (rowData >>> 56);
                    bytes[1] = (byte) (rowData >>> 48);
                    bytes[2] = (byte) (rowData >>> 40);
                    bytes[3] = (byte) (rowData >>> 32);
                    bytes[4] = (byte) (rowData >>> 24);
                    bytes[5] = (byte) (rowData >>> 16);
                    bytes[6] = (byte) (rowData >>> 8);
                    bytes[7] = (byte) (rowData & 0xff);
                    BigInteger val = new BigInteger(1, bytes);
                    rows.add(StandardPacketInputStream.create(val.toString().getBytes()));
                } else {
                    rows.add(StandardPacketInputStream.create(String.valueOf(rowData).getBytes()));
                }
            }
        }
        if (findColumnReturnsOne) {
            return new JDBC4ResultSet(columns, rows, protocol, TYPE_SCROLL_SENSITIVE) {
                @Override
                public int findColumn(String name) {
                    return 1;
                }
            };
        }
        return new JDBC4ResultSet(columns, rows, protocol, TYPE_SCROLL_SENSITIVE);
    }

    /**
     * Create a result set from given data. Useful for creating "fake" resultSets for
     * DatabaseMetaData, (one example is MariaDbDatabaseMetaData.getTypeInfo())
     *
     * @param columnNames - string array of column names
     * @param columnTypes - column types
     * @param data        - each element of this array represents a complete row in the ResultSet. Each value
     *                    is given in its string representation, as in MariaDB text protocol, except boolean (BIT(1))
     *                    values that are represented as "1" or "0" strings
     * @param protocol    protocol
     * @return resultset
     */
    public static JDBC4ResultSet createResultSet(
            String[] columnNames, ColumnType[] columnTypes, String[][] data, Protocol protocol) {
        int columnNameLength = columnNames.length;
        ColumnDefinition[] columns = new ColumnDefinition[columnNameLength];

        //boolean isOracleMode = this.protocol.isOracleMode();
        for (int i = 0; i < columnNameLength; i++) {
            columns[i] = ColumnDefinition.create(columnNames[i], columnTypes[i], protocol.isOracleMode(),protocol.getOptions().getCharacterEncoding());
        }

        List<byte[]> rows = new ArrayList<>();

        for (String[] rowData : data) {
            byte[][] rowBytes = new byte[rowData.length][];
            for (int i = 0; i < rowData.length; i++) {
                if (rowData[i] != null) {
                    rowBytes[i] = rowData[i].getBytes();
                }
            }
            rows.add(StandardPacketInputStream.create(rowBytes, columnTypes));
        }
        return new JDBC4ResultSet(columns, rows, protocol, TYPE_SCROLL_SENSITIVE);
    }

    public static JDBC4ResultSet createEmptyResultSet() {
        return new JDBC4ResultSet(INSERT_ID_COLUMNS, new ArrayList<>(), null, TYPE_SCROLL_SENSITIVE);
    }

    /**
     * Indicate if result-set is still streaming results from server.
     *
     * @return true if streaming is finished
     */
    public boolean isFullyLoaded() {
        // result-set is fully loaded when reaching EOF packet.
        return isEof;
    }

    protected SQLException handleIoException(IOException ioe) {
        return ExceptionFactory.INSTANCE
            .create(
                "Server has closed the connection. \n"
                        + "Please check net_read_timeout/net_write_timeout/wait_timeout server variables. "
                        + "If result set contain huge amount of data, Server expects client to"
                        + " read off the result set relatively fast. "
                        + "In this case, please consider increasing net_read_timeout session variable"
                        + " / processing your result set faster (check Streaming result sets documentation for more information)",
                "08000", ioe);
    }

    public ColumnDefinition[] getColumnsInformation() {
        return columnsInformation;
    }

    private void fetchAllResults() throws IOException, SQLException {
        dataSize = 0;
        processedRows = 0;
        while (!isEof) {
            addStreamingValue();
        }
        updateStartIndexInEntireResult();// startIndexInEntireResult is supposed to be 1
    }

    /**
     * When protocol has a current Streaming result (this) fetch all to permit another query is executing.
     *
     * @throws SQLException if any error occur
     */
    public void fetchRemaining() throws SQLException {
        if (!isEof) {
            lock.lock();
            try {
                lastRowPointer = -1;
                while (!isEof) {
                    addStreamingValue();
                }
                updateStartIndexInEntireResult();
            } catch (SQLException queryException) {
                throw ExceptionFactory.INSTANCE.create(queryException);
            } catch (IOException ioe) {
                throw handleIoException(ioe);
            } finally {
                lock.unlock();
            }
        }
    }

    /**
     * This permit to replace current stream results by next ones.
     *
     * @throws IOException  if socket exception occur
     * @throws SQLException if server return an unexpected error
     */
    private void nextStreamingValue() throws IOException, SQLException {
        lastRowPointer = -1;

        // STREAMING has no need to cache a previous row
        if (dataSize > 0) {
            discardedRows += dataSize;
            dataSize = 0;
        }

        addStreamingValue();
        updateStartIndexInEntireResult();
    }

    /**
     * This permit to add next streaming values to existing resultSet.
     *
     * @throws IOException  if socket exception occur
     * @throws SQLException if server return an unexpected error
     */
    protected void addStreamingValue() throws IOException, SQLException {
        if (dataSize == -1) {
            dataSize = 0;
        }
        processedRows = 0;
        // read only one row once
        byte[] buf = getNextRow();
        if (statement == null || statement.getMaxRows() <= 0
            || endIndexInEntireResult < statement.getMaxRows()) {
            if (buf != null) {
                if (dataSize + 1 >= data.length) {
                    growDataArray();
                }
                data[dataSize++] = buf;
                processedRows++;
                endIndexInEntireResult++;
            }
        } else {
            isLastRowSent = true;
        }
    }

    protected void updateStartIndexInEntireResult() {
        if (dataSize > 0) {
            startIndexInEntireResult = endIndexInEntireResult - dataSize + 1;
        } else {
            startIndexInEntireResult = 0;
            endIndexInEntireResult = 0;
        }
    }

    /**
     * Read next value.
     *
     * @return true if have a new value
     * @throws IOException  exception
     * @throws SQLException exception
     */
    protected byte[] getNextRow() throws IOException, SQLException {
        byte[] buf = this.reader.getPacketArray(false);

        // is error Packet
        if (buf[0] == Packet.ERROR) {
            protocol.removeActiveStreamingResult();
            protocol.removeHasMoreResults();
            protocol.setHasWarnings(false);
            ErrorPacket errorPacket = new ErrorPacket(new Buffer(buf));
            isEof = true;
            throw ExceptionFactory.INSTANCE.create(errorPacket.getMessage(),
                errorPacket.getSqlState(), errorPacket.getErrorCode());
        }

        // is end of stream
        if (buf[0] == Packet.EOF
            && ((eofDeprecated && buf.length < 0xffffff) || (!eofDeprecated && buf.length < 8))) {
            int serverStatus;
            int warnings;

            if (!eofDeprecated) {
                // EOF_Packet
                warnings = (buf[1] & 0xff) + ((buf[2] & 0xff) << 8);
                serverStatus = ((buf[3] & 0xff) + ((buf[4] & 0xff) << 8));

                // CallableResult has been read from intermediate EOF server_status
                // and is mandatory because :
                //
                // - Call query will have an callable resultSet for OUT parameters
                //   this resultSet must be identified and not listed in JDBC statement.getResultSet()
                //
                // - after a callable resultSet, a OK packet is send,
                //   but mysql before 5.7.4 doesn't send MORE_RESULTS_EXISTS flag

                if (callableResult) { //TODO check it set MORE_RESULTS_EXISTS in callableResult,
                    //serverStatus |= MORE_RESULTS_EXISTS;
                }
            } else {
                // OK_Packet with a 0xFE header
                int pos = skipLengthEncodedValue(buf, 1); // skip update count
                pos = skipLengthEncodedValue(buf, pos); // skip insert id
                serverStatus = ((buf[pos++] & 0xff) + ((buf[pos++] & 0xff) << 8));
                warnings = (buf[pos++] & 0xff) + ((buf[pos] & 0xff) << 8);
                callableResult = (serverStatus & ServerStatus.PS_OUT_PARAMETERS) != 0;
            }
            protocol.setServerStatus((short) serverStatus);
            protocol.setHasWarnings(warnings > 0);
            if ((serverStatus & ServerStatus.MORE_RESULTS_EXISTS) == 0) {
                protocol.removeActiveStreamingResult();
            }
            isLastRowSent = (serverStatus & ServerStatus.LAST_ROW_SENT) != 0;
            isEof = true;
            return null;
        }

        // is a result-set row, save it
        isEof = false;
        return buf;
    }

    /**
     * Get current row's raw bytes.
     *
     * @return row's raw bytes
     */
    public byte[] getCurrentRowData() {
        return data[rowPointer];
    }

    /**
     * Update row's raw bytes. in case of row update, refresh the data. (format must correspond to
     * current resultset binary/text row encryption)
     *
     * @param rawData new row's raw data.
     */
    protected void updateRowData(byte[] rawData) {
        data[rowPointer] = rawData;
        row.resetRow(data[rowPointer]);
    }

    private void updateRowDataOneColumn(byte[] newColumn) {
        byte[] newDataRow = new byte[data[rowPointer].length - row.length + newColumn.length];

        System.arraycopy(data[rowPointer], 0, newDataRow, 0, row.pos);
        System.arraycopy(newColumn, 0, newDataRow, row.pos, newColumn.length);
        System.arraycopy(data[rowPointer], row.pos + row.length, newDataRow, row.pos
                                                                             + newColumn.length,
            data[rowPointer].length - row.pos - row.length);

        newDataRow[row.pos - 1] = (byte) newColumn.length;
        updateRowData(newDataRow);
    }

    /**
     * Delete current data. Position cursor to the previous row.
     *
     * @throws SQLException if previous() fail.
     */
    protected void deleteCurrentRowData() throws SQLException {
        // move data
        System.arraycopy(data, rowPointer + 1, data, rowPointer, dataSize - 1 - rowPointer);
        data[dataSize - 1] = null;
        dataSize--;
        lastRowPointer = -1;
        previous();
    }

    public void addRowData(byte[] rawData) {
        if (dataSize + 1 >= data.length) {
            growDataArray();
        }
        data[dataSize] = rawData;
        rowPointer = dataSize;
        dataSize++;
    }

    private int skipLengthEncodedValue(byte[] buf, int pos) {
        int type = buf[pos++] & 0xff;
        switch (type) {
            case 251:
                return pos;
            case 252:
                return pos + 2 + (0xffff & (((buf[pos] & 0xff) + ((buf[pos + 1] & 0xff) << 8))));
            case 253:
                return pos
                       + 3
                       + (0xffffff & ((buf[pos] & 0xff) + ((buf[pos + 1] & 0xff) << 8) + ((buf[pos + 2] & 0xff) << 16)));
            case 254:
                return (int) (pos + 8 + ((buf[pos] & 0xff) + ((long) (buf[pos + 1] & 0xff) << 8)
                                         + ((long) (buf[pos + 2] & 0xff) << 16)
                                         + ((long) (buf[pos + 3] & 0xff) << 24)
                                         + ((long) (buf[pos + 4] & 0xff) << 32)
                                         + ((long) (buf[pos + 5] & 0xff) << 40)
                                         + ((long) (buf[pos + 6] & 0xff) << 48) + ((long) (buf[pos + 7] & 0xff) << 56)));
            default:
                return pos + type;
        }
    }

    /**
     * Grow data array.
     */
    private void growDataArray() {
        int newCapacity = data.length + (data.length >> 1);
        if (newCapacity - MAX_ARRAY_SIZE > 0) {
            newCapacity = MAX_ARRAY_SIZE;
        }
        data = Arrays.copyOf(data, newCapacity);
    }

    /**
     * Connection.abort() has been called, abort result-set.
     *
     * @throws SQLException exception
     */
    public void abort() throws SQLException {
        status = ResourceStatus.CLOSED;
        isEof = true;

        // keep garbage easy
        for (int i = 0; i < data.length; i++) {
            data[i] = null;
        }

        if (statement != null) {
            statement.checkCloseOnCompletion(this);
            statement = null;
        }
    }

    /**
     * Close resultSet.
     */
    public void close() throws SQLException {
        realClose(true);
    }

    public void realClose(boolean calledExplicitly) throws SQLException {
        if (status == ResourceStatus.CLOSING || status == ResourceStatus.CLOSED) {
            return;
        }
        if (protocol != null) {
            protocol.startCallInterface();
        }

        try {
            status = ResourceStatus.CLOSING;
            SQLException exceptionDuringClose = null;
            try {
                closeUpdatable();
            } catch (SQLException sqlEx) {
                exceptionDuringClose = sqlEx;
            }

            if (rsClass == ResultSetClass.STREAMING && !isEof) {
                lock.lock();
                try {
                    while (!isEof) {
                        dataSize = 0; // to avoid storing data
                        getNextRow();
                    }
                    processedRows = 0;
                } catch (SQLException queryException) {
                    throw ExceptionFactory.INSTANCE.create(queryException);
                } catch (IOException ioe) {
                    throw handleIoException(ioe);
                } finally {
                    isEof = true;
                    lock.unlock();
                }
            }

            // keep garbage easy
            Arrays.fill(data, null);

            if (statement != null) {
                try {
                    statement.checkCloseOnCompletion(this);
                } catch (SQLException sqlEx) {
                    exceptionDuringClose = sqlEx;
                }
                statement = null;
            }

            row = null;
            columnsInformation = null;
            complexData = null;
            complexEndPos = null;
            status = ResourceStatus.CLOSED;

            if (exceptionDuringClose != null) {
                throw exceptionDuringClose;
            }
        } finally {
            if (protocol != null) {
                if (protocol.getTimeTrace() != null) {
                    closeTime = System.nanoTime();
                }
                protocol.endCallInterface("JDBC4ResultSet.realClose(live for " + ((closeTime - createTime) / 100) + "us)");
            }
        }
    }

    private void resetVariables() {
        isEof = false;
        isLastRowSent = false;
    }

    // check row pos and column bounds
    private void checkObjectRange(int position) throws SQLException {
        checkClose();

        if (rowPointer < 0) {
            throw new SQLDataException("Current position is before the first row", "22023");
        }

        if (rowPointer >= dataSize) {
            throw new SQLDataException("Current position is after the last row", "22023");
        }

        if (position <= 0 || position > columnInformationLength) {
            throw new SQLDataException("No such column: " + position, "22023");
        }

        if (lastRowPointer != rowPointer || isModified) {
            row.resetRow(data[rowPointer]);
            lastRowPointer = rowPointer;
        }
        row.setPosition(position - 1);
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        if (!protocol.isOracleMode()) {
            checkClose();
        }

        if (this.statement == null) {
            return null;
        }
        return this.statement.getWarnings();
    }

    @Override
    public void clearWarnings() throws SQLException {
        if (!protocol.isOracleMode()) {
            checkClose();
        }

        if (this.statement != null) {
            this.statement.clearWarnings();
        }
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        checkClose();
        return dataSize != 0 && rowPointer == -1;
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        checkClose();
        if (rowPointer < dataSize) {
            // has remaining results
            return false;
        } else {
            // STREAMING ResultSet
            if (rsClass == ResultSetClass.STREAMING) {
                if (!isEof) {
                    // has to read more result to know if it's finished or not
                    // (next packet may be new data or an EOF packet indicating that there is no more data)
                    lock.lock();
                    try {
                        // this time, fetch is added even for streaming forward type only to keep current pointer row.
                        addStreamingValue();
                    } catch (IOException ioe) {
                        throw handleIoException(ioe);
                    } finally {
                        lock.unlock();
                    }
                }
                return dataSize == rowPointer && isEof;
            }

            // COMPLETE ResultSet
            // has read all data and pointer is after last row, so return true
            // but when resultSet contain no row at all, ServerStatus.LAST_ROW_SENT is 1, but jdbc say that must return false
            return dataSize > 0;
        }
    }

    @Override
    public boolean isFirst() throws SQLException {
        checkClose();
        if (rsClass == ResultSetClass.STREAMING) {
            throw new SQLException("Invalid operation on STREAMING ResultSet");
        }

        // COMPLETE ResultSet
        return rowPointer == 0 && dataSize > 0;
    }

    @Override
    public boolean isLast() throws SQLException {
        checkClose();
        if (rsClass == ResultSetClass.STREAMING) {
            throw new SQLException("Invalid operation on STREAMING ResultSet");
        }

        // COMPLETE ResultSet
        if (protocol.isOracleMode()) {
            throw new SQLException("Invalid operation on TYPE_FORWARD_ONLY ResultSet");
        }
        return isEof && rowPointer == dataSize - 1 && dataSize > 0;
    }

    @Override
    public void beforeFirst() throws SQLException {
        if (resultSetScrollType == TYPE_FORWARD_ONLY && protocol.isOracleMode()) {
            throw new SQLException("Invalid operation on TYPE_FORWARD_ONLY ResultSet");
        }
        checkClose();
        if (rsClass == ResultSetClass.STREAMING) {
            throw new SQLException("Invalid operation on STREAMING ResultSet");
        }
        cancelRowInserts();

        // COMPLETE ResultSet
        if (dataSize > 0) {
            rowPointer = -1;
        }
    }

    @Override
    public void afterLast() throws SQLException {
        if (resultSetScrollType == TYPE_FORWARD_ONLY && protocol.isOracleMode()) {
            throw new SQLException("Invalid operation on TYPE_FORWARD_ONLY ResultSet");
        }
        checkClose();
        if (rsClass == ResultSetClass.STREAMING) {
            throw new SQLException("Invalid operation on STREAMING ResultSet");
        }
        cancelRowInserts();

        // COMPLETE ResultSet
        if (dataSize > 0) {
            rowPointer = dataSize;
        }
    }

    @Override
    public boolean first() throws SQLException {
        if (resultSetScrollType == TYPE_FORWARD_ONLY && protocol.isOracleMode()) {
            throw new SQLException("Invalid operation on TYPE_FORWARD_ONLY ResultSet");
        }
        checkClose();
        if (rsClass == ResultSetClass.STREAMING) {
            throw new SQLException("Invalid operation on STREAMING ResultSet");
        }
        cancelRowInserts();

        // COMPLETE ResultSet, dataSize != -1
        if (dataSize == 0) {
            return false;
        }
        rowPointer = 0;
        return dataSize > 0;
    }

    @Override
    public boolean last() throws SQLException {
        if (resultSetScrollType == TYPE_FORWARD_ONLY && protocol.isOracleMode()) {
            throw new SQLException("Invalid operation on TYPE_FORWARD_ONLY ResultSet");
        }
        checkClose();
        if (rsClass == ResultSetClass.STREAMING) {
            throw new SQLException("Invalid operation on STREAMING ResultSet");
        }
        cancelRowInserts();

        // COMPLETE ResultSet
        rowPointer = dataSize - 1;
        return dataSize > 0;
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        if (resultSetScrollType == TYPE_FORWARD_ONLY && protocol.isOracleMode()) {
            throw new SQLException("Invalid operation on TYPE_FORWARD_ONLY ResultSet");
        }
        checkClose();
        if (rsClass == ResultSetClass.STREAMING) {
            throw new SQLException("Invalid operation on STREAMING ResultSet");
        }

        // COMPLETE ResultSet
        if (row == 0 && protocol.isOracleMode()) {
            // Compatible with Oracle
            throw new SQLException("Invalid parameter: absolute(0)");
        }
        cancelRowInserts();

        if (dataSize == 0) {
            // Compatible with Oracle and MySQL
            return false;
        }
        if (row == 0 && !protocol.isOracleMode()) {
            // Compatible with MySQL
            beforeFirst();
            return false;
        }

        if (row > 0) {
            if (row <= dataSize) {
                rowPointer = row - 1;
                return true;
            }

            rowPointer = dataSize; // go to after last position
            return false;
        } else {
            if (dataSize + row >= 0) {
                // absolute position reverse from ending resultSet
                rowPointer = dataSize + row;
                return true;
            }

            rowPointer = -1; // go to before first position
            return false;
        }
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        if (resultSetScrollType == TYPE_FORWARD_ONLY && protocol.isOracleMode()) {
            throw new SQLException("Invalid operation on TYPE_FORWARD_ONLY ResultSet");
        }
        checkClose();
        if (rsClass == ResultSetClass.STREAMING) {
            throw new SQLException("Invalid operation on STREAMING ResultSet");
        }
        cancelRowInserts();

        // COMPLETE ResultSet
        if (dataSize == 0) {
            return false;
        }
        int newPos = rowPointer + rows;
        if (newPos <= -1) {
            rowPointer = -1;
            return false;
        } else if (newPos >= dataSize) {
            rowPointer = dataSize;
            return false;
        } else {
            rowPointer = newPos;
            return true;
        }
    }

    @Override
    public boolean previous() throws SQLException {
        if (resultSetScrollType == TYPE_FORWARD_ONLY && protocol.isOracleMode()) {
            throw new SQLException("Invalid operation on TYPE_FORWARD_ONLY ResultSet");
        }
        checkClose();
        if (rsClass == ResultSetClass.STREAMING) {
            throw new SQLException("Invalid operation on STREAMING ResultSet");
        }
        cancelRowInserts();

        // COMPLETE ResultSet
        if (rowPointer > -1) {
            rowPointer--;
            return rowPointer > -1;
        }
        return false;
    }

    @Override
    public boolean next() throws SQLException {
        checkClose();
        cancelRowInserts();

        if (dataSize == 0) {
            return false;
        }
        if (rowPointer < dataSize - 1) {
            rowPointer++;
            return true;
        }

        if (rsClass == ResultSetClass.STREAMING && !isEof) {
            lock.lock();
            try {
                nextStreamingValue();
            } catch (IOException ioe) {
                throw handleIoException(ioe);
            } finally {
                lock.unlock();
            }

            rowPointer = 0;
            return dataSize > 0;
        }

        // all data are reads and pointer is after last
        rowPointer = dataSize;
        return false;
    }

    @Override
    public int getRow() throws SQLException {
        checkClose();
        if (resultSetScrollType == TYPE_FORWARD_ONLY) {
            return discardedRows + rowPointer + 1;
        }
        return rowPointer + 1;
    }

    @Override
    public int getFetchDirection() throws SQLException {
        if (!protocol.isOracleMode()) {
            checkClose();
        }
        return FETCH_UNKNOWN;//todo
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        if (!protocol.isOracleMode()) {
            checkClose();
        }
        if (direction == FETCH_REVERSE) {
            throw new SQLException(
                "Invalid operation. Allowed direction are ResultSet.FETCH_FORWARD and ResultSet.FETCH_UNKNOWN");
        }//todo
    }

    @Override
    public int getFetchSize() throws SQLException {
        if (!protocol.isOracleMode()) {
            checkClose();
        }
        return this.fetchSize;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        if (!protocol.isOracleMode()) {
            checkClose();
        }

        if (rows < 0) {
            throw new SQLException("invalid fetch size ");
        }

        // Oracle JDBC would change 0 to default
        if (this.protocol.isOracleMode() && rows == 0) {
            this.fetchSize = this.statement.fetchSize;
        } else {
            this.fetchSize = rows;
        }
    }

    @Override
    public int getType() {
        return resultSetScrollType;
    }

    public ResultSetClass getRsClass() {
        return rsClass;
    }

    @Override
    public int getConcurrency() {
        return resultSetConcurType;
    }

    protected void checkClose() throws SQLException {
        if (isClosed()) {
            throw new SQLException("Operation not permit on a closed resultSet", "HY000");
        }

        if (statement != null && statement.isClosed()) {
            throw new SQLException("Operation not permit on a closed statement", "HY000");
        }
    }

    public boolean isCallableResult() {
        return callableResult;
    }

    public boolean isPsOutParameter() {
        return isPsOutParameter;
    }

    public boolean isClosed() {
        return status == ResourceStatus.CLOSED;
    }

    public OceanBaseStatement getStatement() throws SQLException {
        if (!protocol.isOracleMode()) {
            checkClose();
        }
        return statement;
    }

    public void setStatement(OceanBaseStatement statement) {
        this.statement = statement;
    }

    /**
     * {inheritDoc}.
     */
    public boolean wasNull() throws SQLException {
        if (protocol.isOracleMode()) {
            checkClose();
        }
        return row.wasNull();
    }

    /**
     * {inheritDoc}.
     */
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        return getAsciiStream(findColumn(columnLabel));
    }

    /**
     * {inheritDoc}.
     */
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        int actualColumnIndex = columnIndex + columnIndexOffset;
        checkObjectRange(actualColumnIndex);
        if (row.lastValueWasNull()) {
            return null;
        }
        if (columnsInformation[actualColumnIndex - 1].getColumnType().getType() == ColumnType.ORA_CLOB
            .getType()) {
            java.sql.Clob clob = getClob(columnIndex);
            if (clob == null) {
                return null;
            }
            return clob.getAsciiStream();
        }
        if (columnsInformation[actualColumnIndex - 1].getColumnType().getType() == ColumnType.ORA_BLOB
            .getType()) {
            java.sql.Blob blob = getBlob(columnIndex);
            if (blob == null) {
                return null;
            }
            return blob.getBinaryStream();
        }
        try {
            return new ByteArrayInputStream(new String(row.buf, row.pos,
                row.getLengthMaxFieldSize(), protocol.getEncoding()).getBytes());
        } catch (UnsupportedEncodingException e) {
            throw new SQLException("Unsupported character encoding " + protocol.getEncoding());
        }
    }

    /**
     * {inheritDoc}.
     */
    public String getString(int columnIndex) throws SQLException {
        int actualColumnIndex = columnIndex + columnIndexOffset;
        checkObjectRange(actualColumnIndex);
        switch (columnsInformation[actualColumnIndex - 1].getColumnType()) {
            case ORA_CLOB:
                java.sql.Clob clob = getClob(columnIndex);
                if (clob == null) {
                    return null;
                }
                long len = clob.length();
                return clob.getSubString(1, (int) len);
            case ORA_BLOB:
                throw new SQLFeatureNotSupportedException();
            case TIMESTAMP_TZ:
                TIMESTAMPTZ timestamptz = row.getInternalTIMESTAMPTZ(
                    columnsInformation[actualColumnIndex - 1], null, timeZone);
                if (timestamptz == null) {
                    return null;
                }
                return timestamptz.toResultSetString(this.statement.getConnection());
            case TIMESTAMP_LTZ:
                TIMESTAMPLTZ timestampltz = row.getInternalTIMESTAMPLTZ(
                    columnsInformation[actualColumnIndex - 1], null, timeZone);
                if (timestampltz == null) {
                    return null;
                }
                return timestampltz.toResultSetString(this.statement.getConnection());
            default:
                break;
        }

        return row.getInternalString(columnsInformation[actualColumnIndex - 1], null, timeZone);
    }

    /**
     * {inheritDoc}.
     */
    public String getString(String columnLabel) throws SQLException {
        return getString(findColumn(columnLabel));
    }

    private String zeroFillingIfNeeded(String value, ColumnDefinition columnDefinition) {
        if (columnDefinition.isZeroFill()) {
            StringBuilder zeroAppendStr = new StringBuilder();
            long zeroToAdd = columnDefinition.getDisplaySize() - value.length();
            while (zeroToAdd-- > 0) {
                zeroAppendStr.append("0");
            }
            return zeroAppendStr.append(value).toString();
        }
        return value;
    }

    /**
     * {inheritDoc}.
     */
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        int actualColumnIndex = columnIndex + columnIndexOffset;
        checkObjectRange(actualColumnIndex);
        if (row.lastValueWasNull()) {
            return null;
        }
        if (columnsInformation[actualColumnIndex - 1].getColumnType().getType() == ColumnType.ORA_CLOB
            .getType()) {
            return getClob(columnIndex).getAsciiStream();
        }
        if (columnsInformation[actualColumnIndex - 1].getColumnType().getType() == ColumnType.ORA_BLOB
            .getType()) {
            return getBlob(columnIndex).getBinaryStream();
        }
        return new ByteArrayInputStream(row.buf, row.pos, row.getLengthMaxFieldSize());
    }

    /**
     * {inheritDoc}.
     */
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        return getBinaryStream(findColumn(columnLabel));
    }

    /**
     * {inheritDoc}.
     */
    public int getInt(int columnIndex) throws SQLException {
        int actualColumnIndex = columnIndex + columnIndexOffset;
        checkObjectRange(actualColumnIndex);
        return row.getInternalInt(columnsInformation[actualColumnIndex - 1]);
    }

    /**
     * {inheritDoc}.
     */
    public int getInt(String columnLabel) throws SQLException {
        return getInt(findColumn(columnLabel));
    }

    /**
     * {inheritDoc}.
     */
    public long getLong(String columnLabel) throws SQLException {
        return getLong(findColumn(columnLabel));
    }

    /**
     * {inheritDoc}.
     */
    public long getLong(int columnIndex) throws SQLException {
        int actualColumnIndex = columnIndex + columnIndexOffset;
        checkObjectRange(actualColumnIndex);
        return row.getInternalLong(columnsInformation[actualColumnIndex - 1]);
    }

    /**
     * {inheritDoc}.
     */
    public float getFloat(String columnLabel) throws SQLException {
        return getFloat(findColumn(columnLabel));
    }

    /**
     * {inheritDoc}.
     */
    public float getFloat(int columnIndex) throws SQLException {
        int actualColumnIndex = columnIndex + columnIndexOffset;
        checkObjectRange(actualColumnIndex);
        ColumnType type = columnsInformation[actualColumnIndex - 1].getColumnType();
        switch (type) {
            case BINARY_DOUBLE:
                return (float) getDouble(columnIndex);
            default:
                break;
        }
        return row.getInternalFloat(columnsInformation[actualColumnIndex - 1]);
    }

    /**
     * {inheritDoc}.
     */
    public double getDouble(String columnLabel) throws SQLException {
        return getDouble(findColumn(columnLabel));
    }

    /**
     * {inheritDoc}.
     */
    public double getDouble(int columnIndex) throws SQLException {
        int actualColumnIndex = columnIndex + columnIndexOffset;
        checkObjectRange(actualColumnIndex);
        return row.getInternalDouble(columnsInformation[actualColumnIndex - 1]);
    }

    /**
     * {inheritDoc}.
     */
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        return getBigDecimal(findColumn(columnLabel), scale);
    }

    /**
     * {inheritDoc}.
     */
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        int actualColumnIndex = columnIndex + columnIndexOffset;
        checkObjectRange(actualColumnIndex);
        return row.getInternalBigDecimal(columnsInformation[actualColumnIndex - 1]);
    }

    /**
     * {inheritDoc}.
     */
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        int actualColumnIndex = columnIndex + columnIndexOffset;
        checkObjectRange(actualColumnIndex);
        return row.getInternalBigDecimal(columnsInformation[actualColumnIndex - 1]);
    }

    /**
     * {inheritDoc}.
     */
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return getBigDecimal(findColumn(columnLabel));
    }

    /**
     * {inheritDoc}.
     */
    public byte[] getBytes(String columnLabel) throws SQLException {
        return getBytes(findColumn(columnLabel));
    }

    /**
     * {inheritDoc}.
     */
    public byte[] getBytes(int columnIndex) throws SQLException {
        int actualColumnIndex = columnIndex + columnIndexOffset;
        checkObjectRange(actualColumnIndex);
        if (row.lastValueWasNull()) {
            return null;
        }
        if (columnsInformation[actualColumnIndex - 1].getColumnType() == ColumnType.ORA_CLOB) {
            throw new SQLFeatureNotSupportedException();
        }
        if (columnsInformation[actualColumnIndex - 1].getColumnType() == ColumnType.ORA_BLOB) {
            java.sql.Blob blob = getBlob(columnIndex);
            if (blob == null) {
                return null;
            }
            long len = ((com.oceanbase.jdbc.Blob) blob).length();
            return blob.getBytes(1, (int) len);
        }
        if (columnsInformation[actualColumnIndex - 1].getColumnType().getType() == ColumnType.TIMESTAMP_LTZ
            .getType()) {
            TIMESTAMPLTZ timestampltz = getTIMESTAMPLTZ(columnIndex);
            if (timestampltz == null) {
                return null;
            }
            return timestampltz.getBytes();
        }
        if (columnsInformation[actualColumnIndex - 1].getColumnType().getType() == ColumnType.TIMESTAMP_TZ
            .getType()) {
            TIMESTAMPTZ timestamptz = getTIMESTAMPTZ(columnIndex);
            if (timestamptz == null) {
                return null;
            }
            return timestamptz.getBytes();
        }
        if (columnsInformation[actualColumnIndex - 1].getColumnType().getType() == ColumnType.INTERVALYM
            .getType()) {
            INTERVALYM intervalym = getINTERVALYM(columnIndex);
            if (intervalym == null) {
                return null;
            }
            return intervalym.getBytes();
        }
        if (columnsInformation[actualColumnIndex - 1].getColumnType().getType() == ColumnType.INTERVALDS
            .getType()) {
            INTERVALDS intervalds = getINTERVALDS(columnIndex);
            if (intervalds == null) {
                return null;
            }
            return intervalds.getBytes();
        }
        byte[] data = new byte[row.getLengthMaxFieldSize()];
        System.arraycopy(row.buf, row.pos, data, 0, row.getLengthMaxFieldSize());
        return data;
    }

    /**
     * {inheritDoc}.
     */
    public Date getDate(int columnIndex) throws SQLException {
        int actualColumnIndex = columnIndex + columnIndexOffset;
        checkObjectRange(actualColumnIndex);
        return row.getInternalDate(columnsInformation[actualColumnIndex - 1], null, timeZone);
    }

    /**
     * {inheritDoc}.
     */
    public Date getDate(String columnLabel) throws SQLException {
        return getDate(findColumn(columnLabel));
    }

    /**
     * {inheritDoc}.
     */
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        int actualColumnIndex = columnIndex + columnIndexOffset;
        checkObjectRange(actualColumnIndex);
        return row.getInternalDate(columnsInformation[actualColumnIndex - 1], cal, timeZone);
    }

    /**
     * {inheritDoc}.
     */
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        return getDate(findColumn(columnLabel), cal);
    }

    /**
     * {inheritDoc}.
     */
    public Time getTime(int columnIndex) throws SQLException {
        int actualColumnIndex = columnIndex + columnIndexOffset;
        checkObjectRange(actualColumnIndex);
        return row.getInternalTime(columnsInformation[actualColumnIndex - 1], null, timeZone);
    }

    /**
     * {inheritDoc}.
     */
    public Time getTime(String columnLabel) throws SQLException {
        return getTime(findColumn(columnLabel));
    }

    /**
     * {inheritDoc}.
     */
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        int actualColumnIndex = columnIndex + columnIndexOffset;
        checkObjectRange(actualColumnIndex);
        return row.getInternalTime(columnsInformation[actualColumnIndex - 1], cal, timeZone);
    }

    /**
     * {inheritDoc}.
     */
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        return getTime(findColumn(columnLabel), cal);
    }

    public TIMESTAMP getTIMESTAMP(String columnLabel) throws SQLException {
        return getTIMESTAMP(findColumn(columnLabel));
    }

    public TIMESTAMP getTIMESTAMP(int columnIndex) throws SQLException {
        int actualColumnIndex = columnIndex + columnIndexOffset;
        checkObjectRange(actualColumnIndex);
        return row.getInternalTIMESTAMP(columnsInformation[actualColumnIndex - 1], null, timeZone);
    }

    public INTERVALDS getINTERVALDS(String columnLabel) throws SQLException {
        return getINTERVALDS(findColumn(columnLabel));
    }

    @Override
    public NUMBER getNUMBER(int columnIndex) throws SQLException {
        if (protocol.isOracleMode()) {
            checkClose();
        }
        return null;
    }

    @Override
    public NUMBER getNUMBER(String columnName) throws SQLException {
        if (protocol.isOracleMode()) {
            checkClose();
        }
        return null;//todo
    }

    public INTERVALDS getINTERVALDS(int columnIndex) throws SQLException {
        int actualColumnIndex = columnIndex + columnIndexOffset;
        checkObjectRange(actualColumnIndex);
        return row.getInternalINTERVALDS(columnsInformation[actualColumnIndex - 1]);
    }

    public INTERVALYM getINTERVALYM(String columnLabel) throws SQLException {
        return getINTERVALYM(findColumn(columnLabel));
    }

    public INTERVALYM getINTERVALYM(int columnIndex) throws SQLException {
        int actualColumnIndex = columnIndex + columnIndexOffset;
        checkObjectRange(actualColumnIndex);
        return row.getInternalINTERVALYM(columnsInformation[actualColumnIndex - 1]);
    }

    public TIMESTAMPTZ getTIMESTAMPTZ(String columnLabel) throws SQLException {
        return getTIMESTAMPTZ(findColumn(columnLabel));
    }

    public TIMESTAMPTZ getTIMESTAMPTZ(int columnIndex) throws SQLException {
        int actualColumnIndex = columnIndex + columnIndexOffset;
        checkObjectRange(actualColumnIndex);
        return row
            .getInternalTIMESTAMPTZ(columnsInformation[actualColumnIndex - 1], null, timeZone);
    }

    public TIMESTAMPLTZ getTIMESTAMPLTZ(String columnLabel) throws SQLException {
        return getTIMESTAMPLTZ(findColumn(columnLabel));
    }

    public TIMESTAMPLTZ getTIMESTAMPLTZ(int columnIndex) throws SQLException {
        int actualColumnIndex = columnIndex + columnIndexOffset;
        checkObjectRange(actualColumnIndex);
        return row.getInternalTIMESTAMPLTZ(columnsInformation[actualColumnIndex - 1], null,
            timeZone);
    }

    /**
     * {inheritDoc}.
     */
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return getTimestamp(findColumn(columnLabel));
    }

    /**
     * {inheritDoc}.
     */
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        int actualColumnIndex = columnIndex + columnIndexOffset;
        checkObjectRange(actualColumnIndex);
        return row.getInternalTimestamp(columnsInformation[actualColumnIndex - 1], cal, timeZone);
    }

    /**
     * {inheritDoc}.
     */
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        return getTimestamp(findColumn(columnLabel), cal);
    }

    /**
     * {inheritDoc}.
     */
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        int actualColumnIndex = columnIndex + columnIndexOffset;
        checkObjectRange(actualColumnIndex);
        return row.getInternalTimestamp(columnsInformation[actualColumnIndex - 1], null, timeZone);
    }

    /**
     * {inheritDoc}.
     */
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        return getUnicodeStream(findColumn(columnLabel));
    }

    /**
     * {inheritDoc}.
     */
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        int actualColumnIndex = columnIndex + columnIndexOffset;
        checkObjectRange(actualColumnIndex);
        if (row.lastValueWasNull()) {
            return null;
        }
        return new ByteArrayInputStream(new String(row.buf, row.pos, row.getLengthMaxFieldSize(),
            StandardCharsets.UTF_8).getBytes());
    }

    /**
     * {inheritDoc}.
     */
    public String getCursorName() throws SQLException {
        throw ExceptionFactory.INSTANCE.notSupported("Cursors not supported");
    }

    /**
     * {inheritDoc}.
     */
    public ResultSetMetaData getMetaData() throws SQLException {
        checkClose();
        return new OceanBaseResultSetMetaData(columnsInformation, options, forceAlias, this
            .getProtocol().isOracleMode(), columnIndexOffset);
    }

    /**
     * {inheritDoc}.
     */
    public Object getObject(int columnIndex) throws SQLException {
        int actualColumnIndex = columnIndex + columnIndexOffset;
        checkObjectRange(actualColumnIndex);

        ColumnType type = columnsInformation[actualColumnIndex - 1].getColumnType();
        switch (type) {
            case COMPLEX:
                return getComplex(columnIndex);
            case STRUCT:
                return getStruct(columnIndex);
            case ARRAY:
                return getArray(columnIndex);
            case CURSOR:
                return getComplexCursor(columnIndex);
            case ORA_BLOB:
                return getBlob(columnIndex);
            case ORA_CLOB:
                return getClob(columnIndex);
            case ROWID:
                return new RowIdImpl(getString(columnIndex));
            default:
                break;
        }
        return row.getInternalObject(columnsInformation[actualColumnIndex - 1], timeZone);
    }

    /**
     * {inheritDoc}.
     */
    public Object getObject(String columnLabel) throws SQLException {
        return getObject(findColumn(columnLabel));
    }

    /**
     * {inheritDoc}.
     */
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        throw ExceptionFactory.INSTANCE
            .notSupported("Method ResultSet.getObject(int columnIndex, Map<String, Class<?>> map) not supported");
    }

    /**
     * {inheritDoc}.
     */
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        throw ExceptionFactory.INSTANCE
            .notSupported("Method ResultSet.getObject(String columnLabel, Map<String, Class<?>> map) not supported");
    }

    /**
     * {inheritDoc}.
     */
    @SuppressWarnings("unchecked")
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        if (type == null) {
            throw new SQLException("Class type cannot be null");
        }
        int actualColumnIndex = columnIndex + columnIndexOffset;
        checkObjectRange(actualColumnIndex);
        if (row.lastValueWasNull()) {
            return null;
        }
        ColumnDefinition col = columnsInformation[actualColumnIndex - 1];

        if (type.equals(String.class)) {
            if (columnsInformation[actualColumnIndex - 1].getColumnType().getType() == ColumnType.ORA_CLOB
                .getType()) {
                String encoding = this.options.getCharacterEncoding();
                byte[] data = new byte[row.length];
                System.arraycopy(row.buf, row.pos, data, 0, row.length);
                com.oceanbase.jdbc.Clob clob = new com.oceanbase.jdbc.Clob(true, data, encoding,
                    this.statement.getConnection());
                return (T) clob.getSubString(1, (int) clob.length());
            }
            return (T) row.getInternalString(col, null, timeZone);

        } else if (type.equals(Integer.class) || (!protocol.isOracleMode() && type.equals(Integer.TYPE))) {
            return (T) (Integer) row.getInternalInt(col);

        } else if (type.equals(Long.class) || (!protocol.isOracleMode() && type.equals(Long.TYPE))) {
            return (T) (Long) row.getInternalLong(col);

        } else if (type.equals(Short.class) || (!protocol.isOracleMode() && type.equals(Short.TYPE))) {
            // mysql-jdbc 5.x doesn't support, but 8.x supports.
            return (T) (Short) row.getInternalShort(col);

        } else if (type.equals(Double.class) || (!protocol.isOracleMode() && type.equals(Double.TYPE))) {
            return (T) (Double) row.getInternalDouble(col);

        } else if (type.equals(Float.class) || (!protocol.isOracleMode() && type.equals(Float.TYPE))) {
            return (T) (Float) row.getInternalFloat(col);

        } else if (type.equals(Byte.class) || (!protocol.isOracleMode() && type.equals(Byte.TYPE))) {
            // mysql-jdbc 5.x doesn't support, but 8.x supports.
            return (T) (Byte) row.getInternalByte(col);

        } else if (type.equals(byte[].class)) {
            if (col.getColumnType() == ColumnType.ORA_BLOB) {
                String encoding = this.options.getCharacterEncoding();
                if (encoding == null) {
                    encoding = "UTF8";
                }
                byte[] data = new byte[row.length];
                System.arraycopy(row.buf, row.pos, data, 0, row.length);
                Blob blob = new com.oceanbase.jdbc.Blob(true, data, encoding,
                    this.statement.getConnection());
                return (T) blob.getBytes(1, (int) blob.length());
            }
            byte[] data = new byte[row.getLengthMaxFieldSize()];
            System.arraycopy(row.buf, row.pos, data, 0, row.getLengthMaxFieldSize());
            return (T) data;

        } else if (type.equals(Date.class)) {
            return (T) row.getInternalDate(col, null, timeZone);

        } else if (type.equals(Time.class)) {
            return (T) row.getInternalTime(col, null, timeZone);

        } else if (type.equals(Timestamp.class) || type.equals(java.util.Date.class)) {
            return (T) row.getInternalTimestamp(col, null, timeZone);

        } else if (type.equals(Boolean.class) || (!protocol.isOracleMode() && type.equals(Boolean.TYPE))) {
            return (T) (Boolean) row.getInternalBoolean(col);

        } else if (type.equals(Calendar.class)) {
            Calendar calendar = Calendar.getInstance(timeZone);
            Timestamp timestamp = row.getInternalTimestamp(col, null, timeZone);
            if (timestamp == null) {
                return null;
            }
            calendar.setTimeInMillis(timestamp.getTime());
            return type.cast(calendar);

        } else if (type.equals(Clob.class) || type.equals(NClob.class)) {
            if (col.getColumnType() == ColumnType.ORA_CLOB) {
                return (T) getClob(columnIndex);
            } else {
                return (T) new com.oceanbase.jdbc.Clob(row.buf, row.pos, row.getLengthMaxFieldSize());
            }

        } else if (type.equals(InputStream.class)) {
            return (T) new ByteArrayInputStream(row.buf, row.pos, row.getLengthMaxFieldSize());

        } else if (type.equals(Reader.class)) {
            String value = row.getInternalString(col, null, timeZone);
            if (value == null) {
                return null;
            }
            return (T) new StringReader(value);

        } else if (type.equals(BigDecimal.class)) {
            return (T) row.getInternalBigDecimal(col);

        } else if (type.equals(BigInteger.class)) {
            return (T) row.getInternalBigInteger(col);
        } else if (type.equals(BigDecimal.class)) {
            return (T) row.getInternalBigDecimal(col);

        } else if (type.equals(LocalDateTime.class)) {
            ZonedDateTime zonedDateTime = row.getInternalZonedDateTime(col, LocalDateTime.class,
                timeZone);
            return zonedDateTime == null ? null : type.cast(zonedDateTime.withZoneSameInstant(
                ZoneId.systemDefault()).toLocalDateTime());

        } else if (type.equals(ZonedDateTime.class)) {
            ZonedDateTime zonedDateTime = row.getInternalZonedDateTime(col, ZonedDateTime.class,
                    timeZone);
            if (zonedDateTime == null) {
                return null;
            }
            return type.cast(row.getInternalZonedDateTime(col, ZonedDateTime.class, timeZone));

        } else if (type.equals(OffsetDateTime.class)) {
            if (!this.protocol.isOracleMode()) {
                OffsetDateTime offsetDateTime = row.getInternalOffsetDateTime(col, timeZone);
                if (offsetDateTime == null) {
                    return null;
                }
                return type.cast(offsetDateTime);
            } else {
                ZonedDateTime tmpZonedDateTime = row.getInternalZonedDateTime(col,
                        OffsetDateTime.class, timeZone);
                return tmpZonedDateTime == null ? null : type.cast(tmpZonedDateTime
                        .toOffsetDateTime());
            }

        } else if (type.equals(LocalDate.class)) {
            LocalDate localDate = row.getInternalLocalDate(col, timeZone);
            if (localDate == null) {
                return null;
            }
            return type.cast(localDate);

        } else if (type.equals(LocalTime.class)) {
            LocalTime localTime = row.getInternalLocalTime(col, timeZone);
            if (localTime == null) {
                return null;
            }
            return type.cast(localTime);

        } else if (type.equals(OffsetTime.class)) {
            OffsetTime offsetTime = row.getInternalOffsetTime(col, timeZone);
            if (offsetTime == null) {
                return null;
            }
            return type.cast(offsetTime);
        } else if (type.equals(com.oceanbase.jdbc.Blob.class)) {
            if (col.getColumnType() == ColumnType.ORA_BLOB) {
                return (T) getBlob(columnIndex);
            } else {
                return (T) new com.oceanbase.jdbc.Blob(row.buf, row.pos,
                    row.getLengthMaxFieldSize());
            }

        } else if (type.equals(com.oceanbase.jdbc.Clob.class)) {
            if (col.getColumnType() == ColumnType.ORA_CLOB) {
                return (T) getClob(columnIndex);
            } else {
                return (T) new com.oceanbase.jdbc.Clob(row.buf, row.pos,
                    row.getLengthMaxFieldSize());
            }
        } else if (type.equals(TIMESTAMPLTZ.class)) {
            if (col.getColumnType() == ColumnType.TIMESTAMP_LTZ) {
                return (T) getTIMESTAMPLTZ(columnIndex);
            }
        } else if (type.equals(TIMESTAMPTZ.class)) {
            if (col.getColumnType() == ColumnType.TIMESTAMP_TZ) {
                return (T) getTIMESTAMPTZ(columnIndex);
            }
        } else if (type.equals(INTERVALYM.class)) {
            if (col.getColumnType() == ColumnType.INTERVALYM) {
                return (T) getINTERVALYM(columnIndex);
            }
        } else if (type.equals(INTERVALDS.class)) {
            if (col.getColumnType() == ColumnType.INTERVALDS) {
                return (T) getINTERVALDS(columnIndex);
            }
        } else if (options.autoDeserialize) {
            try {
                return type.cast(getObject(columnIndex));
            } catch (ClassCastException classCastException) {
                SQLException exception = new SQLException("Type class '" + type.getName()
                                                          + "' is not supported");
                exception.initCause(classCastException);
                throw exception;
            }
        }
        SQLException exception = new SQLException("Type class '" + type.getName()
                                                  + "' is not supported");
        throw exception;
    }

    @SuppressWarnings("unchecked")
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        return type.cast(getObject(findColumn(columnLabel), type));
    }

    /**
     * {inheritDoc}.
     */
    public int findColumn(String columnLabel) throws SQLException {
        if (!protocol.isOracleMode()) {
            checkClose();
        }
        return columnLabelIndexer.getIndex(columnLabel) - columnIndexOffset + 1;
    }

    /**
     * {inheritDoc}.
     */
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        return getCharacterStream(findColumn(columnLabel));
    }

    /**
     * {inheritDoc}.
     */
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        int actualColumnIndex = columnIndex + columnIndexOffset;
        checkObjectRange(actualColumnIndex);
        if (columnsInformation[actualColumnIndex - 1].getColumnType().getType() == ColumnType.ORA_CLOB
            .getType()) {
            java.sql.Clob clob = getClob(columnIndex);
            if (clob == null) {
                return null;
            } else {
                return clob.getCharacterStream();
            }
        }
        if (columnsInformation[actualColumnIndex - 1].getColumnType().getType() == ColumnType.ORA_BLOB
            .getType()) {
            java.sql.Blob blob = getBlob(columnIndex);
            if (blob == null) {
                return null;
            } else {
                return ((com.oceanbase.jdbc.Blob) blob).getCharacterStream();
            }
        }
        String value = row.getInternalString(columnsInformation[actualColumnIndex - 1], null,
            timeZone);
        if (value == null) {
            return null;
        }
        return new StringReader(value);
    }

    /**
     * {inheritDoc}.
     */
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        return getCharacterStream(columnIndex);
    }

    /**
     * {inheritDoc}.
     */
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        return getCharacterStream(findColumn(columnLabel));
    }

    /**
     * {inheritDoc}.
     */
    public Ref getRef(int columnIndex) throws SQLException {
        throw ExceptionFactory.INSTANCE.notSupported("Getting REFs not supported");
    }

    /**
     * {inheritDoc}.
     */
    public Ref getRef(String columnLabel) throws SQLException {
        throw ExceptionFactory.INSTANCE.notSupported("Getting REFs not supported");
    }

    /**
     * {inheritDoc}.
     */
    public java.sql.Blob getBlob(int columnIndex) throws SQLException {
        int actualColumnIndex = columnIndex + columnIndexOffset;
        checkObjectRange(actualColumnIndex);
        if (row.lastValueWasNull()) {
            return null;
        }
        String encoding = this.options.getCharacterEncoding();
        if (encoding == null) {
            encoding = "UTF8";
        }
        byte[] data = new byte[row.length];
        System.arraycopy(row.buf, row.pos, data, 0, row.length);
        if (columnsInformation[actualColumnIndex - 1].getColumnType().getType() == ColumnType.ORA_BLOB
            .getType()) {
            return new com.oceanbase.jdbc.Blob(true, data, encoding, this.statement.getConnection());

        } else {
            return new com.oceanbase.jdbc.Blob(row.buf, row.pos, row.length);
        }
    }

    /**
     * {inheritDoc}.
     */
    public java.sql.Blob getBlob(String columnLabel) throws SQLException {
        return getBlob(findColumn(columnLabel));
    }

    /**
     * {inheritDoc}.
     */
    public java.sql.Clob getClob(int columnIndex) throws SQLException {
        int actualColumnIndex = columnIndex + columnIndexOffset;
        checkObjectRange(actualColumnIndex);
        if (row.lastValueWasNull()) {
            return null;
        }
        String encoding = this.options.getCharacterEncoding();
        byte[] data = new byte[row.length];
        System.arraycopy(row.buf, row.pos, data, 0, row.length);
        if (columnsInformation[actualColumnIndex - 1].getColumnType() == ColumnType.ORA_CLOB) {
            return new com.oceanbase.jdbc.Clob(true, data, encoding, this.statement.getConnection());
        } else {
            return new com.oceanbase.jdbc.Clob(false, data, encoding, this.statement.getConnection());
        }
    }

    /**
     * {inheritDoc}.
     */
    public Clob getClob(String columnLabel) throws SQLException {
        return getClob(findColumn(columnLabel));
    }

    public void setComplexData(ComplexData[] complexData) {
        this.complexData = complexData;
    }

    public Array getArray(int columnIndex) throws SQLException {
        if (!this.getProtocol().isOracleMode()) {
            throw ExceptionFactory.INSTANCE.notSupported("Arrays not supported");
        }
        int actualColumnIndex = columnIndex + columnIndexOffset;
        checkObjectRange(actualColumnIndex);
        int internalColumnIndex = actualColumnIndex - 1;
        Connection conn = this.statement.getConnection(); // fixme to be the complex connection
        if (this.columnsInformation[internalColumnIndex].getColumnType() == ColumnType.COMPLEX) {
            String typeName = this.columnsInformation[internalColumnIndex].getComplexTypeName();
            ComplexDataType type = ((OceanBaseConnection) conn).getComplexDataType(typeName);
            if (type.getType() != ComplexDataType.TYPE_COLLECTION) {
                throw new SQLException("the field complex type is not TYPE_COLLECTION");
            }
            Array ret = row.getInternalArray(columnsInformation[actualColumnIndex - 1], type,conn);
            complexEndPos[actualColumnIndex - 1] = row.pos;
            return ret;
        } else {
            throw new SQLException("the field type is not FIELD_TYPE_COMPLEX");
        }
    }

    public Object getComplex(int columnIndex) throws SQLException {
        if (!this.getProtocol().isOracleMode()) {
            throw ExceptionFactory.INSTANCE.notSupported("Arrays not supported");
        }
        int actualColumnIndex = columnIndex + columnIndexOffset;
        checkObjectRange(actualColumnIndex);
        if (row.lastValueWasNull()) {
            return null;
        }
        int internalColumnIndex = actualColumnIndex - 1;
        Connection conn = this.statement.getConnection(); // fixme to be the complex connection
        if (this.columnsInformation[internalColumnIndex].getColumnType() == ColumnType.COMPLEX) {
            String typeName = this.columnsInformation[internalColumnIndex].getComplexTypeName();
            ComplexDataType type = ((OceanBaseConnection) conn).getComplexDataType(typeName);
            Object ret = null;
            if (type.getType() == ComplexDataType.TYPE_COLLECTION) {
                ret = row.getInternalArray(columnsInformation[actualColumnIndex - 1], type,conn);
                complexEndPos[actualColumnIndex - 1] = row.pos;
            } else if (type.getType() == ComplexDataType.TYPE_OBJECT) {
                ret = row.getInternalStruct(columnsInformation[actualColumnIndex - 1], type,conn);
                complexEndPos[actualColumnIndex - 1] = row.pos;
            }
            return ret;
        } else {
            throw new SQLException("the field type is not FIELD_TYPE_COMPLEX");
        }
    }

    public Array getArray(String columnLabel) throws SQLException {
        return getArray(findColumn(columnLabel));
    }

    public Struct getStruct(int columnIndex) throws SQLException {
        int actualColumnIndex = columnIndex + columnIndexOffset;
        checkObjectRange(actualColumnIndex);
        if (row.lastValueWasNull()) {
            return null;
        }
        int internalColumnIndex = actualColumnIndex - 1;
        Connection conn = this.statement.getConnection(); // fixme to be the complex connection
        if (this.columnsInformation[internalColumnIndex].getColumnType() == ColumnType.COMPLEX) {
            String typeName = this.columnsInformation[internalColumnIndex].getComplexTypeName();
            ComplexDataType type = ((OceanBaseConnection) conn).getComplexDataType(typeName);
            if (type.getType() != ComplexDataType.TYPE_OBJECT) {
                throw new SQLException("the field complex type is not TYPE_COLLECTION");
            }
            Struct ret = row.getInternalStruct(columnsInformation[actualColumnIndex - 1], type,conn);
            complexEndPos[actualColumnIndex - 1] = row.pos;
            return ret;
        } else {
            throw new SQLException("the field type is not FIELD_TYPE_COMPLEX");
        }
    }

    public Struct getStruct(String columnLabel) throws SQLException {
        return getStruct(findColumn(columnLabel));
    }

    public RefCursor getComplexCursor(int columnIndex) throws SQLException {
        // after reading the eof packet,the protocol been seted to null,but we need it
        int actualColumnIndex = columnIndex + columnIndexOffset;
        checkObjectRange(actualColumnIndex);
        if (row.lastValueWasNull()) {
            return null;
        }
        Connection conn = this.statement.getConnection();
        int internalColumnIndex = actualColumnIndex - 1;
        String typeName = this.columnsInformation[internalColumnIndex].getComplexTypeName();
        ComplexDataType type = new ComplexDataType(typeName, typeName, ColumnType.CURSOR.getType());
        if (complexData[internalColumnIndex] == null) {
            ComplexData data = (ComplexData) row.getInternalComplexCursor(
                columnsInformation[internalColumnIndex], type,conn);
            complexData[internalColumnIndex] = data;
            complexEndPos[actualColumnIndex - 1] = row.pos;
        }

        Object[] objects = ((ComplexData) (complexData[internalColumnIndex])).getAttrData();
        RowObCursorData obCursorData = (RowObCursorData) objects[0];
        if (obCursorData != null && !obCursorData.isOpen()) {
            throw new SQLException("cursor is not open");
        }
        Results results = this.statement.getResults();
        RefCursor obCursor = obCursorData.getObCursor();
        if (obCursor == null) {
            try {
                obCursor = new RefCursor(columnsInformation, results, protocol, false, false,
                    false, obCursorData);
                obCursorData.setObCursor(obCursor);
            } catch (IOException e) {
                throw new SQLException("io exception:" + e.getMessage());
            }
        }
        return obCursor;
    }

    /**
     * {inheritDoc}.
     */
    @Override
    public URL getURL(int columnIndex) throws SQLException {
        int actualColumnIndex = columnIndex + columnIndexOffset;
        checkObjectRange(actualColumnIndex);
        if (row.lastValueWasNull()) {
            return null;
        }
        try {
            return new URL(row.getInternalString(columnsInformation[actualColumnIndex - 1], null,
                timeZone));
        } catch (MalformedURLException e) {
            throw ExceptionFactory.INSTANCE.create("Could not parse as URL");
        }
    }

    /**
     * {inheritDoc}.
     */
    @Override
    public URL getURL(String columnLabel) throws SQLException {
        return getURL(findColumn(columnLabel));
    }

    /**
     * {inheritDoc}.
     */
    public RowId getRowId(int columnIndex) throws SQLException {
        return new RowIdImpl(getString(columnIndex));
    }

    /**
     * {inheritDoc}.
     */
    public RowId getRowId(String columnLabel) throws SQLException {
        return getRowId(findColumn(columnLabel));
    }

    /**
     * {inheritDoc}.
     */
    public NClob getNClob(int columnIndex) throws SQLException {
        int actualColumnIndex = columnIndex + columnIndexOffset;
        checkObjectRange(actualColumnIndex);
        if (row.lastValueWasNull()) {
            return null;
        }
        return new JDBC4NClob(getString(columnIndex), null);
    }

    /**
     * {inheritDoc}.
     */
    public NClob getNClob(String columnLabel) throws SQLException {
        return getNClob(findColumn(columnLabel));
    }

    /**
     * {inheritDoc}.
     */
    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        throw ExceptionFactory.INSTANCE.notSupported("SQLXML not supported");
    }

    /**
     * {inheritDoc}.
     */
    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        throw ExceptionFactory.INSTANCE.notSupported("SQLXML not supported");
    }

    /**
     * {inheritDoc}.
     */
    public String getNString(int columnIndex) throws SQLException {
        return getString(columnIndex);
    }

    /**
     * {inheritDoc}.
     */
    public String getNString(String columnLabel) throws SQLException {
        return getString(findColumn(columnLabel));
    }

    /**
     * {inheritDoc}.
     */
    public boolean getBoolean(int columnIndex) throws SQLException {
        int actualColumnIndex = columnIndex + columnIndexOffset;
        checkObjectRange(actualColumnIndex);
        return row.getInternalBoolean(columnsInformation[actualColumnIndex - 1]);
    }

    /**
     * {inheritDoc}.
     */
    public boolean getBoolean(String columnLabel) throws SQLException {
        return getBoolean(findColumn(columnLabel));
    }

    /**
     * {inheritDoc}.
     */
    public byte getByte(int columnIndex) throws SQLException {
        int actualColumnIndex = columnIndex + columnIndexOffset;
        checkObjectRange(actualColumnIndex);
        return row.getInternalByte(columnsInformation[actualColumnIndex - 1]);
    }

    /**
     * {inheritDoc}.
     */
    public byte getByte(String columnLabel) throws SQLException {
        return getByte(findColumn(columnLabel));
    }

    /**
     * {inheritDoc}.
     */
    public short getShort(int columnIndex) throws SQLException {
        int actualColumnIndex = columnIndex + columnIndexOffset;
        checkObjectRange(actualColumnIndex);
        return row.getInternalShort(columnsInformation[actualColumnIndex - 1]);
    }

    /**
     * {inheritDoc}.
     */
    public short getShort(String columnLabel) throws SQLException {
        return getShort(findColumn(columnLabel));
    }

    /****************************** updatable  characters *******************************/

    private void handelUpdatable(Results results) throws SQLException {
        if (resultSetConcurType == ResultSet.CONCUR_UPDATABLE) {
            updatableColumnLength = columnInformationLength - columnIndexOffset;
            updatableColumns = new UpdatableColumnDefinition[updatableColumnLength];

            if (protocol.isOracleMode()) {
                // a constructing rs of oracle mode is to be checked if updatable by server, so there is no need to check all columns
                mustBeUpdatableForOracle();
            } else {
                checkIfUpdatableForMysql(results);
            }

            updatableParameterHolders = new ParameterHolder[updatableColumnLength];
        }
    }

    private void mustBeUpdatableForOracle() {
        database = columnsInformation[0].getDatabase();
        table = columnsInformation[0].getOriginalTable();
        connection = this.statement.getConnection();

        for (int index = columnIndexOffset; index < columnInformationLength; index++) {
            ColumnDefinition columnDefinition = columnsInformation[index];
            updatableColumns[index - columnIndexOffset] = new UpdatableColumnDefinition(
                columnDefinition, false, false, false, false, false);
        }

        canBeUpdate = true;
        canBeInserted = true;
        canBeRefresh = true;
    }

    private void checkIfUpdatableForMysql(Results results) throws SQLException {
        database = null;
        table = null;
        canBeUpdate = true;
        canBeInserted = true;
        canBeRefresh = false;

        // check that resultSet concern one table and database exactly
        for (ColumnDefinition columnDefinition : columnsInformation) {
            if (columnDefinition.getDatabase() == null || columnDefinition.getDatabase().isEmpty()) {
                cannotUpdateInsertRow("The result-set contains fields without any database information");
                return;
            } else if (database == null) {
                database = columnDefinition.getDatabase();
            } else if (!database.equals(columnDefinition.getDatabase())) {
                cannotUpdateInsertRow("The result-set contains more than one database");
                return;
            }

            if (columnDefinition.getOriginalTable() == null
                || columnDefinition.getOriginalTable().isEmpty()) {
                cannotUpdateInsertRow("The result-set contains fields without any table information");
                return;
            } else if (table == null) {
                table = columnDefinition.getOriginalTable();
            } else if (!table.equals(columnDefinition.getOriginalTable())) {
                cannotUpdateInsertRow("The result-set contains fields on different tables");
                return;
            }
        }
        if (database == null) {
            cannotUpdateInsertRow("The result-set does not contain any database information");
            return;
        }
        if (table == null) {
            cannotUpdateInsertRow("The result-set does not contain any table information");
            return;
        }

        // read table metadata
        if (canBeUpdate) {
            if (results.getStatement() != null && results.getStatement().getConnection() != null) {
                connection = results.getStatement().getConnection();
                Statement stmt = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY);
                ResultSet rs;
                rs = stmt.executeQuery("SHOW COLUMNS FROM `" + database + "`.`" + table + "`");

                boolean primaryFound = false;
                while (rs.next()) {
                    // read SHOW COLUMNS informations
                    String fieldName = rs.getString("Field");
                    boolean canBeNull = "YES".equals(rs.getString("Null"));
                    boolean hasDefault = rs.getString("Default") == null;
                    String extra = rs.getString("Extra");
                    boolean generated = extra != null && !extra.isEmpty();
                    boolean autoIncrement = "auto_increment".equals(extra);
                    boolean primary = "PRI".equals(rs.getString("Key"));

                    boolean found = false;
                    // update column information with SHOW COLUMNS additional informations
                    for (int index = columnIndexOffset; !found && index < columnInformationLength; index++) {
                        ColumnDefinition columnDefinition = columnsInformation[index];
                        if (fieldName.equals(columnDefinition.getName())) {
                            updatableColumns[index - columnIndexOffset] = new UpdatableColumnDefinition(
                                    columnDefinition, canBeNull, hasDefault, generated, primary, autoIncrement);
                            found = true;
                            if (primary) {
                                primaryKeyIndicies.add(index - columnIndexOffset);
                            }
                        }
                    }

                    if (primary) {
                        primaryFound = true;
                    }

                    if (!found) {
                        if (primary) {
                            // without primary key in resultSet, update/delete cannot be done,
                            // since query need to be updated/deleted for this unknown identifier
                            // For insert, key is not mandatory in resultSet if automatically generated,
                            // but data cannot be added to rows in adequate format
                            cannotUpdateInsertRow("Primary key field `" + fieldName
                                                  + "` is not in result-set");
                            return;
                        }

                        // check that missing field can be null / have default values / are generated automatically
                        if (!canBeNull && !hasDefault && !generated) {
                            cannotInsertRow("Field `"
                                            + fieldName
                                            + "` is not present in query returning fields and cannot be null");
                        }
                    }
                }
                rs.close();
                stmt.close();

                if (!primaryFound) {
                    // if there is no primary key (UNIQUE key are considered as primary by SHOW COLUMNS),
                    // rows cannot be updated.
                    cannotUpdateInsertRow("Table `" + database + "`.`" + table
                                          + "` has no primary key");
                    return;
                } else {
                    canBeRefresh = true;
                }

                boolean ensureAllColumnHaveMeta = true;
                for (int index = columnIndexOffset; index < columnInformationLength; index++) {
                    if (updatableColumns[index - columnIndexOffset] == null) {
                        // abnormal error : some field in META are not listed in SHOW COLUMNS
                        cannotUpdateInsertRow("Metadata information not available for table `"
                                              + database + "`.`" + table + "`, field `"
                                              + columnsInformation[index].getName() + "`");
                        ensureAllColumnHaveMeta = false;
                    }
                }
                if (ensureAllColumnHaveMeta) {
                    columnsInformation = updatableColumns;
                }
            }
        } else {
            throw new SQLException("abnormal error : connection is null");
        }
    }

    private void cannotUpdateInsertRow(String reason) {
        if (exceptionUpdateMsg == null) {
            exceptionUpdateMsg = "ResultSet cannot be updated. " + reason;
        }
        if (exceptionInsertMsg == null) {
            exceptionInsertMsg = "No row can be inserted. " + reason;
        }
        canBeUpdate = false;
        canBeInserted = false;
        resultSetConcurType = ResultSet.CONCUR_READ_ONLY;
    }

    private void cannotInsertRow(String reason) {
        if (exceptionInsertMsg == null) {
            exceptionInsertMsg = "No row can be inserted. " + reason;
        }
        canBeInserted = false;
    }

    public boolean rowUpdated() throws SQLException {
        if (protocol.isOracleMode()) {
            return false; // compatible with Oracle
        } else {
            throw ExceptionFactory.INSTANCE.notSupported("Detecting row updates are not supported");
        }
    }

    public boolean rowInserted() throws SQLException {
        if (protocol.isOracleMode()) {
            return false; // compatible with Oracle
        } else {
            throw ExceptionFactory.INSTANCE.notSupported("Detecting inserts are not supported");
        }
    }

    public boolean rowDeleted() throws SQLException {
        if (protocol.isOracleMode()) {
            return false; // compatible with Oracle
        } else {
            throw ExceptionFactory.INSTANCE.notSupported("Row deletes are not supported");
        }
    }

    private void closeUpdatable() throws SQLException {
        if (canBeUpdate) {
            SQLException sqlEx = null;

            try {
                if (refreshPreparedStatement != null) {
                    refreshPreparedStatement.close();
                }
            } catch (SQLException ex) {
                sqlEx = ex;
            }
            try {
                if (insertPreparedStatement != null) {
                    insertPreparedStatement.close();
                }
            } catch (SQLException ex) {
                sqlEx = ex;
            }
            try {
                if (deletePreparedStatement != null) {
                    deletePreparedStatement.close();
                }
            } catch (SQLException ex) {
                sqlEx = ex;
            }

            updatableColumns = null;
            updatableParameterHolders = null;

            if (sqlEx != null) {
                throw sqlEx;
            }
        }
    }

    private void checkUpdatable() throws SQLException {
        if (resultSetConcurType == ResultSet.CONCUR_READ_ONLY) {
            throw ExceptionFactory.INSTANCE.notSupported(NOT_UPDATABLE_ERROR);
        }
    }

    private void checkUpdatable(int position) throws SQLException {
        if (resultSetConcurType == ResultSet.CONCUR_READ_ONLY) {
            throw ExceptionFactory.INSTANCE.notSupported(NOT_UPDATABLE_ERROR);
        }

        if (position <= 0 || position > updatableColumnLength) {
            throw new SQLDataException("No such column: " + position, "22023");
        }

        if (state == STATE_STANDARD) {
            state = STATE_UPDATE;
        }
        if (state == STATE_UPDATE) {
            if (rowPointer < 0) {
                throw new SQLDataException("Current position is before the first row", "22023");
            }

            if (rowPointer >= dataSize) {
                throw new SQLDataException("Current position is after the last row", "22023");
            }

            if (!canBeUpdate) {
                throw new SQLException(exceptionUpdateMsg);
            }
        }
        if (state == STATE_INSERT && !canBeInserted) {
            throw new SQLException(exceptionInsertMsg);
        }
    }

    protected boolean checkRefreshable() throws SQLException {
        if (resultSetConcurType == ResultSet.CONCUR_UPDATABLE && state == STATE_INSERT) {
            throw new SQLException("Cannot call refreshRow() when inserting a new row");
        }

        if (protocol.isOracleMode()) {
            if (!isValidRow(rowPointer)) {
                throw new SQLException("Invalid position in ResultSet.");
            }
        } else {
            if (rowPointer < 0) {
                throw new SQLDataException("Current position is before the first row", "22023");
            }
            if (rowPointer >= dataSize) {
                throw new SQLDataException("Current position is after the last row", "22023");
            }
        }

        if (resultSetConcurType == ResultSet.CONCUR_UPDATABLE && !canBeRefresh) {
            return false;
        }
        return true;
    }

    private StringBuilder generateWhereClauseForMysql() {
        StringBuilder whereClause = new StringBuilder(" WHERE ");

        boolean firstPrimary = true;
        for (int pos = 0; pos < updatableColumnLength; pos++) {
            UpdatableColumnDefinition colInfo = updatableColumns[pos];

            if (colInfo.isPrimary()) {
                if (!firstPrimary) {
                    whereClause.append("AND ");
                }
                firstPrimary = false;
                if (protocol.isOracleMode()) {
                    whereClause.append("\"").append(colInfo.getName()).append("\" = ? ");
                } else {
                    whereClause.append("`").append(colInfo.getName()).append("` = ? ");
                }
            }
        }

        return whereClause;
    }

    private StringBuilder generateColumnClause() {
        StringBuilder columnClause = new StringBuilder();

        for (int pos = 0; pos < updatableColumnLength; pos++) {
            UpdatableColumnDefinition colInfo = updatableColumns[pos];

            if (pos != 0) {
                columnClause.append(",");
            }
            if (protocol.isOracleMode()) {
                columnClause.append("\"").append(colInfo.getName()).append("\"");
            } else {
                columnClause.append("`").append(colInfo.getName()).append("`");
            }
        }

        return columnClause;
    }

    public void insertRow() throws SQLException {
        if (!protocol.isOracleMode()) {
            checkClose();
        }
        if (resultSetConcurType == ResultSet.CONCUR_READ_ONLY) {
            throw ExceptionFactory.INSTANCE
                .notSupported("insertRow are not supported when using ResultSet.CONCUR_READ_ONLY");
        }

        if (protocol != null) {
            protocol.startCallInterface();
        }

        if (state == STATE_INSERT) {
            if (insertPreparedStatement == null) {
                // Create query will all field with WHERE clause contain primary field.
                // if field are not updated, value DEFAULT will be set
                // if field has no default, then insert will throw an exception that will be return to user
                StringBuilder insertSql;
                if (protocol.isOracleMode()) {
                    insertSql = new StringBuilder("INSERT INTO \"" + database + "\".\"" + table
                                                  + "\" ( ");
                } else {
                    insertSql = new StringBuilder("INSERT INTO `" + database + "`.`" + table
                                                  + "` ( ");
                }

                StringBuilder columnClause = generateColumnClause();
                insertSql.append(columnClause).append(") ");

                StringBuilder valueClause = new StringBuilder();
                for (int pos = 0; pos < updatableColumnLength; pos++) {
                    if (pos != 0) {
                        valueClause.append(", ");
                    }
                    valueClause.append("?");
                }
                insertSql.append("VALUES (").append(valueClause).append(")");
                insertPreparedStatement = connection.clientPrepareStatement(insertSql.toString());
            }

            int fieldsIndex = 0;
            for (int pos = 0; pos < updatableColumnLength; pos++) {
                ParameterHolder value = updatableParameterHolders[pos];
                if (value != null) {
                    insertPreparedStatement.setParameter((fieldsIndex++) + 1, value);
                } else {
                    insertPreparedStatement.setParameter((fieldsIndex++) + 1, new DefaultParameter());
                }
            }

            insertPreparedStatement.execute();

            if (!protocol.isOracleMode()) {
                prepareRefreshStmt();
                refreshPreparedStatement.clearParameters();

                for (int i = 0; i < primaryKeyIndicies.size(); i++) {
                    int index = primaryKeyIndicies.get(i);
                    if (updatableColumns[index].isAutoIncrement()) {
                        long autoIncrementId = ((CmdInformationSingle)insertPreparedStatement.results.getCmdInformation()).getInsertId();
                        if (autoIncrementId > 0) {
                            refreshPreparedStatement.setObject(i + 1, autoIncrementId, updatableColumns[index].getColumnType().getSqlType());
                        }
                    } else {
                        ((BasePrepareStatement)refreshPreparedStatement).setParameter(i + 1, updatableParameterHolders[index]);
                    }
                }

                SelectResultSet rs = (SelectResultSet) refreshPreparedStatement.executeQuery();
                // update row data only if not deleted externally
                if (rs.next()) {
                    addRowData(rs.getCurrentRowData());
                    rs.close();
                } else {
                    throw new SQLException(UpdatableResultSet_12);
                }
            }

            Arrays.fill(updatableParameterHolders, null);
        }

        if (protocol != null) {
            protocol.endCallInterface("JDBC4ResultSet.insertRow");
        }
    }

    public void updateRow() throws SQLException {
        if (resultSetConcurType == ResultSet.CONCUR_READ_ONLY) {
            throw ExceptionFactory.INSTANCE
                .notSupported("updateRow are not supported when using ResultSet.CONCUR_READ_ONLY");
        }

        if (state == STATE_INSERT) {
            throw new SQLException("Cannot call updateRow() when inserting a new row");
        }

        if (protocol != null) {
            protocol.startCallInterface();
        }

        if (state == STATE_UPDATE) {
            // state is STATE_UPDATE, meaning that at least one field is modified, update query can be run.
            // Construct UPDATE query according to modified field only
            StringBuilder updateSql, whereClause;
            if (protocol.isOracleMode()) {
                updateSql = new StringBuilder("UPDATE \"" + database + "\".\"" + table + "\" SET ");
                whereClause = new StringBuilder(" WHERE ROWID = ?");
            } else {
                updateSql = new StringBuilder("UPDATE `" + database + "`.`" + table + "` SET ");
                whereClause = generateWhereClauseForMysql();
            }

            boolean firstUpdate = true;
            int fieldsToUpdate = 0;
            for (int pos = 0; pos < updatableColumnLength; pos++) {
                UpdatableColumnDefinition colInfo = updatableColumns[pos];
                ParameterHolder value = updatableParameterHolders[pos];

                if (value != null) {
                    if (!firstUpdate) {
                        updateSql.append(",");
                    }
                    firstUpdate = false;
                    fieldsToUpdate++;
                    if (protocol.isOracleMode()) {
                        updateSql.append("\"").append(colInfo.getName()).append("\" = ? ");
                    } else {
                        updateSql.append("`").append(colInfo.getName()).append("` = ? ");
                    }
                }
            }
            updateSql.append(whereClause);

            ClientSidePreparedStatement preparedStatement = connection
                .clientPrepareStatement(updateSql.toString());
            int fieldsIndex = 0;
            int fieldsPrimaryIndex = 0;
            for (int pos = 0; pos < updatableColumnLength; pos++) {
                ParameterHolder value = updatableParameterHolders[pos];
                if (value != null) {
                    preparedStatement.setParameter((fieldsIndex++) + 1, value);
                }

                if (!protocol.isOracleMode()) {
                    UpdatableColumnDefinition colInfo = updatableColumns[pos];
                    if (colInfo.isPrimary()) {
                        preparedStatement.setObject(fieldsToUpdate + (fieldsPrimaryIndex++) + 1,
                            getObject(pos + 1), colInfo.getColumnType().getSqlType());
                    }
                }
            }
            if (protocol.isOracleMode()) {
                int rowidColumnIndex = this.statement.isAddRowid() ? 0 : 1;
                preparedStatement.setRowId(fieldsToUpdate + 1, getRowId(rowidColumnIndex));
            }
            preparedStatement.execute();
            preparedStatement.close();

            state = STATE_UPDATED;

            if (protocol.isOracleMode()) {
                // TODO:  && resultSetScrollType != ResultSet.TYPE_FORWARD_ONLY
                refreshRowInternalOracle(1);
            } else {
                refreshRow();
            }
            //            if (!protocol.isOracleMode()) {
            //                refreshRow();
            //            } else if (resultSetScrollType != ResultSet.TYPE_FORWARD_ONLY) {
            //                refreshRowInternalOracle(1);
            //            }

            Arrays.fill(updatableParameterHolders, null);
            state = STATE_STANDARD;
        }

        if (protocol != null) {
            protocol.endCallInterface("JDBC4ResultSet.updateRow");
        }
    }

    public void deleteRow() throws SQLException {
        if (!protocol.isOracleMode()) {
            checkClose();
        }
        if (resultSetConcurType == ResultSet.CONCUR_READ_ONLY) {
            throw ExceptionFactory.INSTANCE
                .notSupported("deleteRow are not supported when using ResultSet.CONCUR_READ_ONLY");
        }

        if (state == STATE_INSERT) {
            throw new SQLException("Cannot call deleteRow() when inserting a new row");
        }
        if (!canBeUpdate) {
            throw new SQLDataException(exceptionUpdateMsg);
        }
        if (rowPointer < 0) {
            throw new SQLDataException("Current position is before the first row", "22023");
        }
        if (rowPointer >= dataSize) {
            throw new SQLDataException("Current position is after the last row", "22023");
        }

        if (protocol != null) {
            protocol.startCallInterface();
        }

        if (deletePreparedStatement == null) {
            // Create query with WHERE clause contain primary field.
            StringBuilder deleteSql;
            if (protocol.isOracleMode()) {
                deleteSql = new StringBuilder("DELETE FROM \"" + database + "\".\"" + table
                                              + "\" WHERE ROWID = ?");
            } else {
                deleteSql = new StringBuilder("DELETE FROM `" + database + "`.`" + table + "` ");
                StringBuilder whereClause = generateWhereClauseForMysql();
                deleteSql.append(whereClause);
            }

            deletePreparedStatement = connection.clientPrepareStatement(deleteSql.toString());
        }

        if (protocol.isOracleMode()) {
            int rowidColumnIndex = this.statement.isAddRowid() ? 0 : 1;
            deletePreparedStatement.setRowId(1, getRowId(rowidColumnIndex));
        } else {
            int fieldsPrimaryIndex = 1;
            for (int pos = 0; pos < updatableColumnLength; pos++) {
                UpdatableColumnDefinition colInfo = updatableColumns[pos];
                if (colInfo.isPrimary()) {
                    deletePreparedStatement.setObject(fieldsPrimaryIndex++, getObject(pos + 1),
                        colInfo.getColumnType().getSqlType());
                }
            }
        }

        deletePreparedStatement.executeUpdate();

        if (!protocol.isOracleMode() || resultSetScrollType != ResultSet.TYPE_FORWARD_ONLY) {
            deleteCurrentRowData();
        }

        if (protocol != null) {
            protocol.endCallInterface("JDBC4ResultSet.deleteRow");
        }
    }

    /**
     * SQLException –
     *  if a database access error occurs;
     *  this method is called on a closed result set;
     *  the result set type is TYPE_FORWARD_ONLY, or (TYPE_SCROLL_INSENSITIVE and CONCUR_READ_ONLY) for Oracle mode
     *  the result set type is CONCUR_READ_ONLY for MySQL mode
     *  or if this method is called when the cursor is on the insert row
     *
     * support FETCH_FORWARD direction for Oracle mode now
     */
    public void refreshRow() throws SQLException {
        if (!protocol.isOracleMode()) {
            checkClose();
        }
        if ((protocol.isOracleMode() && (resultSetScrollType == ResultSet.TYPE_FORWARD_ONLY || (resultSetScrollType == ResultSet.TYPE_SCROLL_INSENSITIVE && resultSetConcurType == ResultSet.CONCUR_READ_ONLY)))
            || (!protocol.isOracleMode() && resultSetConcurType == ResultSet.CONCUR_READ_ONLY)) {
            throw ExceptionFactory.INSTANCE.notSupported("refreshRow are not supported when using "
                                                         + resultSetScrollType + " and "
                                                         + resultSetConcurType);
        }
        if (!checkRefreshable()) {
            return;
        }

        if (protocol != null) {
            protocol.startCallInterface();
        }

        if (protocol.isOracleMode()) {
            refreshRowInternalOracle(fetchSize);
        } else {
            updateRowData(refreshRowInternalMysql());
        }

        if (protocol != null) {
            protocol.endCallInterface("JDBC4ResultSet.refreshRow");
        }
    }

    private void prepareRefreshStmt() throws SQLException {
        if (refreshPreparedStatement == null) {
            // Construct SELECT query according to column metadata, with WHERE part containing primary fields
            StringBuilder selectSql = new StringBuilder("SELECT ");

            StringBuilder columnClause = generateColumnClause();
            selectSql.append(columnClause);

            StringBuilder whereClause = generateWhereClauseForMysql();
            selectSql.append(" FROM `").append(database).append("`.`").append(table).append("`")
                .append(whereClause);

            // row's raw bytes must be encoded according to current resultSet type
            // Create Server or Client PrepareStatement accordingly
            if (isBinaryEncoded()) {
                refreshPreparedStatement = connection.serverPrepareStatement(selectSql.toString());
            } else {
                refreshPreparedStatement = connection.clientPrepareStatement(selectSql.toString());
            }
        }
    }

    private byte[] refreshRowInternalMysql() throws SQLException {
        prepareRefreshStmt();
        int fieldsPrimaryIndex = 1;
        for (int pos = 0; pos < updatableColumnLength; pos++) {
            UpdatableColumnDefinition colInfo = updatableColumns[pos];
            if (colInfo.isPrimary()) {
                ParameterHolder value = updatableParameterHolders[pos];

                if (state != STATE_STANDARD && value != null) {
                    // Row has just been updated using updateRow() methods.
                    // updateRow has changed primary key, must use the new value.
                    if (isBinaryEncoded()) {
                        ((ServerSidePreparedStatement) refreshPreparedStatement).setParameter(
                            fieldsPrimaryIndex++, value);
                    } else {
                        ((ClientSidePreparedStatement) refreshPreparedStatement).setParameter(
                            fieldsPrimaryIndex++, value);
                    }
                } else {
                    refreshPreparedStatement.setObject(fieldsPrimaryIndex++, getObject(pos + 1),
                        colInfo.getColumnType().getSqlType());
                }
            }
        }

        SelectResultSet rs = (SelectResultSet) refreshPreparedStatement.executeQuery();

        // update row data only if not deleted externally
        if (rs.next()) {
            byte[] rowData = rs.getCurrentRowData();
            rs.close();
            return rowData;
        } else {
            throw new SQLException(UpdatableResultSet_12);
        }
    }

    protected int refreshRowInternalOracle(int size) throws SQLException {
        int rowidColumnIndex = 0;
        int curRowPointer = rowPointer;

        // get refetch size, refresh [currentRow, currentRow+size]
        int refetchSize = 0;
        RowId[] refetchRowids = new RowId[size];
        while (refetchSize < size && isValidRow(rowPointer)) {
            refetchRowids[refetchSize++] = getRowId(rowidColumnIndex);
            rowPointer++;
        }
        rowPointer = curRowPointer;

        if (refetchSize > 0) {
            // prepare refetch statement
            PreparedStatement pstmt = this.statement.getConnection().prepareStatement(
                getRefetchSql(refetchSize));

            // prepare refetch binds
            int userParamCount = ((BasePrepareStatement) (this.statement)).getParameterCount();
            ((BasePrepareStatement) pstmt).setParameterCount(refetchSize + userParamCount);
            ((BasePrepareStatement) pstmt).setParameters(((BasePrepareStatement) (this.statement))
                .getParameters());
            for (int i = 0; i < refetchSize; i++) {
                pstmt.setRowId(userParamCount + i + 1, refetchRowids[i]);
            }
            ResultSet refetchRs = pstmt.executeQuery();

            // save refetch results
            while (refetchRs.next()) {
                RowId freshRowid = refetchRs.getRowId(1);
                boolean find = false;
                while (!find && rowPointer < curRowPointer + refetchSize) {
                    if (Arrays.equals(getRowId(rowidColumnIndex).getBytes(), freshRowid.getBytes())) {
                        find = true;
                    } else {
                        rowPointer++;
                    }
                }

                if (find) {
                    if (!Arrays.equals(data[rowPointer],
                        ((JDBC4ResultSet) refetchRs).getCurrentRowData())) {
                        data[rowPointer] = Arrays.copyOf(
                            ((JDBC4ResultSet) refetchRs).getCurrentRowData(),
                            ((JDBC4ResultSet) refetchRs).getCurrentRowData().length);
                        isModified = true;
                    }
                }
                rowPointer = curRowPointer;
            }

            refetchRs.close();
            pstmt.close();
        }
        return refetchSize;
    }

    protected boolean isValidRow(int rowIndex) throws SQLException {
        if (rowIndex >= 0 && rowIndex < dataSize) {
            return true;
        }
        if (rowIndex < 0) {
            return false;
        }

        while (rowIndex >= dataSize && this.next()) {
            // get rows afterwards current row from server
            rowPointer = rowIndex;
        }
        return rowIndex < dataSize;
    }

    private String getRefetchSql(int refetchSize) {
        StringBuilder sb = new StringBuilder(this.statement.getActualSql());
        sb.append(this.statement.getWhereEndPos() == -1 ? " WHERE (ROWID=?" : " AND (ROWID=?");

        for (int i = 0; i < refetchSize - 1; i++) {
            sb.append(" OR ROWID=?");
        }
        sb.append(")");

        return String.valueOf(sb);
    }

    protected void cancelRowInserts() {
        if (state == STATE_INSERT) {
            state = STATE_UPDATE;
            setRowPointer(notInsertRowPointer);
        }
    }

    public void cancelRowUpdates() throws SQLException {
        if (!protocol.isOracleMode()) {
            checkClose();
        }
        if (resultSetConcurType == ResultSet.CONCUR_READ_ONLY) {
            throw ExceptionFactory.INSTANCE.notSupported(NOT_UPDATABLE_ERROR);
        }

        Arrays.fill(updatableParameterHolders, null);
        state = STATE_STANDARD;
    }

    public void moveToInsertRow() throws SQLException {
        if (!protocol.isOracleMode()) {
            checkClose();
        }
        if (resultSetConcurType == ResultSet.CONCUR_READ_ONLY) {
            throw ExceptionFactory.INSTANCE.notSupported(NOT_UPDATABLE_ERROR);
        }

        if (!canBeInserted) {
            throw new SQLException(exceptionInsertMsg);
        }
        Arrays.fill(updatableParameterHolders, null);
        state = STATE_INSERT;
        notInsertRowPointer = rowPointer;
    }

    public void moveToCurrentRow() throws SQLException {
        if (!protocol.isOracleMode()) {
            checkClose();
        }
        if (resultSetConcurType == ResultSet.CONCUR_READ_ONLY) {
            throw ExceptionFactory.INSTANCE.notSupported(NOT_UPDATABLE_ERROR);
        }

        Arrays.fill(updatableParameterHolders, null);
        state = STATE_STANDARD;
        setRowPointer(notInsertRowPointer);
    }

    public void updateNull(int columnIndex) throws SQLException {
        checkUpdatable(columnIndex);
        updatableParameterHolders[columnIndex - 1] = new NullParameter();

        //        if (state != STATE_INSERT && protocol.isOracleMode()) {
        //            checkObjectRange(columnIndex + columnIndexOffset);
        //            updateRowDataOneColumn(NullParameter.getNull());
        //        }
    }

    public void updateNull(String columnLabel) throws SQLException {
        checkUpdatable();
        updateNull(findColumn(columnLabel));
    }

    public void updateBoolean(int columnIndex, boolean bool) throws SQLException {
        checkUpdatable(columnIndex);
        updatableParameterHolders[columnIndex - 1] = new ByteParameter(bool ? (byte) 1 : (byte) 0);

        //        if (state != STATE_INSERT && protocol.isOracleMode()) {
        //            checkObjectRange(columnIndex + columnIndexOffset);
        //            updateRowDataOneColumn(bool ? new byte[]{(byte) 1} : new byte[]{(byte) 0});
        //        }
    }

    public void updateBoolean(String columnLabel, boolean value) throws SQLException {
        checkUpdatable();
        updateBoolean(findColumn(columnLabel), value);
    }

    public void updateByte(int columnIndex, byte value) throws SQLException {
        checkUpdatable(columnIndex);
        updatableParameterHolders[columnIndex - 1] = new ByteParameter(value);

        //        if (state != STATE_INSERT && protocol.isOracleMode()) {
        //            checkObjectRange(columnIndex + columnIndexOffset);
        //            updateRowDataOneColumn(new byte[]{value});
        //        }
    }

    public void updateByte(String columnLabel, byte value) throws SQLException {
        checkUpdatable();
        updateByte(findColumn(columnLabel), value);
    }

    public void updateShort(int columnIndex, short value) throws SQLException {
        checkUpdatable(columnIndex);
        updatableParameterHolders[columnIndex - 1] = new ShortParameter(value);
    }

    public void updateShort(String columnLabel, short value) throws SQLException {
        checkUpdatable();
        updateShort(findColumn(columnLabel), value);
    }

    public void updateInt(int columnIndex, int value) throws SQLException {
        checkUpdatable(columnIndex);
        updatableParameterHolders[columnIndex - 1] = new IntParameter(value);
    }

    public void updateInt(String columnLabel, int value) throws SQLException {
        checkUpdatable();
        updateInt(findColumn(columnLabel), value);
    }

    public void updateFloat(int columnIndex, float value) throws SQLException {
        checkUpdatable(columnIndex);
        updatableParameterHolders[columnIndex - 1] = new FloatParameter(value);
    }

    public void updateFloat(String columnLabel, float value) throws SQLException {
        checkUpdatable();
        updateFloat(findColumn(columnLabel), value);
    }

    public void updateDouble(int columnIndex, double value) throws SQLException {
        checkUpdatable(columnIndex);
        updatableParameterHolders[columnIndex - 1] = new DoubleParameter(value);
    }

    public void updateDouble(String columnLabel, double value) throws SQLException {
        checkUpdatable();
        updateDouble(findColumn(columnLabel), value);
    }

    public void updateBigDecimal(int columnIndex, BigDecimal value) throws SQLException {
        checkUpdatable(columnIndex);
        if (value == null) {
            updatableParameterHolders[columnIndex - 1] = new NullParameter(ColumnType.DECIMAL);
            return;
        }
        updatableParameterHolders[columnIndex - 1] = new BigDecimalParameter(value);
    }

    public void updateBigDecimal(String columnLabel, BigDecimal value) throws SQLException {
        checkUpdatable();
        updateBigDecimal(findColumn(columnLabel), value);
    }

    public void updateString(int columnIndex, String value) throws SQLException {
        checkUpdatable(columnIndex);
        if (value == null) {
            updatableParameterHolders[columnIndex - 1] = new NullParameter(ColumnType.STRING);
            return;
        }
        updatableParameterHolders[columnIndex - 1] = new StringParameter(value, noBackslashEscapes,
            protocol.getOptions().getCharacterEncoding());
    }

    public void updateString(String columnLabel, String value) throws SQLException {
        checkUpdatable();
        updateString(findColumn(columnLabel), value);
    }

    public void updateBytes(int columnIndex, byte[] value) throws SQLException {
        checkUpdatable(columnIndex);
        if (value == null) {
            updatableParameterHolders[columnIndex - 1] = new NullParameter(ColumnType.BLOB);
            return;
        }
        if (protocol.isOracleMode()) {
            updatableParameterHolders[columnIndex - 1] = new OBByteArrayParameter(value,
                noBackslashEscapes);
        } else {
            updatableParameterHolders[columnIndex - 1] = new ByteArrayParameter(value,
                noBackslashEscapes);
        }
    }

    public void updateBytes(String columnLabel, byte[] value) throws SQLException {
        checkUpdatable();
        updateBytes(findColumn(columnLabel), value);
    }

    public void updateDate(int columnIndex, Date date) throws SQLException {
        checkUpdatable(columnIndex);
        if (date == null) {
            updatableParameterHolders[columnIndex - 1] = new NullParameter(ColumnType.DATE);
            return;
        }
        updatableParameterHolders[columnIndex - 1] = new DateParameter(date, TimeZone.getDefault(),
            options);
    }

    public void updateDate(String columnLabel, Date value) throws SQLException {
        checkUpdatable();
        updateDate(findColumn(columnLabel), value);
    }

    public void updateTime(int columnIndex, Time time) throws SQLException {
        checkUpdatable(columnIndex);
        if (time == null) {
            updatableParameterHolders[columnIndex - 1] = new NullParameter(ColumnType.TIME);
            return;
        }
        if (this.connection.getProtocol().isOracleMode()) {
            Timestamp ts = new Timestamp(time.getTime());
            TimeZone tz = this.connection.getProtocol().getTimeZone();
            updatableParameterHolders[columnIndex - 1] = new TimestampParameter(ts, tz,
                options.useFractionalSeconds);
        } else {
            updatableParameterHolders[columnIndex - 1] = new TimeParameter(time,
                TimeZone.getDefault(), options.useFractionalSeconds);
        }

    }

    public void updateTime(String columnLabel, Time value) throws SQLException {
        checkUpdatable();
        updateTime(findColumn(columnLabel), value);
    }

    public void updateTimestamp(int columnIndex, Timestamp timeStamp) throws SQLException {
        checkUpdatable(columnIndex);
        if (timeStamp == null) {
            updatableParameterHolders[columnIndex - 1] = new NullParameter(ColumnType.DATETIME);
            return;
        }
        updatableParameterHolders[columnIndex - 1] = new TimestampParameter(timeStamp, timeZone,
            options.useFractionalSeconds);
    }

    public void updateTimestamp(String columnLabel, Timestamp value) throws SQLException {
        checkUpdatable();
        updateTimestamp(findColumn(columnLabel), value);
    }

    public void updateAsciiStream(int columnIndex, InputStream inputStream) throws SQLException {
        updateAsciiStream(columnIndex, inputStream, Long.MAX_VALUE);
    }

    public void updateAsciiStream(String columnLabel, InputStream inputStream) throws SQLException {
        checkUpdatable();
        updateAsciiStream(findColumn(columnLabel), inputStream);
    }

    public void updateAsciiStream(int columnIndex, InputStream inputStream, int length)
                                                                                       throws SQLException {
        updateAsciiStream(columnIndex, inputStream, (long) length);
    }

    public void updateAsciiStream(String columnLabel, InputStream inputStream, int length)
                                                                                          throws SQLException {
        checkUpdatable();
        updateAsciiStream(findColumn(columnLabel), inputStream, length);
    }

    public void updateAsciiStream(int columnIndex, InputStream inputStream, long length)
                                                                                        throws SQLException {
        checkUpdatable(columnIndex);
        if (inputStream == null) {
            updatableParameterHolders[columnIndex - 1] = new NullParameter(ColumnType.BLOB);
            return;
        }
        if (protocol.isOracleMode()) {
            updatableParameterHolders[columnIndex - 1] = new OBStreamParameter(inputStream, length,
                noBackslashEscapes);
        } else {
            updatableParameterHolders[columnIndex - 1] = new StreamParameter(inputStream, length,
                noBackslashEscapes);
        }
    }

    public void updateAsciiStream(String columnLabel, InputStream inputStream, long length)
                                                                                           throws SQLException {
        checkUpdatable();
        updateAsciiStream(findColumn(columnLabel), inputStream, length);
    }

    public void updateBinaryStream(int columnIndex, InputStream inputStream, int length)
                                                                                        throws SQLException {
        updateBinaryStream(columnIndex, inputStream, (long) length);
    }

    public void updateBinaryStream(int columnIndex, InputStream inputStream, long length)
                                                                                         throws SQLException {
        checkUpdatable(columnIndex);
        if (inputStream == null) {
            updatableParameterHolders[columnIndex - 1] = new NullParameter(ColumnType.BLOB);
            return;
        }
        if (protocol.isOracleMode()) {
            updatableParameterHolders[columnIndex - 1] = new OBStreamParameter(inputStream, length,
                noBackslashEscapes);
        } else {
            updatableParameterHolders[columnIndex - 1] = new StreamParameter(inputStream, length,
                noBackslashEscapes);
        }
    }

    public void updateBinaryStream(String columnLabel, InputStream inputStream, int length)
                                                                                           throws SQLException {
        checkUpdatable();
        updateBinaryStream(findColumn(columnLabel), inputStream, (long) length);
    }

    public void updateBinaryStream(String columnLabel, InputStream inputStream, long length)
                                                                                            throws SQLException {
        checkUpdatable();
        updateBinaryStream(findColumn(columnLabel), inputStream, length);
    }

    public void updateBinaryStream(int columnIndex, InputStream inputStream) throws SQLException {
        updateBinaryStream(columnIndex, inputStream, Long.MAX_VALUE);
    }

    public void updateBinaryStream(String columnLabel, InputStream inputStream) throws SQLException {
        checkUpdatable();
        updateBinaryStream(findColumn(columnLabel), inputStream);
    }

    public void updateCharacterStream(int columnIndex, Reader reader, int length)
                                                                                 throws SQLException {
        updateCharacterStream(columnIndex, reader, (long) length);
    }

    public void updateCharacterStream(int columnIndex, Reader value) throws SQLException {
        updateCharacterStream(columnIndex, value, Long.MAX_VALUE);
    }

    public void updateCharacterStream(String columnLabel, Reader reader, int length)
                                                                                    throws SQLException {
        checkUpdatable();
        updateCharacterStream(findColumn(columnLabel), reader, (long) length);
    }

    public void updateCharacterStream(int columnIndex, Reader value, long length)
                                                                                 throws SQLException {
        checkUpdatable(columnIndex);
        if (value == null) {
            updatableParameterHolders[columnIndex - 1] = new NullParameter(ColumnType.BLOB);
            return;
        }
        updatableParameterHolders[columnIndex - 1] = new ReaderParameter(value, length,
            noBackslashEscapes);
    }

    public void updateCharacterStream(String columnLabel, Reader reader, long length)
                                                                                     throws SQLException {
        checkUpdatable();
        updateCharacterStream(findColumn(columnLabel), reader, length);
    }

    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        checkUpdatable();
        updateCharacterStream(findColumn(columnLabel), reader, Long.MAX_VALUE);
    }

    private void updateInternalObject(final int parameterIndex, final Object obj,
                                      final int targetSqlType, final long scaleOrLength)
                                                                                        throws SQLException {
        checkUpdatable(parameterIndex);

        switch (targetSqlType) {
            case Types.ARRAY:
            case Types.DATALINK:
            case Types.JAVA_OBJECT:
            case Types.REF:
            case Types.ROWID:
            case Types.SQLXML:
            case Types.STRUCT:
                throw ExceptionFactory.INSTANCE.notSupported("Type not supported");
            default:
                break;
        }

        if (obj == null) {
            updateNull(parameterIndex);
        } else if (obj instanceof String) {
            if (targetSqlType == Types.BLOB) {
                throw ExceptionFactory.INSTANCE.create("Cannot convert a String to a Blob");
            }
            String str = (String) obj;
            try {
                switch (targetSqlType) {
                    case Types.BIT:
                    case Types.BOOLEAN:
                        updateBoolean(parameterIndex,
                            !("false".equalsIgnoreCase(str) || "0".equals(str)));
                        break;
                    case Types.TINYINT:
                        updateByte(parameterIndex, Byte.parseByte(str));
                        break;
                    case Types.SMALLINT:
                        updateShort(parameterIndex, Short.parseShort(str));
                        break;
                    case Types.INTEGER:
                        updateInt(parameterIndex, Integer.parseInt(str));
                        break;
                    case Types.DOUBLE:
                    case Types.FLOAT:
                        updateDouble(parameterIndex, Double.valueOf(str));
                        break;
                    case Types.REAL:
                        updateFloat(parameterIndex, Float.valueOf(str));
                        break;
                    case Types.BIGINT:
                        updateLong(parameterIndex, Long.valueOf(str));
                        break;
                    case Types.DECIMAL:
                    case Types.NUMERIC:
                        updateBigDecimal(parameterIndex, new BigDecimal(str));
                        break;
                    case Types.CLOB:
                    case Types.NCLOB:
                    case Types.CHAR:
                    case Types.VARCHAR:
                    case Types.LONGVARCHAR:
                    case Types.NCHAR:
                    case Types.NVARCHAR:
                    case Types.LONGNVARCHAR:
                        updateString(parameterIndex, str);
                        break;
                    case Types.TIMESTAMP:
                        if (str.startsWith("0000-00-00")) {
                            updateTimestamp(parameterIndex, null);
                        } else {
                            updateTimestamp(parameterIndex, Timestamp.valueOf(str));
                        }
                        break;
                    case Types.TIME:
                        updateTime(parameterIndex, Time.valueOf((String) obj));
                        break;
                    case Types.TIME_WITH_TIMEZONE:
                        updatableParameterHolders[parameterIndex - 1] = new OffsetTimeParameter(
                            OffsetTime.parse(str), timeZone.toZoneId(),
                            options.useFractionalSeconds, options);
                        break;
                    case Types.TIMESTAMP_WITH_TIMEZONE:
                        updatableParameterHolders[parameterIndex - 1] = new ZonedDateTimeParameter(
                            ZonedDateTime.parse(str, BasePrepareStatement.SPEC_ISO_ZONED_DATE_TIME),
                            timeZone.toZoneId(), options.useFractionalSeconds, options);
                        break;
                    default:
                        throw ExceptionFactory.INSTANCE.create("Could not convert [" + str
                                                               + "] to " + targetSqlType);
                }
            } catch (IllegalArgumentException e) {
                throw ExceptionFactory.INSTANCE.create("Could not convert [" + str + "] to "
                                                       + targetSqlType, e);
            }
        } else if (obj instanceof Number) {
            Number bd = (Number) obj;
            switch (targetSqlType) {
                case Types.TINYINT:
                    updateByte(parameterIndex, bd.byteValue());
                    break;
                case Types.SMALLINT:
                    updateShort(parameterIndex, bd.shortValue());
                    break;
                case Types.INTEGER:
                    updateInt(parameterIndex, bd.intValue());
                    break;
                case Types.BIGINT:
                    updateLong(parameterIndex, bd.longValue());
                    break;
                case Types.FLOAT:
                case Types.DOUBLE:
                    updateDouble(parameterIndex, bd.doubleValue());
                    break;
                case Types.REAL:
                    updateFloat(parameterIndex, bd.floatValue());
                    break;
                case Types.DECIMAL:
                case Types.NUMERIC:
                    if (obj instanceof BigDecimal) {
                        updateBigDecimal(parameterIndex, (BigDecimal) obj);
                    } else if (obj instanceof Double || obj instanceof Float) {
                        updateDouble(parameterIndex, bd.doubleValue());
                    } else {
                        updateLong(parameterIndex, bd.longValue());
                    }
                    break;
                case Types.BIT:
                    updateBoolean(parameterIndex, bd.shortValue() != 0);
                    break;
                case Types.CHAR:
                case Types.VARCHAR:
                    updateString(parameterIndex, bd.toString());
                    break;
                default:
                    throw ExceptionFactory.INSTANCE.create("Could not convert [" + bd + "] to "
                                                           + targetSqlType);
            }
        } else if (obj instanceof byte[]) {
            if (targetSqlType == Types.BINARY || targetSqlType == Types.VARBINARY
                || targetSqlType == Types.LONGVARBINARY) {
                updateBytes(parameterIndex, (byte[]) obj);
            } else {
                throw ExceptionFactory.INSTANCE
                    .create("Can only convert a byte[] to BINARY, VARBINARY or LONGVARBINARY");
            }
        } else if (obj instanceof Time) {
            updateTime(parameterIndex, (Time) obj); // it is just a string anyway
        } else if (obj instanceof Timestamp) {
            updateTimestamp(parameterIndex, (Timestamp) obj);
        } else if (obj instanceof Date) {
            updateDate(parameterIndex, (Date) obj);
        } else if (obj instanceof java.util.Date) {
            long timemillis = ((java.util.Date) obj).getTime();
            if (targetSqlType == Types.DATE) {
                updateDate(parameterIndex, new Date(timemillis));
            } else if (targetSqlType == Types.TIME) {
                updateTime(parameterIndex, new Time(timemillis));
            } else if (targetSqlType == Types.TIMESTAMP) {
                updateTimestamp(parameterIndex, new Timestamp(timemillis));
            }
        } else if (obj instanceof Boolean) {
            updateBoolean(parameterIndex, (Boolean) obj);
        } else if (obj instanceof java.sql.Blob) {
            updateBlob(parameterIndex, (java.sql.Blob) obj);
        } else if (obj instanceof java.sql.Clob) {
            updateClob(parameterIndex, (java.sql.Clob) obj);
        } else if (obj instanceof InputStream) {
            updateBinaryStream(parameterIndex, (InputStream) obj, scaleOrLength);
        } else if (obj instanceof Reader) {
            updateCharacterStream(parameterIndex, (Reader) obj, scaleOrLength);
        } else if (obj instanceof LocalDateTime) {
            updateTimestamp(parameterIndex, Timestamp.valueOf((LocalDateTime) obj));
        } else if (obj instanceof Instant) {
            updateTimestamp(parameterIndex, Timestamp.from((Instant) obj));
        } else if (obj instanceof LocalDate) {
            updateDate(parameterIndex, Date.valueOf((LocalDate) obj));
        } else if (obj instanceof OffsetDateTime) {
            updatableParameterHolders[parameterIndex - 1] = new ZonedDateTimeParameter(
                ((OffsetDateTime) obj).toZonedDateTime(), timeZone.toZoneId(),
                options.useFractionalSeconds, options);
        } else if (obj instanceof OffsetTime) {
            updatableParameterHolders[parameterIndex - 1] = new OffsetTimeParameter(
                (OffsetTime) obj, timeZone.toZoneId(), options.useFractionalSeconds, options);
        } else if (obj instanceof ZonedDateTime) {
            updatableParameterHolders[parameterIndex - 1] = new ZonedDateTimeParameter(
                (ZonedDateTime) obj, timeZone.toZoneId(), options.useFractionalSeconds, options);
        } else if (obj instanceof LocalTime) {
            updateTime(parameterIndex, Time.valueOf((LocalTime) obj));
        } else {
            throw ExceptionFactory.INSTANCE
                .create("Could not set parameter in setObject, could not convert: "
                        + obj.getClass() + " to " + targetSqlType);
        }
    }

    public void updateObject(int columnIndex, Object value, int scaleOrLength) throws SQLException {
        checkUpdatable(columnIndex);
        updateInternalObject(columnIndex, value, updatableColumns[columnIndex - 1].getColumnType()
            .getSqlType(), scaleOrLength);
    }

    public void updateObject(int columnIndex, Object value) throws SQLException {
        checkUpdatable(columnIndex);
        updateInternalObject(columnIndex, value, updatableColumns[columnIndex - 1].getColumnType()
            .getSqlType(), Long.MAX_VALUE);
    }

    public void updateObject(String columnLabel, Object value, int scaleOrLength)
                                                                                 throws SQLException {
        checkUpdatable();
        updateObject(findColumn(columnLabel), value, scaleOrLength);
    }

    public void updateObject(String columnLabel, Object value) throws SQLException {
        checkUpdatable();
        updateObject(findColumn(columnLabel), value);
    }

    public void updateObject(int columnIndex, Object x, SQLType targetSqlType) throws SQLException {
        checkUpdatable(columnIndex);
        updateInternalObject(columnIndex, x, targetSqlType.getVendorTypeNumber(), Long.MAX_VALUE);
    }

    public void updateLong(int columnIndex, long value) throws SQLException {
        checkUpdatable(columnIndex);
        updatableParameterHolders[columnIndex - 1] = new LongParameter(value);
    }

    public void updateLong(String columnLabel, long value) throws SQLException {
        checkUpdatable();
        updateLong(findColumn(columnLabel), value);
    }

    public void updateRef(int columnIndex, Ref ref) throws SQLException {
        throw ExceptionFactory.INSTANCE.notSupported("REF not supported");
    }

    public void updateRef(String columnLabel, Ref ref) throws SQLException {
        throw ExceptionFactory.INSTANCE.notSupported("REF not supported");
    }

    public void updateBlob(int columnIndex, java.sql.Blob blob) throws SQLException {
        checkUpdatable(columnIndex);
        if (blob == null) {
            updatableParameterHolders[columnIndex - 1] = new NullParameter(ColumnType.BLOB);
            return;
        }
        if (protocol.isOracleMode()) {
            updatableParameterHolders[columnIndex - 1] = new OBStreamParameter(
                blob.getBinaryStream(), blob.length(), noBackslashEscapes);
        } else {
            updatableParameterHolders[columnIndex - 1] = new StreamParameter(
                blob.getBinaryStream(), blob.length(), noBackslashEscapes);
        }
    }

    public void updateBlob(String columnLabel, java.sql.Blob blob) throws SQLException {
        throw ExceptionFactory.INSTANCE.notSupported(NOT_UPDATABLE_ERROR);
    }

    public void updateBlob(String columnLabel, Blob blob) throws SQLException {
        checkUpdatable();
        updateBlob(findColumn(columnLabel), blob);
    }

    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        updateBlob(columnIndex, inputStream, Long.MAX_VALUE);
    }

    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        checkUpdatable();
        updateBlob(findColumn(columnLabel), inputStream, Long.MAX_VALUE);
    }

    public void updateBlob(int columnIndex, InputStream inputStream, long length)
                                                                                 throws SQLException {
        checkUpdatable(columnIndex);
        if (inputStream == null) {
            updatableParameterHolders[columnIndex - 1] = new NullParameter(ColumnType.BLOB);
            return;
        }
        if (protocol.isOracleMode()) {
            updatableParameterHolders[columnIndex - 1] = new OBStreamParameter(inputStream, length,
                noBackslashEscapes);
        } else {
            updatableParameterHolders[columnIndex - 1] = new StreamParameter(inputStream, length,
                noBackslashEscapes);
        }
    }

    public void updateBlob(String columnLabel, InputStream inputStream, long length)
                                                                                    throws SQLException {
        checkUpdatable();
        updateBlob(findColumn(columnLabel), inputStream, length);
    }

    public void updateClob(int columnIndex, java.sql.Clob clob) throws SQLException {
        checkUpdatable(columnIndex);
        if (clob == null) {
            updatableParameterHolders[columnIndex - 1] = new NullParameter(ColumnType.BLOB);
            return;
        }
        updatableParameterHolders[columnIndex - 1] = new ReaderParameter(clob.getCharacterStream(),
            clob.length(), noBackslashEscapes);
    }

    public void updateClob(String columnLabel, Clob clob) throws SQLException {
        checkUpdatable();
        updateClob(findColumn(columnLabel), clob);
    }

    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        updateCharacterStream(columnIndex, reader, length);
    }

    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        checkUpdatable();
        updateCharacterStream(findColumn(columnLabel), reader, length);
    }

    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        updateCharacterStream(columnIndex, reader);
    }

    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        checkUpdatable();
        updateCharacterStream(findColumn(columnLabel), reader);
    }

    public void updateArray(int columnIndex, Array array) throws SQLException {
        throw ExceptionFactory.INSTANCE.notSupported("Arrays not supported");
    }

    public void updateArray(String columnLabel, Array array) throws SQLException {
        throw ExceptionFactory.INSTANCE.notSupported("Arrays not supported");
    }

    public void updateRowId(int columnIndex, RowId rowId) throws SQLException {
        throw ExceptionFactory.INSTANCE.notSupported("RowIDs not supported");
    }

    public void updateRowId(String columnLabel, RowId rowId) throws SQLException {
        throw ExceptionFactory.INSTANCE.notSupported("RowIDs not supported");
    }

    public void updateNString(int columnIndex, String nstring) throws SQLException {
        updateString(columnIndex, nstring);
    }

    public void updateNString(String columnLabel, String nstring) throws SQLException {
        checkUpdatable();
        updateString(columnLabel, nstring);
    }

    public void updateNClob(int columnIndex, NClob nclob) throws SQLException {
        updateClob(columnIndex, nclob);
    }

    public void updateNClob(String columnLabel, NClob nclob) throws SQLException {
        updateClob(columnLabel, nclob);
    }

    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        updateClob(columnIndex, reader);
    }

    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        updateClob(columnLabel, reader);
    }

    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        updateClob(columnIndex, reader, length);
    }

    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        updateClob(columnLabel, reader, length);
    }

    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        throw ExceptionFactory.INSTANCE.notSupported("SQLXML not supported");
    }

    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        throw ExceptionFactory.INSTANCE.notSupported("SQLXML not supported");
    }

    public void updateNCharacterStream(int columnIndex, Reader value, long length)
                                                                                  throws SQLException {
        updateCharacterStream(columnIndex, value, length);
    }

    public void updateNCharacterStream(String columnLabel, Reader reader, long length)
                                                                                      throws SQLException {
        updateCharacterStream(columnLabel, reader, length);
    }

    public void updateNCharacterStream(int columnIndex, Reader reader) throws SQLException {
        updateCharacterStream(columnIndex, reader);
    }

    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        updateCharacterStream(columnLabel, reader);
    }

    /*************************** updatable characters ending ****************************/

    /**
     * {inheritDoc}.
     */
    public int getHoldability() throws SQLException {
        if (!protocol.isOracleMode()) {
            throw ExceptionFactory.INSTANCE.notSupported("Method ResultSet.getHoldability() not supported");

        }
        return HOLD_CURSORS_OVER_COMMIT;
    }

    /**
     * {inheritDoc}.
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
                "The receiver is not a wrapper and does not implement the interface");
        }
    }

    /**
     * {inheritDoc}.
     */
    public boolean isWrapperFor(final Class<?> iface) throws SQLException {
        if (!protocol.isOracleMode()) {
            checkClose();
        }
        return iface.isInstance(this);
    }

    /**
     * Force metadata getTableName to return table alias, not original table name.
     */
    public void setForceTableAlias() {
        this.forceAlias = true;
    }

    private void rangeCheck(Object className, long minValue, long maxValue, long value,
                            ColumnDefinition columnInfo) throws SQLException {
        if (value < minValue || value > maxValue) {
            throw new SQLException("Out of range value for column '" + columnInfo.getName()
                                   + "' : value " + value + " is not in " + className + " range",
                "22003", 1264);
        }
    }

    public int getRowPointer() {
        return rowPointer;
    }

    protected void setRowPointer(int pointer) {
        rowPointer = pointer;
    }

    public int getDataSize() {
        return dataSize;
    }

    public long getProcessedRows() {
        return processedRows;
    }

    public boolean isBinaryEncoded() {
        return row.isBinaryEncoded();
    }

    public boolean isEof() {
        return isEof;
    }

}
