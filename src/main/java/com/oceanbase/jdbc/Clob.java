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
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;

import com.oceanbase.jdbc.internal.com.read.Buffer;
import com.oceanbase.jdbc.internal.util.exceptions.ExceptionFactory;

public class Clob extends Lob implements ObClob {

    private static final long serialVersionUID = -3066501059817815286L;
    private static int        maxLength        = 1 * 1024 * 1024 * 1024;
    String                    Clob_0           = "indexToWriteAt must be >= 1";
    String                    Clob_1           = "indexToWriteAt must be >= 1";
    String                    Clob_2           = "Starting position can not be < 1";
    String                    Clob_3           = "String to set can not be NULL";
    String                    Clob_4           = "Starting position can not be < 1";
    String                    Clob_5           = "String to set can not be NULL";
    String                    Clob_6           = "CLOB start position can not be < 1";
    String                    Clob_7           = "CLOB start position + length can not be > length of CLOB";
    String                    Clob_8           = "Illegal starting position for search, '";
    String                    Clob_9           = "'";
    String                    Clob_10          = "Starting position for search is past end of CLOB";
    String                    Clob_11          = "Cannot truncate CLOB of length";
    String                    Clob_12          = "\\\\ to length of";
    String                    Clob_13          = ".";

    /**
     * Creates a Clob with content.
     *
     * @param bytes the content for the Clob.
     */
    public Clob(byte[] bytes) {
        super(bytes);
        this.encoding = Charset.defaultCharset().name();
    }

    public static Clob getEmptyCLOB() throws SQLException {
        byte[] emptyData = new byte[40];
        emptyData[0] = 33;
        emptyData[1] = 66;
        emptyData[2] = 79;
        emptyData[3] = 76;
        emptyData[4] = 1;
        return new Clob(true, emptyData, Charset.defaultCharset().name(), null);

    }

    public Clob(byte[] bytes, ExceptionInterceptor exceptionInterceptor) {
        super(bytes, exceptionInterceptor);
        this.encoding = Charset.defaultCharset().name();
    }

    /**
     * Creates a Clob with content.
     *
     * @param bytes  the content for the Clob.
     * @param offset offset
     * @param length length
     */

    public Clob(byte[] bytes, int offset, int length) {
        super(bytes, offset, length);
        this.encoding = Charset.defaultCharset().name();
    }

    public Clob(String charDataInit, ExceptionInterceptor exceptionInterceptor) {
        this.charData = charDataInit;
        this.exceptionInterceptor = exceptionInterceptor;
        this.encoding = Charset.defaultCharset().name(); // set default encoding
    }

    public Clob(boolean hasLocator, byte[] data, String encoding, OceanBaseConnection conn)
                                                                                           throws SQLException {
        if (!hasLocator) {
            this.encoding = encoding;
            try {
                this.charData = new String(data, this.encoding);
            } catch (UnsupportedEncodingException e) {
                throw new SQLException("Unsupported character encoding");
            }
        } else {
            if (null != data) {
                Buffer buffer = new Buffer(data);
                this.encoding = encoding;
                if (buffer.getLimit() >= ObLobLocator.OB_LOG_LOCATOR_HEADER) {
                    locator = new ObLobLocator();
                    locator.magicCode = buffer.readLongV1();
                    locator.version = buffer.readLongV1();
                    locator.snapshotVersion = buffer.readLongLongV1();
                    locator.tableId = buffer.readBytes(8);
                    locator.columnId = buffer.readLongV1();
                    locator.flags = buffer.readIntV1();
                    locator.option = buffer.readIntV1();
                    locator.payloadOffset = buffer.readLongV1();
                    locator.payloadSize = buffer.readLongV1();
                    locator.binaryData = buffer.getByteBuffer();
                    if (locator.payloadSize + locator.payloadOffset <= buffer.getLimit()
                                                                       - ObLobLocator.OB_LOG_LOCATOR_HEADER
                        && conn != null) {
                        locator.rowId = buffer.readBytes((int) locator.payloadOffset); // row_id must be less than MAX_INT
                        locator.connection = conn;
                        try {
                            if ((int) locator.payloadSize <= Clob.maxLength) {
                                this.charData = new String(
                                    buffer.getBytes(
                                        (int) (ObLobLocator.OB_LOG_LOCATOR_HEADER + locator.payloadOffset),
                                        (int) locator.payloadSize), this.encoding);
                                length = charData.length();
                            } else {
                                throw new SQLException("Exceed max length of Clob for support "
                                                       + Clob.maxLength + " current "
                                                       + locator.payloadSize);
                            }
                        } catch (UnsupportedEncodingException e) {
                            throw new SQLException("Unsupported character encoding "
                                                   + this.encoding);
                        }
                    }
                }

            }
        }
    }

    /**
     * Creates an empty Clob.
     */
    public Clob() {
        super();
        this.encoding = Charset.defaultCharset().name();
    }

    public synchronized ObLobLocator getLocator() {
        return this.locator;
    }

    public synchronized void setLocator(ObLobLocator locator) {
        if (this.locator == null) {
            this.locator = new ObLobLocator();
        }
        this.locator.columnId = locator.columnId;
        this.locator.flags = locator.flags;
        this.locator.magicCode = locator.magicCode;
        this.locator.option = locator.option;
        this.locator.snapshotVersion = locator.snapshotVersion;
        this.locator.tableId = locator.tableId;
        this.locator.rowId = locator.rowId;
        this.locator.version = locator.version;
        this.locator.payloadOffset = locator.payloadOffset;
        this.locator.payloadSize = locator.payloadSize;
        this.locator.binaryData = locator.binaryData;
    }

    /**
     * ToString implementation.
     *
     * @return string value of blob content.
     */
    public String toString() {
        if (charData != null) {
            return charData;
        } else {
            Charset charset = Charset.forName(encoding);
            return new String(data, offset, length, charset);
        }
    }

    /**
     * Get sub string.
     *
     * @param pos    position
     * @param length length of sub string
     * @return substring
     * @throws SQLException if pos is less than 1 or length is less than 0
     */
    public String getSubString(long pos, int length) throws SQLException {

        if (pos < 1) {
            throw ExceptionFactory.INSTANCE.create("position must be >= 1");
        }

        if (length < 0) {
            throw ExceptionFactory.INSTANCE.create("length must be > 0");
        }
        int adjustedStartPos = (int) pos - 1;
        int adjustedEndIndex = adjustedStartPos + length;

        if (this.charData != null) {
            if (adjustedEndIndex > this.charData.length()) {
                // Do nothing just use the adjustedEndIndex.
                adjustedEndIndex = this.charData.length();
                /*
                throw SQLError.createSQLException(Messages.getString("Clob.7"),
                    SQLError.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor); */
            }

            return this.charData.substring(adjustedStartPos, adjustedEndIndex);
        }

        try {
            String val = toString();
            return val.substring((int) pos - 1, Math.min((int) pos - 1 + length, val.length()));
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    public Reader getCharacterStream() {
        if (this.charData != null) {
            return new StringReader(this.charData);
        }
        return new StringReader(toString());
    }

    /**
     * Returns a Reader object that contains a partial Clob value, starting with the character
     * specified by pos, which is length characters in length.
     *
     * @param pos    the offset to the first character of the partial value to be retrieved. The first
     *               character in the Clob is at position 1.
     * @param length the length in characters of the partial value to be retrieved.
     * @return Reader through which the partial Clob value can be read.
     * @throws SQLException if pos is less than 1 or if pos is greater than the number of characters
     *                      in the Clob or if pos + length is greater than the number of characters in the Clob
     */
    public Reader getCharacterStream(long pos, long length) throws SQLException {
        String val = toString();
        if (val.length() < (int) pos - 1 + length) {
            throw ExceptionFactory.INSTANCE
                .create("pos + length is greater than the number of characters in the Clob");
        }
        String sub = val.substring((int) pos - 1, (int) pos - 1 + (int) length);
        return new StringReader(sub);
    }

    /**
     * Set character stream.
     *
     * @param pos position
     * @return writer
     * @throws SQLException if position is invalid
     */
    public Writer setCharacterStream(long pos) throws SQLException {
        int bytePosition = utf8Position((int) pos - 1);
        OutputStream stream = setBinaryStream(bytePosition + 1);
        try {
            return new OutputStreamWriter(stream, encoding);
        } catch (UnsupportedEncodingException e) {
            throw new SQLException("Unsupported character encoding " + this.encoding);
        }
    }

    public InputStream getAsciiStream() throws SQLException {
        if (this.charData != null) {
            try {
                if (this.encoding != null) {
                    return new ByteArrayInputStream(charData.getBytes(this.encoding));
                } else {
                    return new ByteArrayInputStream(charData.getBytes());
                }
            } catch (UnsupportedEncodingException e) {
                throw new SQLException("Unsupported character encoding " + this.encoding);
            }
        }
        return getBinaryStream();
    }

    public long position(String searchStr, long start) throws SQLException {
        if (start < 1) {
            throw new SQLException("Illegal starting position for search, '" + start + "'");
        }
        if ((start - 1) > this.toString().length()) {
            throw new SQLException("Cannot truncate CLOB of length ");
        }
        long pos = toString().indexOf(searchStr, (int) start - 1);
        return (pos == -1) ? (-1) : (pos + 1);
    }

    public long position(java.sql.Clob searchStr, long start) throws SQLException {
        return position(searchStr.toString(), start);
    }

    /**
     * Convert character position into byte position in UTF8 byte array.
     *
     * @param charPosition charPosition
     * @return byte position
     */
    private int utf8Position(int charPosition) {
        int pos = offset;
        for (int i = 0; i < charPosition; i++) {
            int byteValue = data[pos] & 0xff;
            if (byteValue < 0x80) {
                pos += 1;
            } else if (byteValue < 0xC2) {
                throw new UncheckedIOException("invalid UTF8", new CharacterCodingException());
            } else if (byteValue < 0xE0) {
                pos += 2;
            } else if (byteValue < 0xF0) {
                pos += 3;
            } else if (byteValue < 0xF8) {
                pos += 4;
            } else {
                throw new UncheckedIOException("invalid UTF8", new CharacterCodingException());
            }
        }
        return pos;
    }

    /**
     * Set String.
     *
     * @param pos position
     * @param str string
     * @return string length
     * @throws SQLException if UTF-8 conversion failed
     */
    public int setString(long pos, String str) throws SQLException {
        if (this.locator != null) {
            try {
                updateClobToServer(pos, str.getBytes(this.encoding), 0, str.length());
            } catch (UnsupportedEncodingException e) {
                throw new SQLException("Unsupported character encoding " + this.encoding);
            }
        } else {
            int bytePosition = utf8Position((int) pos - 1);
            try {
                super.setBytes(bytePosition + 1 - offset, str.getBytes(encoding));
            } catch (UnsupportedEncodingException e) {
                throw new SQLException("Unsupported character encoding " + this.encoding);
            }
            return str.length();
        }
        return 0;
    }

    public int setString(long pos, String str, int offset, int len) throws SQLException {
        if (pos < 1) {
            throw new SQLException(Clob_4);
        }
        if (str == null) {
            throw new SQLException(Clob_5);
        }
        try {
            return setString(pos, str.substring(offset, offset + len));
        } catch (StringIndexOutOfBoundsException e) {
            throw new SQLException(e.getMessage());
        }
    }

    public OutputStream setAsciiStream(long pos) throws SQLException {
        return setBinaryStream(utf8Position((int) pos - 1) + 1);
    }

    /**
     * Return character length of the Clob. Assume UTF8 encoding.
     */
    @Override
    public long length() {
        // The length of a character string is the number of UTF-16 units (not the number of characters)
        if (this.charData != null) {
            return this.charData.length();
        }

        Charset charset = Charset.forName(encoding);
        if (!charset.equals(StandardCharsets.UTF_8)) {
            return toString().length();
        }
        long len = 0;
        int pos = offset;

        // set ASCII (<= 127 chars)
        for (; len < length && data[pos] >= 0;) {
            len++;
            pos++;
        }

        // multi-bytes UTF-8
        while (pos < offset + length) {
            byte firstByte = data[pos++];
            if (firstByte < 0) {
                if (firstByte >> 5 != -2 || (firstByte & 30) == 0) {
                    if (firstByte >> 4 == -2) {
                        if (pos + 1 < offset + length) {
                            pos += 2;
                            len++;
                        } else {
                            throw new UncheckedIOException("invalid UTF8",
                                new CharacterCodingException());
                        }
                    } else if (firstByte >> 3 != -2) {
                        throw new UncheckedIOException("invalid UTF8",
                            new CharacterCodingException());
                    } else if (pos + 2 < offset + length) {
                        pos += 3;
                        len += 2;
                    } else {
                        // bad truncated UTF8
                        pos += offset + length;
                        len += 1;
                    }
                } else {
                    pos++;
                    len++;
                }
            } else {
                len++;
            }
        }
        return len;
    }

    @Override
    public void truncate(final long truncateLen) throws SQLException {
        if (locator != null) {
            if (length > this.charData.length()) {
                throw new SQLException("Clob length is more than charData length");
            }

            if (this.locator == null) {
                this.charData = this.charData.substring(0, (int) truncateLen);
            } else {
                trimClobToServer((int) truncateLen);
            }
            return;
        }
        if (charData != null && charData.length() > 0 && truncateLen < charData.length()) {
            this.charData = this.charData.substring(0, (int) truncateLen);
            return;
        }
        // truncate the number of UTF-16 characters
        // this can result in a bad UTF-8 string if string finish with a
        // character represented in 2 UTF-16
        long len = 0;
        int pos = offset;

        // set ASCII (<= 127 chars)
        for (; len < length && len < truncateLen && data[pos] >= 0;) {
            len++;
            pos++;
        }

        // multi-bytes UTF-8
        while (pos < offset + length && len < truncateLen) {
            byte firstByte = data[pos++];
            if (firstByte < 0) {
                if (firstByte >> 5 != -2 || (firstByte & 30) == 0) {
                    if (firstByte >> 4 == -2) {
                        if (pos + 1 < offset + length) {
                            pos += 2;
                            len++;
                        } else {
                            throw new UncheckedIOException("invalid UTF8",
                                new CharacterCodingException());
                        }
                    } else if (firstByte >> 3 != -2) {
                        throw new UncheckedIOException("invalid UTF8",
                            new CharacterCodingException());
                    } else if (pos + 2 < offset + length) {
                        if (len + 2 <= truncateLen) {
                            pos += 3;
                            len += 2;
                        } else {
                            // truncation will result in bad UTF-8 String
                            pos += 1;
                            len = truncateLen;
                        }
                    } else {
                        throw new UncheckedIOException("invalid UTF8",
                            new CharacterCodingException());
                    }
                } else {
                    pos++;
                    len++;
                }
            } else {
                len++;
            }
        }
        length = pos - offset;
    }

    /**
     * Update <code>Clob</code> object by the DBMS_LOB.write(?,?,?,?), starting with writeAt and write the bytes to
     * database.
     *
     * @param writeAt Start position to be write.
     * @param bytes   byte[] data to be write.
     * @param offset  first position offset will be write.
     * @param length  date length to be write.
     * @throws SQLException if database error occur
     */
    public synchronized void updateClobToServer(long writeAt, byte[] bytes, int offset, int length)
                                                                                                   throws SQLException {
        if (this.locator == null || this.locator.connection == null) {
            throw new SQLException("Invalid operation on closed CLOB");
        }
        java.sql.CallableStatement cstmt = this.locator.connection
            .prepareCall("{call DBMS_LOB.write( ?, ?, ?, ?)}");
        cstmt.setClob(1, this);
        cstmt.setInt(2, length);
        cstmt.setInt(3, (int) writeAt);
        try {
            //            cstmt.setString(4, StringUtils.toString(bytes, this.encoding)); // todo
            cstmt.setString(4, new String(bytes, this.encoding));
        } catch (UnsupportedEncodingException e) {
            throw new SQLException(e.getMessage());
        }
        cstmt.registerOutParameter(1, Types.CLOB);
        cstmt.execute();

        Clob r = (Clob) cstmt.getClob(1);
        if (r == null || r.getLocator() == null) {
            throw new SQLException("Invalid operation on closed CLOB");
        } else {
            setLocator(r.locator);
            this.data = r.data;
            int from = (int) (ObLobLocator.OB_LOG_LOCATOR_HEADER + locator.payloadOffset);
            int to = (int) (from + locator.payloadSize);
            this.encoding = r.encoding;
            try {
                this.charData = new String(Arrays.copyOfRange(r.locator.binaryData, from, to),
                    this.encoding);
            } catch (UnsupportedEncodingException e) {
                throw new SQLException("Unsupported character encoding " + this.encoding);
            }
        }

    }

    /**
     * Truncate <code>Clob</code> object by the DBMS_LOB.trim(?,?), truncate data from len, exception if len is big than data's length
     * and success when len is less than data's length.
     *
     * @param len truncate data from len
     * @throws SQLException if database error occur
     */
    public synchronized void trimClobToServer(int len) throws SQLException {
        if (this.locator == null || this.locator.connection == null) {
            throw new SQLException("Invalid operation on closed CLOB");
        }

        java.sql.CallableStatement cstmt = this.locator.connection
            .prepareCall("{call DBMS_LOB.trim( ?, ?)}");
        cstmt.setClob(1, this);
        cstmt.setInt(2, len);
        cstmt.registerOutParameter(1, Types.CLOB);
        cstmt.execute();

        Clob r = (Clob) cstmt.getClob(1);
        if (r == null || r.getLocator() == null) {
            throw new SQLException("Invalid operation on closed CLOB");
        } else {
            setLocator(r.locator);
            this.data = r.data;
            int from = (int) (ObLobLocator.OB_LOG_LOCATOR_HEADER + locator.payloadOffset);
            int to = (int) (from + locator.payloadSize);
            this.encoding = r.encoding;
            try {
                this.charData = new String(Arrays.copyOfRange(r.locator.binaryData, from, to),
                    this.encoding);
            } catch (UnsupportedEncodingException e) {
                throw new SQLException("Unsupported character encoding " + this.encoding);

            }
        }
    }

    @Override
    public boolean isEmptyLob() {
        if (this.locator != null) {
            return locator.payloadSize == 0;
        } else {
            return ((data == null || data.length == 0) && (charData == null || charData.length() == 0));
        }
    }
}
