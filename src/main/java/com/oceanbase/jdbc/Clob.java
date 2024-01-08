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
import java.sql.CallableStatement;
import java.sql.SQLException;
import java.sql.Types;

import com.oceanbase.jdbc.internal.com.read.Buffer;
import com.oceanbase.jdbc.internal.util.exceptions.ExceptionFactory;

public class Clob extends Lob implements java.sql.Clob {

    private static final long   serialVersionUID = -3066501059817815286L;
    static int                  maxLength        = 1024 * 1024 * 1024;
    private static final String Clob_0           = "indexToWriteAt must be >= 1";
    private static final String Clob_1           = "indexToWriteAt must be >= 1";
    private static final String Clob_2           = "Starting position can not be < 1";
    private static final String Clob_3           = "String to set can not be NULL";
    private static final String Clob_4           = "Starting position can not be < 1";
    private static final String Clob_5           = "String to set can not be NULL";
    private static final String Clob_6           = "CLOB start position can not be < 1";
    private static final String Clob_7           = "CLOB start position + length can not be > length of CLOB";
    private static final String Clob_8           = "Illegal starting position for search, '";
    private static final String Clob_9           = "'";
    private static final String Clob_10          = "Starting position for search is past end of CLOB";
    private static final String Clob_11          = "Cannot truncate CLOB of length";
    private static final String Clob_12          = "\\\\ to length of";
    private static final String Clob_13          = ".";

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
        if (null != conn) {
            this.encoding = conn.getProtocol().getEncoding();
            /* It only be used by getCharacterStream for Blob, and to decode the byte[] data to char[]
             *  Use the encoding with the Connection's encoding's Name assigned by USER. For that it is
             *  user which encoding byte[] to char[]. */
        } else if (null != encoding) {
            this.encoding = encoding; // not to be here
        } else {
            this.encoding = "UTF-8";
        }

        if (!hasLocator) {
            try {
                this.charData = new String(data, this.encoding);
            } catch (UnsupportedEncodingException e) {
                throw new SQLException("Unsupported character encoding");
            }
        } else if (null != data) {
            Buffer buffer = new Buffer(data);
            long magicCode = buffer.readLong4BytesV1();
            long version = buffer.readLong4BytesV1();

            if (version == 1) {
                buildObLobLocatorV1(false, buffer, magicCode, version, conn);
            } else if ((version & 0x000000ff) == 2) {
                buildObLobLocatorV2(false, buffer, magicCode, version, conn);
            } else {
                throw new SQLException("Unknown version of lob locator!");
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

        // out-row of lob locator v2, needs to be read from server by procedure DBMS_LOB.READ
        if (this.length == 0 && !isEmptyLob()) {
            if (locator != null && locator instanceof ObLobLocatorV2) {
                readFromServer();
            }
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

    public Reader getCharacterStream() throws SQLException {
        // out-row of lob locator v2, needs to be read from server by procedure DBMS_LOB.READ
        if (this.length == 0 && !isEmptyLob()) {
            if (locator != null && locator instanceof ObLobLocatorV2) {
                readFromServer();
            }
        }
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
        // out-row of lob locator v2, needs to be read from server by procedure DBMS_LOB.READ
        if (this.length == 0 && !isEmptyLob()) {
            if (locator != null && locator instanceof ObLobLocatorV2) {
                readFromServer();
            }
        }
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
        if (pos < 1L) {
            throw new SQLException(Clob_4);
        }
        if (str == null || str.length() == 0) {
            return 0;
        }

        if (this.locator != null) {
            try {
                updateClobToServer(pos, str, str.length());
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
        }
        return str.length();
    }

    public int setString(long pos, String str, int offset, int len) throws SQLException {
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
    public long length() throws SQLException {
        // The length of a character string is the number of UTF-16 units (not the number of characters)
        if (this.charData != null) {
            return this.charData.length();
        }

        if (data == null || data.length == 0) {
            if (locator != null && locator.payloadSize > 0 && locator instanceof ObLobLocatorV2) {
                if (lengthFromServer == -1) {
                    lengthFromServer = 0; // change state
                    // in this case, this is out-row lob which hasn't been read from server, so both charData and data are empty
                    getLengthFromServer();
                }
                return lengthFromServer;
            }
        }

        Charset charset = Charset.forName(encoding);
        if (!charset.equals(StandardCharsets.UTF_8)) {
            return toString().length();
        }

        long len = 0;
        int pos = offset;
        // set ASCII (<= 127 chars)
        while (len < length && data[pos] >= 0) {
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
        if (truncateLen < 0) {
            if (this.isOracleMode) {
                throw new SQLException("Invalid argument: 'len' should not be < 0");
            }
            throw new StringIndexOutOfBoundsException("String index out of range: " + truncateLen);
        }

        // charData either hasn't been obtained or is complete.
        if (this.charData != null && truncateLen > this.charData.length()) {
            throw new SQLException("Cannot truncate CLOB of length " + this.charData.length()
                                   + " to length of " + truncateLen);
        }

        if (locator != null) {
            trimClobToServer((int) truncateLen);
            return;
        } else if (this.charData != null) {
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
     * Update <code>Clob</code> object by the DBMS_LOB.WRITE(lob_loc, amount, offset, buffer), starting with writeAt and write the bytes to
     * database.
     *
     * DBMS_LOB.WRITE (
     *    lob_loc  IN OUT  NOCOPY CLOB   CHARACTER SET ANY_CS,
     *    amount   IN             INTEGER,
     *    offset   IN             INTEGER,
     *    buffer   IN             VARCHAR2 CHARACTER SET lob_loc%CHARSET);
     *
     * lob_loc: Locator for the internal LOB to be written to. For more information
     * amount: Number of bytes (for BLOBs) or characters (for CLOBs) to write
     * offset: Offset in bytes (for BLOBs) or characters (for CLOBs) from the start of the LOB (origin: 1) for the write operation.
     * buffer: Input buffer for the write
     *
     * @param writeAt Start position to be write.
     * @param str   String data to be write.
     * @param length  date length to be write.
     * @throws SQLException if database error occur
     */
    private synchronized void updateClobToServer(long writeAt, String str, int length)
                                                                                      throws SQLException,
                                                                                      UnsupportedEncodingException {
        if (this.locator == null || this.locator.connection == null) {
            throw new SQLException("Invalid operation on closed CLOB");
        }

        int writeAmount, writeOffset = (int) writeAt, localOffset = 0, lengthLeft = length;
        try (CallableStatement cstmt = this.locator.connection.prepareCall("{call DBMS_LOB.write( ?, ?, ?, ?)}")) {
            ((OceanBaseStatement) cstmt).setInternal();
            while (lengthLeft > 0) {
                writeAmount = Math.min(lengthLeft, DBMS_LOB_MAX_AMOUNT);

                cstmt.setClob(1, this);
                cstmt.setInt(2, writeAmount);
                cstmt.setInt(3, writeOffset);
                cstmt.setString(4, str.substring(localOffset, localOffset + writeAmount));
                cstmt.registerOutParameter(1, Types.CLOB);
                cstmt.execute();

                writeOffset += writeAmount;
                localOffset += writeAmount;
                lengthLeft -= writeAmount;
            }

            Clob r = (Clob) cstmt.getClob(1);
            if (r == null || r.getLocator() == null) {
                throw new SQLException("Invalid operation on closed CLOB");
            } else {
                copy(r);
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
    private synchronized void trimClobToServer(int len) throws SQLException {
        if (this.locator == null || this.locator.connection == null) {
            throw new SQLException("Invalid operation on closed CLOB");
        }
        clearData();

        try (java.sql.CallableStatement cstmt = this.locator.connection.prepareCall("{call DBMS_LOB.trim( ?, ?)}")) {
            ((OceanBaseStatement) cstmt).setInternal();
            cstmt.setClob(1, this);
            cstmt.setInt(2, len);
            cstmt.registerOutParameter(1, Types.CLOB);
            cstmt.execute();

            Clob r = (Clob) cstmt.getClob(1);
            if (r == null || r.getLocator() == null) {
                throw new SQLException("Invalid operation on closed CLOB");
            } else {
                copy(r);
            }
        }
    }

    /**
     * Read Clob object by the DBMS_LOB.READ(lob_loc, amount, offset, buffer).
     *
     * DBMS_LOB.READ (
     *    lob_loc   IN             CLOB CHARACTER SET ANY_CS,
     *    amount    IN OUT  NOCOPY INTEGER,
     *    offset    IN             INTEGER,
     *    buffer    OUT            VARCHAR2 CHARACTER SET lob_loc%CHARSET);
     * lob_loc: Locator for the LOB to be read. For more information, see Operational Notes.
     * file_loc: The file locator for the LOB to be examined.
     * amount: Number of bytes (for BLOBs) or characters (for CLOBs) to read, or number that were read.
     * offset: Offset in bytes (for BLOBs) or characters (for CLOBs) from the start of the LOB (origin: 1).
     * buffer: Output buffer for the read operation.
     *
     * @throws SQLException if database READ procedure exceptions occur
     * INVALID_ARGVAL: amount is less than 1, or amount is larger than 32767 bytes (or the character equivalent)
     *                 offset is less than 1, or offset is larger than LOBMAXSIZE
     *                 amount is greater, in bytes or characters, than the capacity of buffer.
     * NO_DATA_FOUND: End of the LOB is reached, and there are no more bytes or characters to read from the LOB: amount has a value of 0.
     */
    protected synchronized void readFromServer() throws SQLException {
        if (this.locator == null || this.locator.connection == null) {
            throw new SQLException("Invalid operation on closed CLOB");
        }

        int offset = 1;
        try (java.sql.CallableStatement cstmt = this.locator.connection.prepareCall("{call DBMS_LOB.READ( ?, ?, ?, ?)}")) {
            ((OceanBaseStatement) cstmt).setInternal();
            while (true) {
                cstmt.setClob(1, this);
                cstmt.setInt(2, DBMS_LOB_MAX_AMOUNT);
                cstmt.setInt(3, offset);
                cstmt.registerOutParameter(2, Types.INTEGER);
                cstmt.registerOutParameter(4, Types.VARCHAR);
                cstmt.execute();

                int amount = cstmt.getInt(2);
                offset += amount;

                try {
                    if (amount <= Clob.maxLength) {
                        byte[] bytes = cstmt.getBytes(4);
                        String str = new String(bytes, this.encoding);
                        if (charData == null) {
                            charData = str;
                        } else {
                            charData += str;
                        }
                    } else {
                        throw new SQLException("Exceed max length of Clob for support "
                                               + Clob.maxLength + " current " + amount);
                    }
                } catch (UnsupportedEncodingException e) {
                    throw new SQLException("Unsupported character encoding " + this.encoding);
                }
            }
        } catch (SQLException ex) {
            if (!ex.getMessage().contains("no data found")) {
                throw ex;
            }
        }
        length = charData.length();
    }

    /**
     * This function gets the length of the specified LOB. The length in bytes or characters is returned.
     *
     * The length returned for a BFILE includes the EOF, if it exists.
     * Any 0-byte or space filler in the LOB caused by previous ERASE or WRITE operations is also included in the length count.
     * The length of an empty internal LOB is 0.
     *
     * DBMS_LOB.GETLENGTH (
     *    lob_loc    IN  CLOB   CHARACTER SET ANY_CS)
     *   RETURN INTEGER;
     *
     * @throws SQLException if database error occur
     */
    private synchronized void getLengthFromServer() throws SQLException {
        if (this.locator == null || this.locator.connection == null) {
            throw new SQLException("Invalid operation on closed CLOB");
        }

        try (java.sql.CallableStatement cstmt = this.locator.connection.prepareCall("{? = call DBMS_LOB.GETLENGTH( ?)}")) {
            ((OceanBaseStatement) cstmt).setInternal();
            cstmt.setClob(2, this);
            cstmt.registerOutParameter(1, Types.INTEGER);
            cstmt.execute();

            lengthFromServer = cstmt.getInt(1);
        }
    }
}
