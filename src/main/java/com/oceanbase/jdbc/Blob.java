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
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.sql.Types;

import com.oceanbase.jdbc.internal.com.read.Buffer;
import com.oceanbase.jdbc.internal.util.exceptions.ExceptionFactory;

/**
 * The LobLocator for OceanBase Blob/Clob object. It will be send when the serverCapabilities's
 * OB_CLIENT_SUPPORT_LOB_LOCATOR has been set. Server will update data base on the <code>ObLobLocator</code> object.
 * Length for the <code>ObLobLocator</code> is OB_LOG_LOCATOR_HEADER(40), and it has payloadOffset length rowId, and the
 * data left if data store on the database. Such as: magic_code_: 21 42 4f 4c version_: 01 00 00 00 snapshot_version_:
 * 89 ae d4 cb 3f af 05 00 table_id_: 7b c3 00 00 00 e9 03 00 column_id_: 11 00 00 00 flags_： 00 00 option_: 00 00
 * payload_offset_: 11 00 00 00 payload_size_: 03 00 00 00 row_id_: 2a 41 41 49 4b 41 51 41 41 41 41 41 41 41 41 41 3d
 * data_(payload): 61 62 63 #will be store to Blob/Clob object
 *
 */
class ObLobLocator {
    protected long magicCode; // uint32_t magic_code_;
    protected long version; // uint32_t version_;
    protected long snapshotVersion; // int64_t snapshot_version_;
    protected byte[] tableId; // uint64_t table_id_;
    protected long columnId; // uint32_t column_id_;
    protected int flags; // uint16_t flags_;
    protected int option; // uint16_t option_;
    protected long payloadOffset; // uint32_t payload_offset_;
    protected long payloadSize; // uint32_t payload_size_;
    protected byte[] rowId; // row_id length == payloadOffset
    protected byte[] binaryData; // binary to data to send
    public static int OB_LOG_LOCATOR_HEADER = 40;
    protected volatile OceanBaseConnection connection;
}

public class Blob implements ObBlob, Serializable {

    private static final long serialVersionUID = -4736603161284649490L;
    protected byte[] data = null;
    protected transient int offset;
    protected transient int length;
    protected ObLobLocator locator = null;
    protected String encoding = null;
    protected String charData = null;
    protected ExceptionInterceptor exceptionInterceptor = null;

    /** Creates an empty blob. */
    public Blob() {
        data = new byte[0];
        offset = 0;
        length = 0;
    }

    public static Blob getEmptyBLOB() throws SQLException {
        return null;
    }

    public Blob(Blob other) {
        data = other.data;
        offset = other.offset;
        length = other.length;
    }

    /**
     * Creates a blob with content.
     *
     * @param bytes
     *            the content for the blob.
     */
    public Blob(byte[] bytes) {
        data = bytes;
        offset = 0;
        length = bytes.length;
    }

    public Blob(byte[] bytes, ExceptionInterceptor exceptionInterceptor) {
        data = bytes;
        offset = 0;
        length = bytes.length;
        this.exceptionInterceptor = exceptionInterceptor;
    }

    /**
     * Creates a blob with content.
     *
     * @param bytes
     *            the content for the blob.
     * @param offset
     *            offset
     * @param length
     *            length
     */
    public Blob(byte[] bytes, int offset, int length) {
        data = bytes;
        this.offset = offset;
        this.length = Math.min(bytes.length - offset, length);
    }

    /**
     * Create a BLOB with Locator
     * 
     * @param data
     */
    public Blob(boolean hasLocator, byte[] data, String encoding, OceanBaseConnection conn) {
        if (null != conn) {
            this.encoding = "UTF-8";
            /*
             * It only be used by getCharacterStream for Blob, and to decode the byte[] data to char[] Use the encoding
             * with the Connection's encoding's Name assigned by USER. For that it is user which encoding byte[] to
             * char[].
             */
            // this.encoding =
        } else if (null != encoding) {
            this.encoding = encoding; // not to be here
        }
        // readLong in 1.x is equivalent to readInt in 2.0,readLongLong in 1.x is equivalent to readLong in 2.0.
        { /* Has Locator with Blob */
            if (null != data && null != conn) {
                Buffer buffer = new Buffer(data);
                if (buffer.getLimit() >= ObLobLocator.OB_LOG_LOCATOR_HEADER) {
                    locator = new ObLobLocator();
                    locator.magicCode = buffer.readLongV1();
                    locator.version = buffer.readLongV1();
                    locator.snapshotVersion = buffer.readLongLongV1();
                    locator.tableId = buffer.getBytes(8);
                    locator.columnId = buffer.readLongV1();
                    locator.flags = buffer.readIntV1();
                    locator.option = buffer.readIntV1();
                    locator.payloadOffset = buffer.readLongV1();
                    locator.payloadSize = buffer.readLongV1();
                    locator.binaryData = buffer.getByteBuffer();
                    if (locator.payloadSize + locator.payloadOffset <= buffer.getLimit()
                            - ObLobLocator.OB_LOG_LOCATOR_HEADER) {
                        locator.rowId = buffer.getBytes((int) locator.payloadOffset); // row_id must be less than
                                                                                      // MAX_INT
                        this.data = (buffer.getBytes((int) locator.payloadSize)); // binary data less than MAX_INT
                        this.length = (int) locator.payloadSize;
                        locator.connection = conn;
                    }
                }
            }
        }
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        // if (offset != 0 || data.length != length) {
        // offset = 0;
        // length = 0;
        // }
        // ?
        out.writeInt(offset);
        out.writeInt(length);
        out.defaultWriteObject();
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        offset = in.readInt();
        length = in.readInt();
        in.defaultReadObject();
        // offset = 0;
        // length = data.length;
    }

    /**
     * Returns the number of bytes in the <code>BLOB</code> value designated by this <code>Blob</code> object.
     *
     * @return length of the <code>BLOB</code> in bytes
     */
    public long length() {
        return length;
    }

    /**
     * Retrieves all or part of the <code>BLOB</code> value that this <code>Blob</code> object represents, as an array
     * of bytes. This <code>byte</code> array contains up to <code>length
     * </code> consecutive bytes starting at position <code>pos</code>.
     *
     * @param pos
     *            the ordinal position of the first byte in the <code>BLOB</code> value to be extracted; the first byte
     *            is at position 1
     * @param length
     *            the number of consecutive bytes to be copied; the value for length must be 0 or greater
     * @return a byte array containing up to <code>length</code> consecutive bytes from the <code>BLOB
     *     </code> value designated by this <code>Blob</code> object, starting with the byte at position
     *         <code>pos</code>
     * @throws SQLException
     *             if there is an error accessing the <code>BLOB</code> value; if pos is less than 1 or length is less
     *             than 0
     * @see #setBytes
     * @since 1.2
     */
    public byte[] getBytes(final long pos, final int length) throws SQLException {
        if (pos < 1) {
            throw ExceptionFactory.INSTANCE
                    .create(String.format("Out of range (position should be > 0, but is %s)", pos));
        }
        final int offset = this.offset + (int) (pos - 1);
        int len = length > this.length ? this.length : length;
        byte[] result = new byte[len];
        System.arraycopy(data, offset, result, 0, Math.min(this.length - (int) (pos - 1), length));
        return result;
    }

    /**
     * Retrieves the <code>BLOB</code> value designated by this <code>Blob</code> instance as a stream.
     *
     * @return a stream containing the <code>BLOB</code> data
     * @throws SQLException
     *             if something went wrong
     * @see #setBinaryStream
     */
    public InputStream getBinaryStream() throws SQLException {
        return getBinaryStream(1, length);
    }

    /**
     * Returns an <code>InputStream</code> object that contains a partial <code>Blob</code> value, starting with the
     * byte specified by pos, which is length bytes in length.
     *
     * @param pos
     *            the offset to the first byte of the partial value to be retrieved. The first byte in the
     *            <code>Blob</code> is at position 1
     * @param length
     *            the length in bytes of the partial value to be retrieved
     * @return <code>InputStream</code> through which the partial <code>Blob</code> value can be read.
     * @throws SQLException
     *             if pos is less than 1 or if pos is greater than the number of bytes in the <code>Blob</code> or if
     *             pos + length is greater than the number of bytes in the <code>Blob
     *     </code>
     */
    public InputStream getBinaryStream(final long pos, final long length) throws SQLException {
        if (pos < 1) {
            throw ExceptionFactory.INSTANCE.create("Out of range (position should be > 0)");
        }
        if (pos - 1 > this.length) {
            throw ExceptionFactory.INSTANCE.create("Out of range (position > stream size)");
        }
        if (pos + length - 1 > this.length) {
            throw ExceptionFactory.INSTANCE.create("Out of range (position + length - 1 > streamSize)");
        }

        return new ByteArrayInputStream(data, this.offset + (int) pos - 1, (int) length);
    }

    public Reader getCharacterStream() throws SQLException {
        if (this.data != null) {
            return new StringReader(new String(this.data, Charset.forName(this.encoding)));
        }
        return null;
    }

    /**
     * Retrieves the byte position at which the specified byte array <code>pattern</code> begins within the
     * <code>BLOB</code> value that this <code>Blob</code> object represents. The search for <code>pattern</code> begins
     * at position <code>start</code>.
     *
     * @param pattern
     *            the byte array for which to search
     * @param start
     *            the position at which to begin searching; the first position is 1
     * @return the position at which the pattern appears, else -1
     */
    public long position(final byte[] pattern, final long start) throws SQLException {
        if (pattern.length == 0) {
            return 0;
        }
        if (start < 1) {
            throw ExceptionFactory.INSTANCE
                    .create(String.format("Out of range (position should be > 0, but is %s)", start));
        }
        if (start > this.length) {
            throw ExceptionFactory.INSTANCE.create("Out of range (start > stream size)");
        }

        outer: for (int i = (int) (offset + start - 1); i <= offset + this.length - pattern.length; i++) {
            for (int j = 0; j < pattern.length; j++) {
                if (data[i + j] != pattern[j]) {
                    continue outer;
                }
            }
            return i + 1 - offset;
        }
        return -1;
    }

    /**
     * Retrieves the byte position in the <code>BLOB</code> value designated by this <code>Blob</code> object at which
     * <code>pattern</code> begins. The search begins at position <code>start</code>.
     *
     * @param pattern
     *            the <code>Blob</code> object designating the <code>BLOB</code> value for which to search
     * @param start
     *            the position in the <code>BLOB</code> value at which to begin searching; the first position is 1
     * @return the position at which the pattern begins, else -1
     */
    public long position(final java.sql.Blob pattern, final long start) throws SQLException {
        byte[] blobBytes = pattern.getBytes(1, (int) pattern.length());
        return position(blobBytes, start);
    }

    /**
     * Writes the given array of bytes to the <code>BLOB</code> value that this <code>Blob</code> object represents,
     * starting at position <code>pos</code>, and returns the number of bytes written. The array of bytes will overwrite
     * the existing bytes in the <code>Blob</code> object starting at the position <code>pos</code>. If the end of the
     * <code>Blob</code> value is reached while writing the array of bytes, then the length of the <code>Blob</code>
     * value will be increased to accommodate the extra bytes.
     *
     * @param pos
     *            the position in the <code>BLOB</code> object at which to start writing; the first position is 1
     * @param bytes
     *            the array of bytes to be written to the <code>BLOB</code> value that this <code>
     *     Blob</code> object represents
     * @return the number of bytes written
     * @see #getBytes
     */
    public int setBytes(final long pos, final byte[] bytes) throws SQLException {
        if (pos < 1) {
            throw ExceptionFactory.INSTANCE.create("pos should be > 0, first position is 1.");
        }
        if (this.locator != null) {
            updateBlobToServer(pos, bytes, offset, bytes.length);
            return bytes.length;
        }
        final int arrayPos = (int) pos - 1;

        if (length > arrayPos + bytes.length) {
            System.arraycopy(bytes, 0, data, offset + arrayPos, bytes.length);
        } else {

            byte[] newContent = new byte[arrayPos + bytes.length];
            if (Math.min(arrayPos, length) > 0) {
                System.arraycopy(data, this.offset, newContent, 0, Math.min(arrayPos, length));
            }
            System.arraycopy(bytes, 0, newContent, arrayPos, bytes.length);
            data = newContent;
            length = arrayPos + bytes.length;
            offset = 0;
            charData = new String(data, Charset.forName(this.encoding == null ? "UTF-8" : this.encoding));
        }
        return bytes.length;
    }

    /**
     * Writes all or part of the given <code>byte</code> array to the <code>BLOB</code> value that this
     * <code>Blob</code> object represents and returns the number of bytes written. Writing starts at position
     * <code>pos</code> in the <code>BLOB</code> value; <code>len</code> bytes from the given byte array are written.
     * The array of bytes will overwrite the existing bytes in the <code>Blob</code> object starting at the position
     * <code>pos</code>. If the end of the <code>
     * Blob</code> value is reached while writing the array of bytes, then the length of the <code>
     * Blob</code> value will be increased to accommodate the extra bytes.
     *
     * <p>
     * <b>Note:</b> If the value specified for <code>pos</code> is greater then the length+1 of the <code>BLOB</code>
     * value then the behavior is undefined. Some JDBC drivers may throw a <code>
     * SQLException</code> while other drivers may support this operation.
     *
     * @param pos
     *            the position in the <code>BLOB</code> object at which to start writing; the first position is 1
     * @param bytes
     *            the array of bytes to be written to this <code>BLOB</code> object
     * @param offset
     *            the offset into the array <code>bytes</code> at which to start reading the bytes to be set
     * @param len
     *            the number of bytes to be written to the <code>BLOB</code> value from the array of bytes
     *            <code>bytes</code>
     * @return the number of bytes written
     * @throws SQLException
     *             if there is an error accessing the <code>BLOB</code> value or if pos is less than 1
     * @see #getBytes
     */
    public int setBytes(final long pos, final byte[] bytes, final int offset, final int len) throws SQLException {

        if (pos < 1) {
            throw ExceptionFactory.INSTANCE.create("pos should be > 0, first position is 1.");
        }
        if (this.locator != null) {
            updateBlobToServer(pos, bytes, offset, len);
            return len;
        }
        final int arrayPos = (int) pos - 1;
        final int byteToWrite = Math.min(bytes.length - offset, len);

        if (length > arrayPos + byteToWrite) {

            System.arraycopy(bytes, offset, data, this.offset + arrayPos, byteToWrite);

        } else {

            byte[] newContent = new byte[arrayPos + byteToWrite];
            if (Math.min(arrayPos, length) > 0) {
                System.arraycopy(data, this.offset, newContent, 0, Math.min(arrayPos, length));
            }
            System.arraycopy(bytes, offset, newContent, arrayPos, byteToWrite);
            data = newContent;
            length = arrayPos + byteToWrite;
            this.offset = 0;
            charData = new String(data, Charset.forName(this.encoding == null ? "UTF-8" : this.encoding));
        }

        return byteToWrite;
    }

    /**
     * Retrieves a stream that can be used to write to the <code>BLOB</code> value that this <code>
     * Blob</code> object represents. The stream begins at position <code>pos</code>. The bytes written to the stream
     * will overwrite the existing bytes in the <code>Blob</code> object starting at the position <code>pos</code>. If
     * the end of the <code>Blob</code> value is reached while writing to the stream, then the length of the
     * <code>Blob</code> value will be increased to accommodate the extra bytes.
     *
     * <p>
     * <b>Note:</b> If the value specified for <code>pos</code> is greater then the length+1 of the <code>BLOB</code>
     * value then the behavior is undefined. Some JDBC drivers may throw a <code>
     * SQLException</code> while other drivers may support this operation.
     *
     * @param pos
     *            the position in the <code>BLOB</code> value at which to start writing; the first position is 1
     * @return a <code>java.io.OutputStream</code> object to which data can be written
     * @throws SQLException
     *             if there is an error accessing the <code>BLOB</code> value or if pos is less than 1
     * @see #getBinaryStream
     * @since 1.4
     */
    public OutputStream setBinaryStream(final long pos) throws SQLException {
        if (pos < 1) {
            throw ExceptionFactory.INSTANCE.create("Invalid position in blob");
        }
        if (offset > 0) {
            byte[] tmp = new byte[length];
            System.arraycopy(data, offset, tmp, 0, length);
            data = tmp;
            offset = 0;
        }
        return new BlobOutputStream(this, (int) (pos - 1) + offset);
    }

    /**
     * Truncates the <code>BLOB</code> value that this <code>Blob</code> object represents to be <code>len</code> bytes
     * in length.
     *
     * @param len
     *            the length, in bytes, to which the <code>BLOB</code> value that this <code>Blob
     *     </code> object represents should be truncated
     */
    public void truncate(final long len) throws SQLException {
        if (locator != null) {
            trimBlobToServer((int) len);
        } else {
            if (len >= 0 && len < this.length) {
                this.length = (int) len;
            }
        }
    }

    /**
     * This method frees the <code>Blob</code> object and releases the resources that it holds. The object is invalid
     * once the <code>free</code> method is called.
     *
     * <p>
     * After <code>free</code> has been called, any attempt to invoke a method other than <code>
     * free</code> will result in a <code>SQLException</code> being thrown. If <code>free</code> is called multiple
     * times, the subsequent calls to <code>free</code> are treated as a no-op.
     */
    public void free() {
        this.data = new byte[0];
        this.offset = 0;
        this.length = 0;
    }

    /**
     * 
     * @return
     */
    public synchronized ObLobLocator getLocator() {
        return this.locator;
    }

    /**
     * Update <code>ObLobLocator</code> object which Bbob contains.
     *
     * @param locator
     *            Source <code>ObLobLocator</code> object.
     */
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
     * Update <code>Blob</code> object by the DBMS_LOB.write(?,?,?,?), starting with writeAt and write the bytes to
     * database.
     *
     * @param writeAt
     *            Start position to be write.
     * @param bytes
     *            byte[] data to be write.
     * @param offset
     *            first position offset will be write.
     * @param length
     *            date length to be write.
     * @throws SQLException
     *             if database error occur
     */
    public synchronized void updateBlobToServer(long writeAt, byte[] bytes, int offset, int length)
            throws SQLException {
        if (this.locator == null || this.locator.connection == null) {
            throw new SQLException("Invalid operation on closed BLOB");
        }

        java.sql.CallableStatement cstmt = this.locator.connection.prepareCall("{call DBMS_LOB.write( ?, ?, ?, ?)}");
        ((BasePrepareStatement) cstmt).setLobLocator(1, this);
        cstmt.setInt(2, length);
        cstmt.setInt(3, (int) writeAt);
        cstmt.setBytes(4, bytes);
        cstmt.registerOutParameter(1, Types.BLOB);
        cstmt.execute();

        Blob r = (Blob) cstmt.getBlob(1);
        if (r == null || r.getLocator() == null) {
            throw new SQLException("Invalid operator on setBytes for BLOB");
        } else {
            setLocator(r.locator);
            this.data = r.data;
        }

    }

    /**
     * Truncate <code>Blob</code> object by the DBMS_LOB.trim(?,?), truncate data from len, exception if len is big than
     * data's length and success when len is less than data's length.
     * 
     * @param len
     *            truncate data from len
     * @throws SQLException
     *             if database error occur
     */
    public synchronized void trimBlobToServer(int len) throws SQLException {
        if (this.locator == null || this.locator.connection == null) {
            // throw SQLError.createSQLException("Invalid operation on closed BLOB",
            // SQLError.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
            throw new SQLException("Invalid operation on closed BLOB");
        }

        java.sql.CallableStatement cstmt = this.locator.connection.prepareCall("{call DBMS_LOB.trim( ?, ?)}");
        ((BasePrepareStatement) cstmt).setLobLocator(1, this);
        cstmt.setInt(2, len);
        cstmt.registerOutParameter(1, Types.BLOB);
        cstmt.execute();

        Blob r = (Blob) cstmt.getBlob(1);
        if (r == null || r.getLocator() == null) {
            throw new SQLException("Invalid operator on trim() for BLOB");
        } else {
            setLocator(r.locator);
            this.data = r.data;
        }
    }
}
