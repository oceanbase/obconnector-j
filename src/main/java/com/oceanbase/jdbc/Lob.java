/**
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

import java.io.*;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.util.Arrays;

import com.oceanbase.jdbc.internal.com.read.Buffer;
import com.oceanbase.jdbc.internal.util.exceptions.ExceptionFactory;

class ObLobLocator {

    protected byte[]                       binaryData;  // binary to data to send
    protected long                         payloadSize; // for ObLobLocatorV1, it's payload_offset_;
                                                        // for ObLobLocatorV2, it's payload_offset_ or byte_size_.
    protected volatile OceanBaseConnection connection;

}

class ObLobLocatorV1 extends ObLobLocator {
    /**
     * The LobLocator for OceanBase Blob/Clob object. It will be send when the serverCapabilities's OB_CLIENT_SUPPORT_LOB_LOCATOR has been set. Server will
     * update data base on the <code>ObLobLocatorV1</code> object. Length for the <code>ObLobLocatorV1</code> is OB_LOG_LOCATOR_HEADER(40), and it has payloadOffset
     * length rowId, and the data left if data store on the database.
     * Such as:
     *      magic_code_:		21 42 4f 4c
     *      version_:			01 00 00 00
     *      snapshot_version_:	89 ae d4 cb 3f af 05 00
     *      table_id_:			7b c3 00 00 00 e9 03 00
     *      column_id_:		    11 00 00 00
     *      flags_：			00 00
     *      option_:			00 00
     *      payload_offset_:	11 00 00 00
     *      payload_size_:		03 00 00 00
     *      row_id_:		    2a 41 41 49 4b 41 51 41 41 41 41 41 41 41 41 41 3d
     *      data_(payload):	    61 62 63 #will be store to Blob/Clob object
     */

    protected long                         magicCode;                 // uint32_t magic_code_;
    protected long                         version;                   // uint32_t version_;
    protected long                         snapshotVersion;           // int64_t  snapshot_version_;
    protected byte[]                       tableId;                   // uint64_t table_id_;
    protected long                         columnId;                  // uint32_t column_id_;
    protected int                          flags;                     // uint16_t flags_;
    protected int                          option;                    // uint16_t option_;
    protected long                         payloadOffset;             // uint32_t payload_offset_;
//    protected long                         payloadSize;               // uint32_t payload_size_;
    protected byte[]                       rowId;                     // row_id length == payloadOffset

    public static int                      OB_LOG_LOCATOR_HEADER = 40;

    public ObLobLocatorV1() {}

}

// 8 bytes
class ObMemLobCommon {

    protected long magic;       // uint32_t. Consistent with V1
    // type, flags, version correspond to the old version in V1
    protected byte version;     // uint32_t: 8 bits. When version is 1, parse according to V1
    protected byte type;        // uint32_t: 4 bits. Persistent/TmpFull/TmpDelta
    protected int  flags;       // uint32_t: 20 bits

    ObMemLobCommon(long magicCode, long version) {
        this.magic = magicCode;
        this.version = (byte) (version & 0xff);
        this.type = (byte) (version >> 8 & 0x0f);
        this.flags = (int) (version >> 12 & 0xfffff);
    }

    public boolean hasInrowData() {
        return (flags & Flag.hasInRowData) == 1 << 1;
    }

    public boolean isSimple() {
        return (flags & Flag.isSimple) == 1 << 3;
    }

    public boolean hasExtern() {
        return (flags & Flag.hasExtern) == 1 << 4;
    }

    public static class Flag {
        public static final int readOnly = 1;
        public static final int hasInRowData = 1 << 1;
        public static final int isOpen   = 1 << 2;      // only persistent lob could be open
        public static final int isSimple  = 1 << 3;     // It is used when there is only ObMemLobCommon.
                                                        // When it is 0, it includes at least ObLobCommon (Disk), but it does not mean that there must be extern.
                                                        // is_simple is only used to optimize some scenarios of mysql.
        public static final int hasExtern = 1 << 4;
        // reserve 15 bits
    }

}

class ObLobLocatorV2 extends ObLobLocator {

    public static int MEM_LOB_COMMON_LENGTH = 8;
    public static int LOB_COMMON = 4;
    public static int MEM_LOB_EXTERN_HEADER_LENGTH = 32;

    ObMemLobCommon common;

    public ObLobLocatorV2() {}

}

public abstract class Lob implements Serializable {

    protected static int DBMS_LOB_MAX_AMOUNT = 32767;

    protected byte[]               data                 = null;
    protected transient int        offset;
    protected transient int        length;
    protected ObLobLocator         locator              = null;
    protected String               encoding             = null;
    protected String               charData             = null;
    protected int                  lengthFromServer     = -1;
    protected ExceptionInterceptor exceptionInterceptor = null;

    public Lob() {
        data = new byte[0];
        offset = 0;
        length = 0;
    }

    /**
     * Creates a blob with content.
     *
     * @param bytes the content for the blob.
     */
    public Lob(byte[] bytes) {
        data = bytes;
        offset = 0;
        length = bytes.length;
    }

    public Lob(byte[] bytes, ExceptionInterceptor exceptionInterceptor) {
        data = bytes;
        offset = 0;
        length = bytes.length;
        this.exceptionInterceptor = exceptionInterceptor;
    }

    /**
     * Creates a blob with content.
     *
     * @param bytes the content for the blob.
     * @param offset offset
     * @param length length
     */
    public Lob(byte[] bytes, int offset, int length) {
        data = bytes;
        this.offset = offset;
        this.length = Math.min(bytes.length - offset, length);
    }

    public void copy(Lob lob) {
        this.locator = lob.locator;
        this.data = lob.data;
        this.offset = lob.offset;
        this.length = lob.length;
        this.encoding = lob.encoding;
        this.charData = lob.charData;
        this.lengthFromServer = lob.lengthFromServer;
        this.exceptionInterceptor = lob.exceptionInterceptor;
    }

    public void clearData() {
        data = new byte[0];
        charData = null;
        offset = 0;
        length = 0;
    }

    public void buildObLobLocatorV1(boolean isBinary, Buffer buffer, long magicCode, long version, OceanBaseConnection conn) throws SQLException {
        ObLobLocatorV1 locator = new ObLobLocatorV1();
        locator.binaryData = buffer.getByteBuffer();

        locator.magicCode = magicCode;
        locator.version = version;
        locator.snapshotVersion = buffer.readLongV1();
        locator.tableId = buffer.readBytes(8);
        locator.columnId = buffer.readLong4BytesV1();
        locator.flags = buffer.readInt2BytesV1();
        locator.option = buffer.readInt2BytesV1();
        locator.payloadOffset = buffer.readLong4BytesV1();
        locator.payloadSize = buffer.readLong4BytesV1();

        if (locator.payloadSize + locator.payloadOffset <= buffer.remaining()) {
            locator.rowId = buffer.readBytes((int) locator.payloadOffset); // row_id must be less than MAX_INT
            readInRowLobData(isBinary, buffer, locator.payloadSize);
        }

        locator.connection = conn;
        this.locator = locator;
    }

    public void buildObLobLocatorV2(boolean isBinary, Buffer buffer, long magicCode, long version, OceanBaseConnection conn) throws SQLException {
        ObLobLocatorV2 locator = new ObLobLocatorV2();
        ObMemLobCommon common = new ObMemLobCommon(magicCode, version);

        if (common.hasInrowData()) {
            if (common.hasExtern()) {
                // [ObMemLobCommon 8 bytes][ObMemLobExternHeader 32 bytes]<---- payload_offset ---->[data]
                buffer.skipBytes(24); // ObMemLobExternHeader: reserved_, table_id_, column_idx_, flags_, rowkey_size_
                long payloadOffset = buffer.readLong4BytesV1();
                locator.payloadSize = buffer.readLong4BytesV1();

                if (locator.payloadSize + payloadOffset <= buffer.remaining()) {
                    buffer.skipBytes((int) payloadOffset);

                    readInRowLobData(isBinary, buffer, locator.payloadSize);
                }
            } else if (common.isSimple()) {
                // [ObMemLobCommon 8 bytes][data]
                locator.payloadSize = buffer.remaining();
                readInRowLobData(isBinary, buffer, locator.payloadSize);
            } else if (!common.hasExtern() && !common.isSimple()) {
                buffer.skipByte(); // ObLobCommon.version
                int lobCommonFlag = buffer.readInt3Bytes();

                // uint32_t is_init_ : 1
                if ((lobCommonFlag & 1) == 0) {
                    // [ObMemLobCommon 8 bytes][ObLobCommon 4 bytes : is_init_ == 0][data]
                    locator.payloadSize = buffer.remaining();
                    readInRowLobData(isBinary, buffer, locator.payloadSize);
                } else {
                    // [ObMemLobCommon 8 bytes][ObLobCommon 4 bytes : is_init_ == 1][ObLobData 24 bytes][data]
                    buffer.skipBytes(16); // ObLobData.id_
                    locator.payloadSize = buffer.readLong(); // ObLobData.byte_size_

                    readInRowLobData(isBinary, buffer, locator.payloadSize);
                }
            }
        } else {
            // out-row data
            if (common.hasExtern()) {
                // [ObMemLobCommon : has_extern == 1][ObMemLobExternHeader : rowkey_size][uint16 : ex_size]<--- ex_size+rowkey_size --->[ObLobCommon][ObLobData]
                buffer.skipBytes(22); // ObMemLobExternHeader: reserved_, table_id_, column_idx_, flags_
                int rowkeySize = buffer.readInt2Bytes();
                buffer.skipBytes(8); // ObMemLobExternHeader: payload_offset_, payload_size_

                int exSize = buffer.readInt2Bytes();
                buffer.skipBytes(rowkeySize + exSize);

                buffer.skipBytes(4); // ObLobCommon

                buffer.skipBytes(16); // ObLobData.id_
                locator.payloadSize = buffer.readLong(); // ObLobData.byte_size_
            } else {
                // [ObMemLobCommon : has_extern == 1][ObLobCommon 4 bytes][ObLobData 24 bytes]
                buffer.skipBytes(4); // ObLobCommon

                buffer.skipBytes(16); // ObLobData.id_
                locator.payloadSize = buffer.readLong(); // ObLobData.byte_size_
            }
        }

        locator.binaryData = buffer.getByteBuffer();
        locator.common = common;
        locator.connection = conn;
        this.locator = locator;
    }

    private void readInRowLobData(boolean isBinary, Buffer buffer, long lobLength) throws SQLException {
        if (isBinary) {
            data = buffer.readBytes((int) lobLength);
            length = data.length;
        } else {
            try {
                if (lobLength <= Clob.maxLength) {
                    charData = new String(buffer.readBytes((int) lobLength), this.encoding);
                    length = charData.length();
                } else {
                    throw new SQLException("Exceed max length of Clob for support " + Clob.maxLength + " current " + lobLength);
                }
            } catch (UnsupportedEncodingException e) {
                throw new SQLException("Unsupported character encoding " + this.encoding);
            }
        }
    }

    /**
     * Retrieves a stream that can be used to write to the <code>BLOB</code> value that this <code>
     * Blob</code> object represents. The stream begins at position <code>pos</code>. The bytes
     * written to the stream will overwrite the existing bytes in the <code>Blob</code> object
     * starting at the position <code>pos</code>. If the end of the <code>Blob</code> value is reached
     * while writing to the stream, then the length of the <code>Blob</code> value will be increased
     * to accommodate the extra bytes.
     *
     * <p><b>Note:</b> If the value specified for <code>pos</code> is greater then the length+1 of the
     * <code>BLOB</code> value then the behavior is undefined. Some JDBC drivers may throw a <code>
     * SQLException</code> while other drivers may support this operation.
     *
     * @param pos the position in the <code>BLOB</code> value at which to start writing; the first
     *     position is 1
     * @return a <code>java.io.OutputStream</code> object to which data can be written
     * @throws SQLException if there is an error accessing the <code>BLOB</code> value or if pos is
     *     less than 1
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
        return new LobOutputStream(this, (int) (pos - 1) + offset);
    }

    public InputStream getBinaryStream() throws SQLException {
        return getBinaryStream(1, length);
    }

    public InputStream getBinaryStream(final long pos, final long length) throws SQLException {
        if (pos < 1) {
            throw ExceptionFactory.INSTANCE.create("Out of range (position should be > 0)");
        }
        if (pos - 1 > this.length) {
            throw ExceptionFactory.INSTANCE.create("Out of range (position > stream size)");
        }
        if (pos + length - 1 > this.length) {
            throw ExceptionFactory.INSTANCE
                .create("Out of range (position + length - 1 > streamSize)");
        }

        return new ByteArrayInputStream(data, this.offset + (int) pos - 1, (int) length);
    }

    public int setBytes(final long pos, final byte[] bytes) throws SQLException {
        if (pos < 1) {
            throw ExceptionFactory.INSTANCE.create("pos should be > 0, first position is 1.");
        }
        //      if (this.locator != null) {
        //        updateBlobToServer(pos, bytes, offset, bytes.length);
        //        return bytes.length;
        //      }
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
        }
        charData = new String(data,
            Charset.forName(this.encoding == null ? "UTF-8" : this.encoding));
        return bytes.length;
    }

    /**
     * This method frees the <code>Blob</code> object and releases the resources that it holds. The
     * object is invalid once the <code>free</code> method is called.
     *
     * <p>After <code>free</code> has been called, any attempt to invoke a method other than <code>
     * free</code> will result in a <code>SQLException</code> being thrown. If <code>free</code> is
     * called multiple times, the subsequent calls to <code>free</code> are treated as a no-op.
     */
    public void free() {
        this.data = new byte[0];
        this.offset = 0;
        this.length = 0;
    }

    public boolean isEmptyLob() {
        if (this.locator != null) {
            return locator.payloadSize == 0;
        } else {
            return ((data == null || data.length == 0) && (charData == null || charData.length() == 0));
        }
    }

    public byte[] getBytes(final long pos, final int length) throws SQLException {
        if (pos < 1) {
            throw ExceptionFactory.INSTANCE.create(String.format(
                    "Out of range (position should be > 0, but is %s)", pos));
        }

        // out-row of lob locator v2, needs to be read from server by procedure DBMS_LOB.READ
        if (this.length == 0 && !isEmptyLob()) {
            if (locator != null && locator instanceof ObLobLocatorV2) {
                readFromServer();
            }
        }

        final int offset = this.offset + (int) (pos - 1);
        int len = length > this.length ? this.length : length;
        byte[] result = new byte[len];
        System.arraycopy(data, offset, result, 0, Math.min(this.length - (int) (pos - 1), length));
        return result;
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        out.writeInt(offset);
        out.writeInt(length);
        out.defaultWriteObject();
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        offset = in.readInt();
        length = in.readInt();
        in.defaultReadObject();
    }

    public synchronized ObLobLocator getLocator() {
        return this.locator;
    }

    abstract protected void readFromServer() throws SQLException;

}
