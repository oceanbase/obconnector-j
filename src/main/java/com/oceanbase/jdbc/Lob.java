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

import com.oceanbase.jdbc.internal.util.exceptions.ExceptionFactory;

/**
 * The LobLocator for OceanBase Blob/Clob object. It will be send when the serverCapabilities's OB_CLIENT_SUPPORT_LOB_LOCATOR has been set. Server will
 * update data base on the <code>ObLobLocator</code> object. Length for the <code>ObLobLocator</code> is OB_LOG_LOCATOR_HEADER(40), and it has payloadOffset
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
 *
 */

class ObLobLocator {
    protected long                         magicCode;                 // uint32_t magic_code_;
    protected long                         version;                   // uint32_t version_;
    protected long                         snapshotVersion;           // int64_t  snapshot_version_;
    protected byte[]                       tableId;                   // uint64_t table_id_;
    protected long                         columnId;                  // uint32_t column_id_;
    protected int                          flags;                     // uint16_t flags_;
    protected int                          option;                    // uint16_t option_;
    protected long                         payloadOffset;             // uint32_t payload_offset_;
    protected long                         payloadSize;               // uint32_t payload_size_;
    protected byte[]                       rowId;                     // row_id length == payloadOffset
    protected byte[]                       binaryData;                // binary to data to send
    public static int                      OB_LOG_LOCATOR_HEADER = 40;
    protected volatile OceanBaseConnection connection;
}

public class Lob implements Serializable {
    protected byte[]               data                 = null;
    protected transient int        offset;
    protected transient int        length;
    protected ObLobLocator         locator              = null;
    protected String               encoding             = null;
    protected String               charData             = null;
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
            if (data == null || data.length == 0) {
                return true;
            }
        }
        return false;
    }

    public byte[] getBytes(final long pos, final int length) throws SQLException {
        if (pos < 1) {
            throw ExceptionFactory.INSTANCE.create(String.format(
                "Out of range (position should be > 0, but is %s)", pos));
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
}
