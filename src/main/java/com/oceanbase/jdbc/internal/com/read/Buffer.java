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
package com.oceanbase.jdbc.internal.com.read;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class Buffer {

    public byte[]              buf;
    public int                 position;
    public int                 limit;
    static final long          NULL_LENGTH      = -1;
    public final static byte[] EMPTY_BYTE_ARRAY = new byte[0];

    public Buffer(final byte[] buf, int limit) {
        this.buf = buf;
        this.limit = limit;
    }

    /**
     * Constructor with default limit and offset.
     *
     * @param buf byte array
     */
    public Buffer(final byte[] buf) {
        this.buf = buf;
        this.limit = this.buf.length;
    }

    public Buffer() {

    }

    public int remaining() {
        return limit - position;
    }

    public void checkRemainder(long need) throws IOException {
        if (remaining() < need) {
            throw new IOException("Buffer will overflow: need " + need + " bytes, but remain "
                                  + remaining() + " bytes.");
        }
    }

    /**
     * Reads a string from the buffer, looks for a 0 to end the string.
     *
     * @param charset the charset to use, for example ASCII
     * @return the read string
     */
    public String readStringNullEnd(final Charset charset) {
        int initialPosition = position;
        int cnt = 0;
        while (remaining() > 0 && (buf[position++] != 0)) {
            cnt++;
        }
        return new String(buf, initialPosition, cnt, charset);
    }

    /**
     * Reads a byte array from the buffer, looks for a 0 to end the array.
     *
     * @return the read array
     */
    public byte[] readBytesNullEnd() {
        int initialPosition = position;
        int cnt = 0;
        while (remaining() > 0 && (buf[position++] != 0)) {
            cnt++;
        }
        final byte[] tmpArr = new byte[cnt];
        System.arraycopy(buf, initialPosition, tmpArr, 0, cnt);
        return tmpArr;
    }

    /**
     * Read String with defined length.
     *
     * @param numberOfBytes raw data length.
     * @return String value
     */
    public String readString(final int numberOfBytes) {
        position += numberOfBytes;
        return new String(buf, position - numberOfBytes, numberOfBytes);
    }

    /**
     * Read a short (2 bytes) from the buffer.
     *
     * @return an short
     */
    public short readShort() {
        return (short) ((buf[position++] & 0xff) + ((buf[position++] & 0xff) << 8));
    }

    /**
     * Read a int (2 bytes) from the buffer.
     *
     * @return a int
     */
    public int readInt2() {
        return (buf[this.position++] & 0xff) | ((buf[this.position++] & 0xff) << 8);
    }

    public int readIntV1() {
        byte[] b = this.buf; // a little bit optimization

        return (b[this.position++] & 0xff) | ((b[this.position++] & 0xff) << 8);
    }

    /**
     * Read 24 bit integer.
     *
     * @return length
     */
    public int readInt3() {
        return (buf[position++] & 0xff) + ((buf[position++] & 0xff) << 8)
               + ((buf[position++] & 0xff) << 16);
    }

    public int read24bitword() {
        return (buf[position++] & 0xff) + ((buf[position++] & 0xff) << 8)
               + ((buf[position++] & 0xff) << 16);
    }

    /**
     * Read a int (4 bytes) from the buffer.
     *
     * @return a int
     */
    public int readInt4() {
        return ((buf[position++] & 0xff) | ((buf[position++] & 0xff) << 8)
                + ((buf[position++] & 0xff) << 16) | ((buf[position++] & 0xff) << 24));
    }

    public int readInt() {
        return ((buf[position++] & 0xff) + ((buf[position++] & 0xff) << 8)
                + ((buf[position++] & 0xff) << 16) + ((buf[position++] & 0xff) << 24));
    }

    public int readnBytes() {
        int sw = this.buf[this.position++] & 0xff;

        switch (sw) {
            case 1:
                return this.buf[this.position++] & 0xff;

            case 2:
                return this.readInt4();

            case 3:
                return this.readInt3();

            case 4:
                return (int) this.readLong4();

            default:
                return 255;
        }
    }

    /**
     * Read a long (4 bytes) from the buffer.
     *
     * @return a long
     */
    public long readLong4() {
        byte[] b = this.buf;

        return ((long) b[this.position++] & 0xff) | (((long) b[this.position++] & 0xff) << 8)
               | ((long) (b[this.position++] & 0xff) << 16)
               | ((long) (b[this.position++] & 0xff) << 24);
    }

    public long readLongV1() {
        byte[] b = this.buf;

        return ((long) b[this.position++] & 0xff) | (((long) b[this.position++] & 0xff) << 8)
               | ((long) (b[this.position++] & 0xff) << 16)
               | ((long) (b[this.position++] & 0xff) << 24);
    }

    /**
     * Read a long (8 bytes) from the buffer.
     *
     * @return a long
     */
    public long readLong8() {
        return ((buf[position++] & 0xff) + ((long) (buf[position++] & 0xff) << 8)
                + ((long) (buf[position++] & 0xff) << 16) + ((long) (buf[position++] & 0xff) << 24)
                + ((long) (buf[position++] & 0xff) << 32) + ((long) (buf[position++] & 0xff) << 40)
                + ((long) (buf[position++] & 0xff) << 48) + ((long) (buf[position++] & 0xff) << 56));
    }

    public long readLongLongV1() {
        byte[] b = this.buf;

        return (b[this.position++] & 0xff) | ((long) (b[this.position++] & 0xff) << 8)
               | ((long) (b[this.position++] & 0xff) << 16)
               | ((long) (b[this.position++] & 0xff) << 24)
               | ((long) (b[this.position++] & 0xff) << 32)
               | ((long) (b[this.position++] & 0xff) << 40)
               | ((long) (b[this.position++] & 0xff) << 48)
               | ((long) (b[this.position++] & 0xff) << 56);
    }

    public long readLong() {
        return ((buf[position++] & 0xff) + ((long) (buf[position++] & 0xff) << 8)
                + ((long) (buf[position++] & 0xff) << 16) + ((long) (buf[position++] & 0xff) << 24)
                + ((long) (buf[position++] & 0xff) << 32) + ((long) (buf[position++] & 0xff) << 40)
                + ((long) (buf[position++] & 0xff) << 48) + ((long) (buf[position++] & 0xff) << 56));
    }

    /**
     * Reads a byte from the buffer.
     *
     * @return the byte
     */
    public byte readByte() {
        return buf[position++];
    }

    public byte[] readLenByteArray(int offset) {
        long len = this.readFieldLength();

        if (len == NULL_LENGTH) {
            return null;
        }

        if (len == 0) {
            return EMPTY_BYTE_ARRAY;
        }

        this.position += offset;

        return readBytes((int) len);
    }

    /**
     * Read current buffer byte without incrementing position.
     *
     * @return the byte
     */
    public byte getByte() {
        return buf[position];
    }

    public void skipByte() {
        position++;
    }

    public byte getByteAt(final int position) {
        return buf[position];
    }

    /**
     * Read raw data.
     *
     * @param numberOfBytes raw data length.
     * @return raw data
     */
    public byte[] readBytes(final int numberOfBytes) {
        final byte[] tmpArr = new byte[numberOfBytes];
        System.arraycopy(buf, position, tmpArr, 0, numberOfBytes);
        position += numberOfBytes;
        return tmpArr;
    }

    public byte[] readRawBytes(final int numberOfBytes) {
        final byte[] tmpArr = new byte[numberOfBytes];
        System.arraycopy(buf, position, tmpArr, 0, numberOfBytes);
        position += numberOfBytes;
        return tmpArr;
    }

    public byte[] getBytes(int len) {
        byte[] b = new byte[len];
        System.arraycopy(this.buf, this.position, b, 0, len);

        return b;
    }

    public byte[] getBytes(int offset, int len) {
        byte[] dest = new byte[len];
        System.arraycopy(this.buf, offset, dest, 0, len);

        return dest;
    }

    public void skipBytes(final int bytesToSkip) {
        position += bytesToSkip;
    }

    /**
     * Get next data bytes with length encoded prefix.
     *
     * @return the raw binary data
     */
    public byte[] getLengthEncodedBytes() {
        int length = (int) getLengthEncodedNumeric();
        if (length == -1) {
            return null;
        }

        byte[] tmpBuf = new byte[length];
        System.arraycopy(buf, position, tmpBuf, 0, length);
        position += length;
        return tmpBuf;
    }

    /** Skip next length encode binary data. */
    public void skipLengthEncodedBytes() {
        long length = getLengthEncodedNumeric();
        if (length == -1) {
            return;
        }
        position += length;
    }

    /**
     * Get next binary data length.
     *
     * @return length of next binary data
     */
    public long getLengthEncodedNumeric() {
        int type = this.buf[this.position++] & 0xff;
        switch (type) {
            case 251:
                return -1;
            case 252:
                return 0xffff & readShort();
            case 253:
                return 0xffffff & readInt3();
            case 254:
                return readLong8();
            default:
                return type;
        }
    }

    /** Skip length encoded numeric */
    public void skipLengthEncodedNumeric() {
        int type = this.buf[this.position++] & 0xff;
        switch (type) {
            case 252:
                this.position += 2;
                return;
            case 253:
                this.position += 3;
                return;
            case 254:
                this.position += 8;
                return;
            default:
                return;
        }
    }

    /**
     * Get next data bytes from length encoded prefix.
     *
     * @return buffer
     */
    public Buffer getLengthEncodedBuffer() {
        return new Buffer(getLengthEncodedBytes());
    }

    /**
     * Reads length-encoded string.
     *
     * @param charset the charset to use, for example ASCII
     * @return the read string
     */
    public String readStringLengthEncoded(final Charset charset) {
        int length = (int) getLengthEncodedNumeric();
        String string = new String(buf, position, length, charset);
        position += length;
        return string;
    }

    public int fastSkipLenString() {
        long len = this.readFieldLength();

        this.position += len;

        return (int) len;
    }

    public final long readFieldLength() {
        int sw = this.buf[this.position++] & 0xff;

        switch (sw) {
            case 251:
                return NULL_LENGTH;

            case 252:
                return readInt2();

            case 253:
                return readInt3();

            case 254:
                return readLong8();

            default:
                return sw;
        }
    }

    public void writeByte(byte b) {
        while (remaining() < 1) {
            grow();
        }
        buf[position++] = b;
    }

    public void writeBytes(byte[] bytes, int off, int length) {
        while (remaining() < length) {
            grow();
        }
        System.arraycopy(bytes, off, buf, position, length);
        position += length;
    }

    /**
     * Write bytes.
     *
     * @param header header byte
     * @param bytes command bytes
     */
    public void writeBytes(byte header, byte[] bytes) {
        int length = bytes.length;
        while (remaining() < length + 10) {
            grow();
        }
        writeLength(length + 1);
        buf[position++] = header;
        System.arraycopy(bytes, 0, buf, position, length);
        position += length;
    }

    public void writeShort(short num) {
        while (remaining() < 2) {
            grow();
        }
        buf[position++] = (byte) num;
        buf[position++] = (byte) (num >>> 8);
    }

    public void writeShortV2(short num) {
        while (remaining() < 2) {
            grow();
        }
        buf[position++] = (byte) (num >>> 8);
        buf[position++] = (byte) num;
    }

    public void writeLongInt(int num) {
        while (remaining() < 3) {
            grow();
        }
        buf[position++] = (byte) num;
        buf[position++] = (byte) (num >>> 8);
        buf[position++] = (byte) (num >>> 16);
    }

    public void writeInt(int num) {
        while (remaining() < 4) {
            grow();
        }
        buf[position++] = (byte) num;
        buf[position++] = (byte) (num >>> 8);
        buf[position++] = (byte) (num >>> 16);
        buf[position++] = (byte) (num >>> 24);
    }

    public void writeIntV2(int num) {
        while (remaining() < 4) {
            grow();
        }
        buf[position++] = (byte) (num >>> 24);
        buf[position++] = (byte) (num >>> 16);
        buf[position++] = (byte) (num >>> 8);
        buf[position++] = (byte) num;
    }

    public void writeNBytes(long num, int n) {
        while (remaining() < n) {
            grow();
        }

        for (int i = 0; i < n; i++) {
            buf[position++] = (byte) (num >>> 8 * i);
        }
    }

    public void writeLongFromLowToHigh(long num) {
        while (remaining() < 8) {
            grow();
        }
        buf[position++] = (byte) num;
        buf[position++] = (byte) (num >>> 8);
        buf[position++] = (byte) (num >>> 16);
        buf[position++] = (byte) (num >>> 24);
        buf[position++] = (byte) (num >>> 32);
        buf[position++] = (byte) (num >>> 40);
        buf[position++] = (byte) (num >>> 48);
        buf[position++] = (byte) (num >>> 56);
    }

    public void writeLongFromHighToLow(long num) {
        while (remaining() < 8) {
            grow();
        }
        buf[position++] = (byte) ((num >>> 56) & 0xff);
        buf[position++] = (byte) ((num >>> 48) & 0xff);
        buf[position++] = (byte) ((num >>> 40) & 0xff);
        buf[position++] = (byte) ((num >>> 32) & 0xff);
        buf[position++] = (byte) ((num >>> 24) & 0xff);
        buf[position++] = (byte) ((num >>> 16) & 0xff);
        buf[position++] = (byte) ((num >>> 8) & 0xff);
        buf[position++] = (byte) (num & 0xff);
    }

    public void writeString(String str) {
        int length = str.length();
        while (remaining() < length) {
            grow();
        }
        System.arraycopy(str.getBytes(StandardCharsets.UTF_8), 0, buf, position, length);
        position += length;
    }

    /**
     * Write value with length encoded prefix.
     *
     * @param value value to write
     */
    public void writeStringLength(String value, Charset charset) {
        byte[] bytes = value.getBytes(charset);
        int length = bytes.length;
        while (remaining() < length + 9) {
            grow();
        }
        writeLength(length);
        System.arraycopy(bytes, 0, buf, position, length);
        position += length;
    }

    /**
     * Write value with length encoded prefix.
     *
     * @param bytes value to write
     */
    public void writeStringLength(byte[] bytes) {
        int length = bytes.length;
        while (remaining() < length + 9) {
            grow();
        }
        writeLength(length);
        System.arraycopy(bytes, 0, buf, position, length);
        position += length;
    }

    /**
     * Write value with length encoded prefix. value length MUST be less than 251 char
     *
     * @param value value to write
     */
    public void writeStringSmallLength(byte[] value) {
        int length = value.length;
        while (remaining() < length + 1) {
            grow();
        }
        buf[position++] = (byte) length;
        System.arraycopy(value, 0, buf, position, length);
        position += length;
    }

    /**
     * Write length.
     *
     * @param length length
     */
    public void writeLength(long length) {
        if (length < 251) {
            buf[position++] = (byte) length;
        } else if (length < 65536) {
            buf[position++] = (byte) 0xfc;
            buf[position++] = (byte) length;
            buf[position++] = (byte) (length >>> 8);
        } else if (length < 16777216) {
            buf[position++] = (byte) 0xfd;
            buf[position++] = (byte) length;
            buf[position++] = (byte) (length >>> 8);
            buf[position++] = (byte) (length >>> 16);
        } else {
            buf[position++] = (byte) 0xfe;
            buf[position++] = (byte) length;
            buf[position++] = (byte) (length >>> 8);
            buf[position++] = (byte) (length >>> 16);
            buf[position++] = (byte) (length >>> 24);
            buf[position++] = (byte) (length >>> 32);
            buf[position++] = (byte) (length >>> 40);
            buf[position++] = (byte) (length >>> 48);
            buf[position++] = (byte) (length >>> 54);
        }
    }

    /** Grow data array. */
    private void grow() {
        int newCapacity = buf.length + (buf.length >> 1);
        if (newCapacity - (Integer.MAX_VALUE - 8) > 0) {
            newCapacity = Integer.MAX_VALUE - 8;
        }
        buf = Arrays.copyOf(buf, newCapacity);
        this.limit = newCapacity;
    }

    public int getPosition() {
        return position;
    }

    public void setPosition(int position) {
        this.position = position;
    }

    public int getLimit() {
        return limit;
    }

    /**
     * Returns the array of bytes this Buffer is using to read from.
     *
     * @return byte array being read from
     */
    public byte[] getByteBuffer() {
        return buf;
    }
}
