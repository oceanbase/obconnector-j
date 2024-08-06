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
package com.oceanbase.jdbc.internal.io.output;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.TimeZone;

import com.oceanbase.jdbc.internal.io.LruTraceCache;
import com.oceanbase.jdbc.internal.protocol.TimeTrace;
import com.oceanbase.jdbc.internal.util.Utils;
import com.oceanbase.jdbc.internal.util.exceptions.MaxAllowedPacketException;

public abstract class AbstractPacketOutputStream extends FilterOutputStream implements
                                                                           PacketOutputStream {

    protected long            threadId;
    protected int             mysqlSeqNo                 = 0;
    protected LruTraceCache   traceCache                 = null;
    protected String          serverThreadLog            = "";
    protected final int       maxQuerySizeToLog;
    protected boolean         enableNetworkStatistics    = false;
    protected long            timestampBeforeFlush       = 0;

    private static final byte QUOTE                      = (byte) '\'';
    private static final byte DBL_QUOTE                  = (byte) '"';
    private static final byte ZERO_BYTE                  = (byte) '\0';
    private static final byte BACKSLASH                  = (byte) '\\';
    private static final int  SMALL_BUFFER_SIZE          = 8192;
    private static final int  MEDIUM_BUFFER_SIZE         = 128 * 1024;
    private static final int  LARGE_BUFFER_SIZE          = 1024 * 1024;

    protected byte[]          buf;
    /**
     relative position in mysql payload
     */
    protected int             pos;
    protected long            cmdLength;
    protected int             maxPacketLength;
    protected int             maxAllowedPacket           = Integer.MAX_VALUE;
    protected boolean         permitTrace;
    private int               mark                       = -1;
    private boolean           bufferContainDataAfterMark = false;
    private TimeZone          tm                         = null;
    protected Charset         charset                    = null;
    private TimeTrace         timeTrace;
    private boolean           encloseParamInParentheses;
    private boolean           isOracleMode;

    /**
     * Common feature to write data into socket, creating MariaDB Packet.
     *
     * @param out socket outputStream
     * @param maxQuerySizeToLog maximum query size to log
     * @param threadId thread id
     */
    public AbstractPacketOutputStream(OutputStream out, int maxQuerySizeToLog, long threadId,
                                      String encoding, TimeTrace timeTrace, boolean encloseParamInParentheses) {
        super(out);
        buf = new byte[SMALL_BUFFER_SIZE];
        this.maxQuerySizeToLog = maxQuerySizeToLog;
        cmdLength = 0;
        this.threadId = threadId;
        this.charset = Charset.forName(encoding);
        this.timeTrace = timeTrace;
        this.encloseParamInParentheses = encloseParamInParentheses;
    }

    public void setTimeZone(TimeZone tm) {
        this.tm = tm;
    }

    public TimeZone getTimeZone() {
        return tm;
    }

    public int getMaxPacketLength() {
        return maxPacketLength;
    }

    public abstract void startPacket(int seqNo);

    public void enableNetworkStatistics(boolean flag) {
        enableNetworkStatistics = flag;
        timestampBeforeFlush = 0;
    }

    public long getTimestampBeforeFlush() {
        return timestampBeforeFlush;
    }

    public void clearNetworkStatistics() {
        timestampBeforeFlush = 0;
    }

    public Charset getCharset() {
        return charset;
    }

    protected abstract void flushBuffer(boolean commandEnd) throws IOException;

    /**
     * Buffer growing use 4 size only to avoid creating/copying that are expensive operations.
     * possible size
     *
     * <ol>
     *   <li>SMALL_BUFFER_SIZE = 8k (default)
     *   <li>MEDIUM_BUFFER_SIZE = 128k
     *   <li>LARGE_BUFFER_SIZE = 1M
     *   <li>getMaxPacketLength = 16M (+ 4 is using no compression)
     * </ol>
     *
     * @param len length to add
     */
    private void growBuffer(int len) throws IOException {
        int bufferLength = buf.length;
        int newCapacity;
        if (bufferLength == SMALL_BUFFER_SIZE) {
            if (len + pos < MEDIUM_BUFFER_SIZE) {
                newCapacity = MEDIUM_BUFFER_SIZE;
            } else if (len + pos < LARGE_BUFFER_SIZE) {
                newCapacity = LARGE_BUFFER_SIZE;
            } else {
                newCapacity = getMaxPacketLength();
            }
        } else if (bufferLength == MEDIUM_BUFFER_SIZE) {
            if (len + pos < LARGE_BUFFER_SIZE) {
                newCapacity = LARGE_BUFFER_SIZE;
            } else {
                newCapacity = getMaxPacketLength();
            }
        } else if (bufferContainDataAfterMark) {
            // want to add some information to buffer without having the command Header
            // must grow buffer until having all the query
            newCapacity = Math.max(len + pos, getMaxPacketLength());
        } else {
            newCapacity = getMaxPacketLength();
        }

        if (mark != -1 && len + pos > newCapacity) {
            // buffer is > 16M with mark.
            // flush until mark, reset pos at beginning
            flushBufferStopAtMark();

            if (len + pos <= bufferLength) {
                return;
            }

            // need to keep all data, buffer can grow more than maxPacketLength
            // grow buffer if needed
            if (len + pos > newCapacity) {
                newCapacity = len + pos;
            }
        }

        byte[] newBuf = new byte[newCapacity];
        System.arraycopy(buf, 0, newBuf, 0, pos);
        buf = newBuf;
    }

    /**
     * Send empty packet.
     *
     * @param seqNo packet sequence
     * @throws IOException if socket error occur.
     */
    public void writeEmptyPacket(int seqNo) throws IOException {
        startPacket(seqNo);
        writeEmptyPacket();
        out.flush();
        cmdLength = 0;
    }

    public abstract void writeEmptyPacket() throws IOException;

    /**
     * Send packet to socket.
     *
     * @throws IOException if socket error occur.
     */
    public void flush() throws IOException {
        flushBuffer(true);
        doFlush();
    }

    public void flushLoadDataLocalFile() throws IOException {
        flushBuffer(false);
        doFlush();
    }

    public void doFlush() throws IOException {
        out.flush();

        // if buffer is big, and last query doesn't use at least half of it, resize buffer to default
        // value
        if (buf.length > SMALL_BUFFER_SIZE && cmdLength * 2 < buf.length) {
            buf = new byte[SMALL_BUFFER_SIZE];
        }

        endSendRequest();
        if (cmdLength >= maxAllowedPacket) {
            throw new MaxAllowedPacketException("query size (" + cmdLength
                                                + ") is >= to max_allowed_packet ("
                                                + maxAllowedPacket + ")", true);
        }
    }

    public boolean checkRemainingSize(int len) {
        return getMaxPacketLength() - pos > len;
    }

    /**
     * Count query size. If query size is greater than max_allowed_packet and nothing has been already
     * send, throw an exception to avoid having the connection closed.
     *
     * @param length additional length to query size
     * @throws MaxAllowedPacketException if query has not to be send.
     */
    public void checkMaxAllowedLength(int length) throws MaxAllowedPacketException {
        if (cmdLength + length >= maxAllowedPacket && cmdLength == 0) {
            // launch exception only if no packet has been send.
            throw new MaxAllowedPacketException("query size (" + (cmdLength + length)
                                                + ") is >= to max_allowed_packet ("
                                                + maxAllowedPacket + ")", false);
        }
    }

    public boolean exceedMaxLength() {
        return cmdLength + (pos - initialPacketPos()) >= maxAllowedPacket;
    }

    public OutputStream getOutputStream() {
        return out;
    }

    /**
     * Write short value into buffer. flush buffer if too small.
     *
     * @param value short value
     * @throws IOException if socket error occur
     */
    public void writeShort(short value) throws IOException {
        if (2 > buf.length - pos) {
            // not enough space remaining
            byte[] arr = new byte[2];
            arr[0] = (byte) value;
            arr[1] = (byte) (value >> 8);
            write(arr, 0, 2);
            return;
        }

        buf[pos] = (byte) value;
        buf[pos + 1] = (byte) (value >> 8);
        pos += 2;
    }

    /**
     * writeInt operation for jdbc 1.x
     * @param i
     * @throws IOException
     */
    public final void writeIntV1(int i) throws IOException {

        byte[] b = this.buf;
        b[this.pos++] = (byte) (i & 0xff);
        b[this.pos++] = (byte) (i >>> 8);
    }

    /**
     * Write int value into buffer. flush buffer if too small.
     *
     * @param value int value
     * @throws IOException if socket error occur
     */
    public void writeInt(int value) throws IOException {
        if (4 > buf.length - pos) {
            // not enough space remaining
            byte[] arr = new byte[4];
            arr[0] = (byte) value;
            arr[1] = (byte) (value >> 8);
            arr[2] = (byte) (value >> 16);
            arr[3] = (byte) (value >> 24);
            write(arr, 0, 4);
            return;
        }

        buf[pos] = (byte) value;
        buf[pos + 1] = (byte) (value >> 8);
        buf[pos + 2] = (byte) (value >> 16);
        buf[pos + 3] = (byte) (value >> 24);
        pos += 4;
    }

    public void writeFloatV1(float f) throws IOException {

        int i = Float.floatToIntBits(f);
        byte[] b = this.buf;
        b[this.pos++] = (byte) (i & 0xff);
        b[this.pos++] = (byte) (i >>> 8);
        b[this.pos++] = (byte) (i >>> 16);
        b[this.pos++] = (byte) (i >>> 24);
    }

    /**
     * Write long value into buffer. flush buffer if too small.
     *
     * @param value long value
     * @throws IOException if socket error occur
     */
    public void writeLong(long value) throws IOException {
        if (8 > buf.length - pos) {
            // not enough space remaining
            byte[] arr = new byte[8];
            arr[0] = (byte) value;
            arr[1] = (byte) (value >> 8);
            arr[2] = (byte) (value >> 16);
            arr[3] = (byte) (value >> 24);
            arr[4] = (byte) (value >> 32);
            arr[5] = (byte) (value >> 40);
            arr[6] = (byte) (value >> 48);
            arr[7] = (byte) (value >> 56);
            write(arr, 0, 8);
            return;
        }

        buf[pos] = (byte) value;
        buf[pos + 1] = (byte) (value >> 8);
        buf[pos + 2] = (byte) (value >> 16);
        buf[pos + 3] = (byte) (value >> 24);
        buf[pos + 4] = (byte) (value >> 32);
        buf[pos + 5] = (byte) (value >> 40);
        buf[pos + 6] = (byte) (value >> 48);
        buf[pos + 7] = (byte) (value >> 56);
        pos += 8;
    }

    /**
     * writeLong operation for jdbc 1.x
     * @param i
     * @throws IOException
     */
    public void writeLongV1(long i) throws IOException {

        byte[] b = this.buf;
        b[this.pos++] = (byte) (i & 0xff);
        b[this.pos++] = (byte) (i >>> 8);
        b[this.pos++] = (byte) (i >>> 16);
        b[this.pos++] = (byte) (i >>> 24);
    }

    /**
     * Write byte value, len times into buffer. flush buffer if too small.
     *
     * @param value byte value
     * @param len number of time to write value.
     * @throws IOException if socket error occur.
     */
    public void writeBytes(byte value, int len) throws IOException {
        if (len > buf.length - pos) {
            // not enough space remaining
            byte[] arr = new byte[len];
            Arrays.fill(arr, value);
            write(arr, 0, len);
            return;
        }

        for (int i = pos; i < pos + len; i++) {
            buf[i] = value;
        }
        pos += len;
    }

    /**
     * Write field length into buffer, flush socket if needed.
     *
     * @param length field length
     * @throws IOException if socket error occur.
     */
    public void writeFieldLength(long length) throws IOException {
        if (length < 251) {

            write((byte) length);
            return;
        }

        if (length < 65536) {

            if (3 > buf.length - pos) {
                // not enough space remaining
                byte[] arr = new byte[3];
                arr[0] = (byte) 0xfc;
                arr[1] = (byte) length;
                arr[2] = (byte) (length >>> 8);
                write(arr, 0, 3);
                return;
            }

            buf[pos] = (byte) 0xfc;
            buf[pos + 1] = (byte) length;
            buf[pos + 2] = (byte) (length >>> 8);
            pos += 3;
            return;
        }

        if (length < 16777216) {

            if (4 > buf.length - pos) {
                // not enough space remaining
                byte[] arr = new byte[4];
                arr[0] = (byte) 0xfd;
                arr[1] = (byte) length;
                arr[2] = (byte) (length >>> 8);
                arr[3] = (byte) (length >>> 16);
                write(arr, 0, 4);
                return;
            }

            buf[pos] = (byte) 0xfd;
            buf[pos + 1] = (byte) length;
            buf[pos + 2] = (byte) (length >>> 8);
            buf[pos + 3] = (byte) (length >>> 16);
            pos += 4;
            return;
        }

        if (9 > buf.length - pos) {
            // not enough space remaining
            byte[] arr = new byte[9];
            arr[0] = (byte) 0xfe;
            arr[1] = (byte) length;
            arr[2] = (byte) (length >>> 8);
            arr[3] = (byte) (length >>> 16);
            arr[4] = (byte) (length >>> 24);
            arr[5] = (byte) (length >>> 32);
            arr[6] = (byte) (length >>> 40);
            arr[7] = (byte) (length >>> 48);
            arr[8] = (byte) (length >>> 56);
            write(arr, 0, 9);
            return;
        }

        buf[pos] = (byte) 0xfe;
        buf[pos + 1] = (byte) length;
        buf[pos + 2] = (byte) (length >>> 8);
        buf[pos + 3] = (byte) (length >>> 16);
        buf[pos + 4] = (byte) (length >>> 24);
        buf[pos + 5] = (byte) (length >>> 32);
        buf[pos + 6] = (byte) (length >>> 40);
        buf[pos + 7] = (byte) (length >>> 48);
        buf[pos + 8] = (byte) (length >>> 56);
        pos += 9;
    }

    /**
     * Write byte into buffer, flush buffer to socket if needed.
     *
     * @param value byte to send
     * @throws IOException if socket error occur.
     */
    public void write(int value) throws IOException {
        if (pos >= buf.length) {
            if (pos >= getMaxPacketLength() && !bufferContainDataAfterMark) {
                // buffer is more than a Packet, must flushBuffer()
                flushBuffer(false);
            } else {
                growBuffer(1);
            }
        }
        buf[pos++] = (byte) value;
    }

    public void write(byte[] arr) throws IOException {
        write(arr, 0, arr.length);
    }

    /**
     * Write byte array to buffer. If buffer is full, flush socket.
     *
     * @param arr byte array
     * @param off offset
     * @param len byte length to write
     * @throws IOException if socket error occur
     */
    public void write(byte[] arr, int off, int len) throws IOException {
        if (len > buf.length - pos) {
            if (buf.length != getMaxPacketLength()) {
                growBuffer(len);
            }

            // max buffer size
            if (len > buf.length - pos) {

                if (mark != -1) {
                    growBuffer(len);
                    if (mark != -1) {
                        flushBufferStopAtMark();
                    }

                } else {
                    // not enough space in buffer, will stream :
                    // fill buffer and flush until all data are snd
                    int remainingLen = len;
                    do {
                        int lenToFillBuffer = Math.min(getMaxPacketLength() - pos, remainingLen);
                        System.arraycopy(arr, off, buf, pos, lenToFillBuffer);
                        remainingLen -= lenToFillBuffer;
                        off += lenToFillBuffer;
                        pos += lenToFillBuffer;
                        if (remainingLen > 0) {
                            flushBuffer(false);
                        } else {
                            break;
                        }
                    } while (true);
                    return;
                }
            }
        }

        System.arraycopy(arr, off, buf, pos, len);
        pos += len;
    }

    public void write(String str) throws IOException {
        write(str, false, false);
    }

    /**
     * Write string to socket.
     *
     * @param str string
     * @param escape must be escape
     * @param noBackslashEscapes escape method
     * @throws IOException if socket error occur
     */
    public void write(String str, boolean escape, boolean noBackslashEscapes) throws IOException {

        int charsLength = str.length();

        // not enough space remaining
        if (charsLength * 3 + 2 >= buf.length - pos || this.charset != StandardCharsets.UTF_8) {
            byte[] arr = str.getBytes(this.charset);
            if (escape) {
                write(QUOTE);
                writeBytesEscaped(arr, arr.length, noBackslashEscapes);
                write(QUOTE);
            } else {
                write(arr, 0, arr.length);
            }
            return;
        }

        // create UTF-8 byte array
        // since java char are internally using UTF-16 using surrogate's pattern, 4 bytes unicode
        // characters will
        // represent 2 characters : example "\uD83C\uDFA4" = 🎤 unicode 8 "no microphones"
        // so max size is 3 * charLength
        // (escape characters are 1 byte encoded, so length might only be 2 when escape)
        // + 2 for the quotes for text protocol
        int charsOffset = 0;
        char currChar;

        // quick loop if only ASCII chars for faster escape
        if (escape) {
            buf[pos++] = QUOTE;
            if (noBackslashEscapes) {
                for (; charsOffset < charsLength && (currChar = str.charAt(charsOffset)) < 0x80; charsOffset++) {
                    if (currChar == QUOTE) {
                        buf[pos++] = QUOTE;
                    }
                    buf[pos++] = (byte) currChar;
                }
            } else {
                for (; charsOffset < charsLength && (currChar = str.charAt(charsOffset)) < 0x80; charsOffset++) {
                    switch (currChar) {
                        case 0:
                            buf[pos++] = BACKSLASH;
                            buf[pos++] = '0';
                            break;
                        case '\n':
                            buf[pos++] = BACKSLASH;
                            buf[pos++] = 'n';
                            break;
                        case '\r':
                            buf[pos++] = BACKSLASH;
                            buf[pos++] = 'r';
                            break;
                        case '\\':
                            buf[pos++] = BACKSLASH;
                            buf[pos++] = BACKSLASH;
                            break;
                        case '\'':
                            buf[pos++] = BACKSLASH;
                            buf[pos++] = '\'';
                            break;
                        case '"':
                            buf[pos++] = BACKSLASH;
                            buf[pos++] = '"';
                            break;
                        case '\032':
                            buf[pos++] = BACKSLASH;
                            buf[pos++] = 'Z';
                            break;
                        case '\u00a5':
                        case '\u20a9':
                            CharsetEncoder charsetEncoder = this.charset.newEncoder();
                            if (charsetEncoder != null) {
                                CharBuffer charBuffer = CharBuffer.allocate(1);
                                ByteBuffer byteBuffer = ByteBuffer.allocate(1);
                                charBuffer.put(currChar);
                                charBuffer.position(0);
                                charsetEncoder.encode(charBuffer, byteBuffer, true);
                                if (byteBuffer.get(0) == '\\') {
                                    buf[pos++] = BACKSLASH;
                                }
                            }
                            buf[pos++] = (byte) currChar;
                            break;
                        default:
                            buf[pos++] = (byte) currChar;
                            break;
                    }

                }
            }
        } else {
            for (; charsOffset < charsLength && (currChar = str.charAt(charsOffset)) < 0x80; charsOffset++) {
                buf[pos++] = (byte) currChar;
            }
        }

        // if quick loop not finished
        while (charsOffset < charsLength) {
            currChar = str.charAt(charsOffset++);
            if (currChar < 0x80) {
                if (escape) {
                    if (noBackslashEscapes) {
                        if (currChar == QUOTE) {
                            buf[pos++] = QUOTE;
                        }
                    } else if (currChar == BACKSLASH || currChar == QUOTE || currChar == ZERO_BYTE
                               || currChar == DBL_QUOTE) {
                        buf[pos++] = BACKSLASH;
                    }
                }
                buf[pos++] = (byte) currChar;
            } else if (currChar < 0x800) {
                buf[pos++] = (byte) (0xc0 | (currChar >> 6));
                buf[pos++] = (byte) (0x80 | (currChar & 0x3f));
            } else if (currChar >= 0xD800 && currChar < 0xE000) {
                // reserved for surrogate - see https://en.wikipedia.org/wiki/UTF-16
                if (currChar < 0xDC00) {
                    // is high surrogate
                    if (charsOffset + 1 > charsLength) {
                        buf[pos++] = (byte) 0x63;
                    } else {
                        char nextChar = str.charAt(charsOffset);
                        if (nextChar >= 0xDC00 && nextChar < 0xE000) {
                            // is low surrogate
                            int surrogatePairs = ((currChar << 10) + nextChar)
                                                 + (0x010000 - (0xD800 << 10) - 0xDC00);
                            buf[pos++] = (byte) (0xf0 | ((surrogatePairs >> 18)));
                            buf[pos++] = (byte) (0x80 | ((surrogatePairs >> 12) & 0x3f));
                            buf[pos++] = (byte) (0x80 | ((surrogatePairs >> 6) & 0x3f));
                            buf[pos++] = (byte) (0x80 | (surrogatePairs & 0x3f));
                            charsOffset++;
                        } else {
                            // must have low surrogate
                            buf[pos++] = (byte) 0x3f;
                        }
                    }
                } else {
                    // low surrogate without high surrogate before
                    buf[pos++] = (byte) 0x3f;
                }
            } else {
                buf[pos++] = (byte) (0xe0 | ((currChar >> 12)));
                buf[pos++] = (byte) (0x80 | ((currChar >> 6) & 0x3f));
                buf[pos++] = (byte) (0x80 | (currChar & 0x3f));
            }
        }
        if (escape) {
            buf[pos++] = QUOTE;
        }
    }

    /**
     * Write stream into socket.
     *
     * @param is inputStream
     * @param escape must be escape
     * @param noBackslashEscapes escape method
     * @throws IOException if socket error occur
     */
    public void write(InputStream is, boolean escape, boolean noBackslashEscapes)
                                                                                 throws IOException {
        byte[] array = new byte[4096];
        int len;
        if (escape) {
            while ((len = is.read(array)) > 0) {
                writeBytesEscaped(array, len, noBackslashEscapes);
            }
        } else {
            while ((len = is.read(array)) > 0) {
                write(array, 0, len);
            }
        }
    }

    /**
     * Write stream AS hexdump into socket.
     *
     * @param is inputStream
     * @param escape must be escape
     * @param noBackslashEscapes escape method
     * @throws IOException if socket error occur
     */
    public void writeHex(InputStream is, boolean escape, boolean noBackslashEscapes)
                                                                                    throws IOException {
        byte[] array = new byte[2048];
        int len;
        //TODO optimize byte[] to hexbyte[] directly, not use hexString any more
        if (escape) {
            while ((len = is.read(array)) > 0) {
                writeBytesEscaped(Utils.toHexString(array).getBytes(), (len << 1),
                    noBackslashEscapes);
            }
        } else {
            while ((len = is.read(array)) > 0) {
                write(Utils.toHexString(array).getBytes(), 0, (len << 1));
            }
        }
    }

    /**
     * Write stream into socket.
     *
     * @param is inputStream
     * @param length write length
     * @param escape must be escape
     * @param noBackslashEscapes escape method
     * @throws IOException if socket error occur
     */
    public void write(InputStream is, long length, boolean escape, boolean noBackslashEscapes)
                                                                                              throws IOException {
        byte[] array = new byte[4096];
        int len;
        while (length > 0 && (len = is.read(array, 0, Math.min(4096, (int) length))) > 0) {
            if (escape) {
                writeBytesEscaped(array, len, noBackslashEscapes);
            } else {
                write(array, 0, len);
            }
            length -= len;
        }
    }

    /**
     * Write stream As hexdump into socket.
     *
     * @param is inputStream
     * @param length write length
     * @param escape must be escape
     * @param noBackslashEscapes escape method
     * @throws IOException if socket error occur
     */
    public void writeHex(InputStream is, long length, boolean escape, boolean noBackslashEscapes)
                                                                                                 throws IOException {
        byte[] array = new byte[2048];
        int len;
        while (length > 0 && (len = is.read(array, 0, Math.min(2048, (int) length))) > 0) {
            writeBytesEscaped(Utils.toHexString(array).getBytes(), (len << 1), escape);
            length -= len;
        }
    }

    /**
     * Write reader into socket.
     *
     * @param reader reader
     * @param escape must be escape
     * @param noBackslashEscapes escape method
     * @throws IOException if socket error occur
     */
    public void write(Reader reader, boolean escape, boolean noBackslashEscapes) throws IOException {
        char[] buffer = new char[4096];
        int len;
        while ((len = reader.read(buffer)) >= 0) {
            byte[] data = new String(buffer, 0, len).getBytes(this.charset);
            if (escape) {
                writeBytesEscaped(data, data.length, noBackslashEscapes);
            } else {
                write(data);
            }
        }
    }

    public void writeEscapeQuote(Reader reader, boolean noBackslashEscapes) throws IOException {
        char[] buffer = new char[4096];
        int len;
        while ((len = reader.read(buffer)) >= 0) {
            byte[] data = new String(buffer, 0, len).getBytes(this.charset);
            writeBytesEscapedQuote(data, data.length, noBackslashEscapes);
        }
    }

    public void writeEscapeQuote(Reader reader, long length, boolean noBackslashEscapes)
                                                                                        throws IOException {
        char[] buffer = new char[4096];
        int len;
        while (length > 0 && (len = reader.read(buffer, 0, Math.min((int) length, 4096))) >= 0) {
            byte[] data = new String(buffer, 0, len).getBytes(this.charset);
            writeBytesEscapedQuote(data, data.length, noBackslashEscapes);
            length -= len;
        }
    }

    /**
     * Write reader into socket.
     *
     * @param reader reader
     * @param length write length
     * @param escape must be escape
     * @param noBackslashEscapes escape method
     * @throws IOException if socket error occur
     */
    public void write(Reader reader, long length, boolean escape, boolean noBackslashEscapes)
                                                                                             throws IOException {
        char[] buffer = new char[4096];
        int len;
        while (length > 0 && (len = reader.read(buffer, 0, Math.min((int) length, 4096))) >= 0) {
            byte[] data = new String(buffer, 0, len).getBytes(this.charset);
            if (escape) {
                writeBytesEscaped(data, data.length, noBackslashEscapes);
            } else {
                write(data);
            }
            length -= len;
        }
    }

    /**
     * Write escape bytes to socket.
     *
     * @param bytes bytes
     * @param len len to write
     * @param noBackslashEscapes escape method
     * @throws IOException if socket error occur
     */
    public void writeBytesEscaped(byte[] bytes, int len, boolean noBackslashEscapes)
                                                                                    throws IOException {
        if (len * 2 > buf.length - pos) {

            // makes buffer bigger (up to 16M)
            if (buf.length != getMaxPacketLength()) {
                growBuffer(len * 2);
            }

            // data may be bigger than buffer.
            // must flush buffer when full (and reset position to 0)
            if (len * 2 > buf.length - pos) {

                if (mark != -1) {
                    growBuffer(len * 2);
                    if (mark != -1) {
                        flushBufferStopAtMark();
                    }

                } else {

                    // not enough space in buffer, will fill buffer
                    if (noBackslashEscapes) {
                        for (int i = 0; i < len; i++) {
                            if (QUOTE == bytes[i]) {
                                buf[pos++] = QUOTE;
                                if (buf.length <= pos) {
                                    flushBuffer(false);
                                }
                            }
                            buf[pos++] = bytes[i];
                            if (buf.length <= pos) {
                                flushBuffer(false);
                            }
                        }
                    } else {
                        for (int i = 0; i < len; i++) {
                            if (bytes[i] == QUOTE || bytes[i] == BACKSLASH || bytes[i] == DBL_QUOTE
                                || bytes[i] == ZERO_BYTE) {
                                buf[pos++] = '\\';
                                if (buf.length <= pos) {
                                    flushBuffer(false);
                                }
                            }
                            buf[pos++] = bytes[i];
                            if (buf.length <= pos) {
                                flushBuffer(false);
                            }
                        }
                    }
                    return;
                }
            }
        }

        // sure to have enough place filling buffer directly
        if (noBackslashEscapes) {
            for (int i = 0; i < len; i++) {
                if (QUOTE == bytes[i]) {
                    buf[pos++] = QUOTE;
                }
                buf[pos++] = bytes[i];
            }
        } else {
            for (int i = 0; i < len; i++) {
                if (bytes[i] == QUOTE || bytes[i] == BACKSLASH || bytes[i] == '"'
                    || bytes[i] == ZERO_BYTE) {
                    buf[pos++] = BACKSLASH; // add escape slash
                }
                buf[pos++] = bytes[i];
            }
        }
    }

    /**
     * Write escape bytes to socket.
     *
     * @param bytes bytes
     * @param len len to write
     * @param noBackslashEscapes escape method
     * @throws IOException if socket error occur
     */
    public void writeBytesEscapedQuote(byte[] bytes, int len, boolean noBackslashEscapes)
                                                                                         throws IOException {
        if (len * 2 > buf.length - pos) {

            // makes buffer bigger (up to 16M)
            if (buf.length != getMaxPacketLength()) {
                growBuffer(len * 2);
            }

            // data may be bigger than buffer.
            // must flush buffer when full (and reset position to 0)
            if (len * 2 > buf.length - pos) {

                if (mark != -1) {
                    growBuffer(len * 2);
                    if (mark != -1) {
                        flushBufferStopAtMark();
                    }

                } else {

                    // not enough space in buffer, will fill buffer
                    if (noBackslashEscapes) {
                        for (int i = 0; i < len; i++) {
                            if (QUOTE == bytes[i]) {
                                buf[pos++] = QUOTE;
                                if (buf.length <= pos) {
                                    flushBuffer(false);
                                }
                            }
                            buf[pos++] = bytes[i];
                            if (buf.length <= pos) {
                                flushBuffer(false);
                            }
                        }
                    } else {
                        for (int i = 0; i < len; i++) {
                            if (bytes[i] == QUOTE /*
                                                  || bytes[i] == BACKSLASH
                                                  || bytes[i] == DBL_QUOTE
                                                  || bytes[i] == ZERO_BYTE*/) {
                                buf[pos++] = '\'';
                                if (buf.length <= pos) {
                                    flushBuffer(false);
                                }
                            }
                            buf[pos++] = bytes[i];
                            if (buf.length <= pos) {
                                flushBuffer(false);
                            }
                        }
                    }
                    return;
                }
            }
        }

        // sure to have enough place filling buffer directly
        if (noBackslashEscapes) {
            for (int i = 0; i < len; i++) {
                if (QUOTE == bytes[i]) {
                    buf[pos++] = QUOTE;
                }
                buf[pos++] = bytes[i];
            }
        } else {
            for (int i = 0; i < len; i++) {
                if (bytes[i] == QUOTE/*
                                     || bytes[i] == BACKSLASH
                                     || bytes[i] == '"'
                                     || bytes[i] == ZERO_BYTE*/) {
                    buf[pos++] = '\''; // add escape slash
                }
                buf[pos++] = bytes[i];
            }
        }
    }

    public int getMaxAllowedPacket() {
        return maxAllowedPacket;
    }

    public abstract void setMaxAllowedPacket(int maxAllowedPacket);

    public void permitTrace(boolean permitTrace) {
        this.permitTrace = permitTrace;
    }

    /**
     * Set server thread id.
     *
     * @param serverThreadId current server thread id.
     * @param isMaster is server master
     */
    public void setServerThreadId(long serverThreadId, Boolean isMaster) {
        this.serverThreadLog = "conn=" + serverThreadId
                               + ((isMaster != null) ? "(" + (isMaster ? "M" : "S") + ")" : "");
    }

    public void setTraceCache(LruTraceCache traceCache) {
        this.traceCache = traceCache;
    }

    @Override
    public void mark() {
        mark = pos;
    }

    @Override
    public boolean isMarked() {
        return mark != -1;
    }

    /**
     * Flush to last mark.
     *
     * @throws IOException if flush fail.
     */
    @Override
    public void flushBufferStopAtMark() throws IOException {
        final int end = pos;
        pos = mark;
        flushBuffer(true);
        out.flush();
        startPacket(0);

        System.arraycopy(buf, mark, buf, pos, end - mark);
        pos += end - mark;
        mark = -1;
        bufferContainDataAfterMark = true;
    }

    public boolean bufferIsDataAfterMark() {
        return bufferContainDataAfterMark;
    }

    /**
     * Reset mark flag and send bytes after mark flag.
     *
     * @return bytes after mark flag
     */
    public byte[] resetMark() {
        mark = -1;

        if (bufferContainDataAfterMark) {
            byte[] data = Arrays.copyOfRange(buf, initialPacketPos(), pos);
            startPacket(0);
            bufferContainDataAfterMark = false;
            return data;
        }
        return null;
    }

    /**
     * Get current position
     * @return position value
     */
    public int getPosition() {
        return pos;
    }

    /**
     * Set current position
     * @param val set current position vallue to val
     */
    public void setPosition(int val) {
        pos = val;
    }

    public void startSendRequest() {
        if (timeTrace != null) {
            timeTrace.startSendRequest();
        }
    }

    public void endSendRequest() {
        if (timeTrace != null) {
            timeTrace.endSendRequest();
        }
    }

    public boolean isEncloseParamInParentheses() {
        return encloseParamInParentheses;
    }

    public boolean isOracleMode() {
        return isOracleMode;
    }

    public void setOracleMode(boolean oracleMode) {
        isOracleMode = oracleMode;
    }

}
