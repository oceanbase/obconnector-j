package com.alipay.oceanbase.jdbc.internal.io.output;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;

import com.alipay.oceanbase.jdbc.internal.util.exceptions.MaxAllowedPacketException;
import com.alipay.oceanbase.jdbc.internal.io.LruTraceCache;

@SuppressWarnings("RedundantThrows")
public interface PacketOutputStream {

    void startPacket(int seqNo);

    void writeEmptyPacket(int seqNo) throws IOException;

    void writeEmptyPacket() throws IOException;

    void write(int arr) throws IOException;

    void write(byte[] arr) throws IOException;

    void write(byte[] arr, int off, int len) throws IOException;

    void write(String str) throws IOException;

    void write(String str, boolean escape, boolean noBackslashEscapes) throws IOException;

    void write(InputStream is, boolean escape, boolean noBackslashEscapes) throws IOException;

    void write(InputStream is, long length, boolean escape, boolean noBackslashEscapes)
                                                                                       throws IOException;

    void writeHex(InputStream is, boolean escape, boolean noBackslashEscapes) throws IOException;

    void writeHex(InputStream is, long length, boolean escape, boolean noBackslashEscapes)
                                                                                          throws IOException;

    void write(Reader reader, boolean escape, boolean noBackslashEscapes) throws IOException;

    void write(Reader reader, long length, boolean escape, boolean noBackslashEscapes)
                                                                                      throws IOException;

    void writeBytesEscaped(byte[] bytes, int len, boolean noBackslashEscapes) throws IOException;

    void writeBytesEscapedQuote(byte[] bytes, int len, boolean noBackslashEscapes)
                                                                                  throws IOException;

    void flush() throws IOException;

    void close() throws IOException;

    boolean checkRemainingSize(int len);

    boolean exceedMaxLength();

    OutputStream getOutputStream();

    void writeShort(short value) throws IOException;

    void writeInt(int value) throws IOException;

    void writeLong(long value) throws IOException;

    void writeBytes(byte value, int len) throws IOException;

    void writeFieldLength(long length) throws IOException;

    int getMaxAllowedPacket();

    void setMaxAllowedPacket(int maxAllowedPacket);

    void permitTrace(boolean permitTrace);

    void setServerThreadId(long serverThreadId, Boolean isMaster);

    void setTraceCache(LruTraceCache traceCache);

    void mark() throws MaxAllowedPacketException;

    boolean isMarked();

    void flushBufferStopAtMark() throws IOException;

    boolean bufferIsDataAfterMark();

    byte[] resetMark();

    int initialPacketPos();

    void checkMaxAllowedLength(int length) throws MaxAllowedPacketException;
}
