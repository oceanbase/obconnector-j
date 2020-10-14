package com.alipay.oceanbase.jdbc.internal.io.input;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

import com.alipay.oceanbase.jdbc.internal.com.read.Buffer;
import com.alipay.oceanbase.jdbc.internal.io.LruTraceCache;
import com.alipay.oceanbase.jdbc.internal.io.TraceObject;
import com.alipay.oceanbase.jdbc.internal.logging.Logger;
import com.alipay.oceanbase.jdbc.internal.logging.LoggerFactory;
import com.alipay.oceanbase.jdbc.internal.util.Utils;

public class DecompressPacketInputStream implements PacketInputStream {

    private static final int    REUSABLE_BUFFER_LENGTH = 1024;
    private static final int    MAX_PACKET_SIZE        = 0xffffff;
    private static final Logger logger                 = LoggerFactory
                                                           .getLogger(DecompressPacketInputStream.class);
    private final byte[]        header                 = new byte[7];
    private final byte[]        reusableArray          = new byte[REUSABLE_BUFFER_LENGTH];
    private final InputStream   inputStream;
    private final int           maxQuerySizeToLog;
    // compress packet can contain multiple standard packet
    private byte[]              cacheData              = new byte[0];
    private int                 cachePos;
    private int                 cacheEnd;
    private int                 packetSeq;
    private int                 compressPacketSeq;
    private String              serverThreadLog        = "";
    private LruTraceCache       traceCache             = null;
    private long                threadId;

    public DecompressPacketInputStream(InputStream in, int maxQuerySizeToLog, long threadId) {
        inputStream = in;
        this.maxQuerySizeToLog = maxQuerySizeToLog;
        this.threadId = threadId;
    }

    @Override
    public Buffer getPacket(boolean reUsable) throws IOException {
        return new Buffer(getPacketArray(reUsable));
    }

    /**
     * Get next packet. Packet can be compressed, and if so, can contain many standard packet.
     *
     * @param reUsable if can use existing reusable buffer to avoid creating array
     * @return array packet.
     * @throws IOException if socket exception occur.
     */
    public byte[] getPacketArray(boolean reUsable) throws IOException {

        byte[] cachePacket = getNextCachePacket();
        if (cachePacket != null) {
            return cachePacket;
        }

        // loop until having the whole packet
        do {
            // Read 7 byte header
            readBlocking(header, 7);

            int compressedLength = (header[0] & 0xff) + ((header[1] & 0xff) << 8)
                                   + ((header[2] & 0xff) << 16);
            compressPacketSeq = header[3] & 0xff;
            int decompressedLength = (header[4] & 0xff) + ((header[5] & 0xff) << 8)
                                     + ((header[6] & 0xff) << 16);

            byte[] rawBytes;
            if (reUsable && decompressedLength == 0 && compressedLength < REUSABLE_BUFFER_LENGTH) {
                rawBytes = reusableArray;
            } else {
                rawBytes = new byte[decompressedLength != 0 ? decompressedLength : compressedLength];
            }

            readCompressBlocking(rawBytes, compressedLength, decompressedLength);

            if (traceCache != null) {
                int length = decompressedLength != 0 ? decompressedLength : compressedLength;
                traceCache.put(new TraceObject(false,
                    decompressedLength == 0 ? TraceObject.COMPRESSED_PROTOCOL_NOT_COMPRESSED_PACKET
                        : TraceObject.COMPRESSED_PROTOCOL_COMPRESSED_PACKET, threadId, Arrays
                        .copyOfRange(header, 0, 7), Arrays.copyOfRange(rawBytes, 0,
                        length > 1000 ? 1000 : length)));
            }

            if (logger.isTraceEnabled()) {
                int length = decompressedLength != 0 ? decompressedLength : compressedLength;
                logger.trace("read {} {}{}", (decompressedLength == 0 ? "uncompress" : "compress"),
                    serverThreadLog,
                    Utils.hexdump(maxQuerySizeToLog - 7, 0, length, header, rawBytes));
            }

            cache(rawBytes, decompressedLength == 0 ? compressedLength : decompressedLength);
            byte[] packet = getNextCachePacket();
            if (packet != null) {
                return packet;
            }

        } while (true);
    }

    private void readCompressBlocking(byte[] arr, int compressedLength, int decompressedLength)
                                                                                               throws IOException {
        if (decompressedLength != 0) {

            byte[] compressedBuffer = new byte[compressedLength];
            // Read compress content
            readBlocking(compressedBuffer, compressedLength);

            Inflater inflater = new Inflater();
            inflater.setInput(compressedBuffer);
            try {
                int actualUncompressBytes = inflater.inflate(arr);
                if (actualUncompressBytes != decompressedLength) {
                    throw new IOException("Invalid exception length after decompression "
                                          + actualUncompressBytes + ",expected "
                                          + decompressedLength);
                }
            } catch (DataFormatException dfe) {
                throw new IOException(dfe);
            }
            inflater.end();

        } else {
            // Read standard content
            readBlocking(arr, compressedLength);
        }
    }

    private void readBlocking(byte[] arr, int length) throws IOException {
        int remaining = length;
        int off = 0;
        do {
            int count = inputStream.read(arr, off, remaining);
            if (count < 0) {
                throw new EOFException("unexpected end of stream, read " + (length - remaining)
                                       + " bytes from " + length + " (socket was closed by server)");
            }
            remaining -= count;
            off += count;
        } while (remaining > 0);
    }

    private void cache(byte[] rawBytes, int length) {
        if (cachePos >= cacheEnd) {
            cacheData = rawBytes;
            cachePos = 0;
            cacheEnd = length;
        } else {
            // must add to cache
            byte[] newCache = new byte[length + cacheEnd - cachePos];
            System.arraycopy(cacheData, cachePos, newCache, 0, cacheEnd - cachePos);
            System.arraycopy(rawBytes, 0, newCache, cacheEnd - cachePos, length);
            cacheData = newCache;
            cachePos = 0;
            cacheEnd = newCache.length;
        }
    }

    private byte[] getNextCachePacket() {
        int packetOffset = 0;

        // if packet is not totally fetch, return null
        while (cacheEnd > cachePos + 4 + packetOffset * (MAX_PACKET_SIZE + 4)) {
            int lastPacketLength = (cacheData[cachePos + packetOffset * (MAX_PACKET_SIZE + 4)] & 0xff)
                                   + ((cacheData[cachePos + packetOffset * (MAX_PACKET_SIZE + 4)
                                                 + 1] & 0xff) << 8)
                                   + ((cacheData[cachePos + packetOffset * (MAX_PACKET_SIZE + 4)
                                                 + 2] & 0xff) << 16);
            if (lastPacketLength == MAX_PACKET_SIZE) {
                packetOffset += 1;
            } else if (cacheEnd >= cachePos + 4 + packetOffset * (MAX_PACKET_SIZE + 4)
                                   + lastPacketLength) {
                // packet is totally fetched.

                // if packet was less than 16M
                if (packetOffset == 0) {
                    packetSeq = cacheData[cachePos + 3];

                    if (cacheEnd - (cachePos + 4) >= lastPacketLength) {
                        byte[] packet = new byte[lastPacketLength];
                        System.arraycopy(cacheData, cachePos + 4, packet, 0, lastPacketLength);

                        if (logger.isTraceEnabled()) {
                            logger.trace("read packet: seq={} len={} {}{}", packetSeq,
                                lastPacketLength, serverThreadLog, Utils.hexdump(maxQuerySizeToLog,
                                    cachePos + 4, lastPacketLength, cacheData));
                        }

                        cachePos += 4 + lastPacketLength;
                        return packet;
                    }
                } else {
                    byte[] packet = new byte[lastPacketLength + packetOffset * MAX_PACKET_SIZE];
                    int offset = 0;
                    do {
                        lastPacketLength = (cacheData[cachePos] & 0xff)
                                           + ((cacheData[cachePos + 1] & 0xff) << 8)
                                           + ((cacheData[cachePos + 2] & 0xff) << 16);
                        packetSeq = cacheData[cachePos + 3];
                        System.arraycopy(cacheData, cachePos + 4, packet, offset, lastPacketLength);
                        offset += lastPacketLength;

                        if (logger.isTraceEnabled()) {
                            logger.trace("read packet: seq={} len={} {}{}", packetSeq,
                                lastPacketLength, serverThreadLog, Utils.hexdump(maxQuerySizeToLog,
                                    cachePos + 4, lastPacketLength, cacheData));
                        }

                        cachePos += 4 + lastPacketLength;

                    } while (lastPacketLength == MAX_PACKET_SIZE);
                    return packet;
                }
            } else {
                return null;
            }
        }
        return null;
    }

    @Override
    public int getLastPacketSeq() {
        return packetSeq;
    }

    @Override
    public int getCompressLastPacketSeq() {
        return compressPacketSeq;
    }

    @Override
    public void close() throws IOException {
        inputStream.close();
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
}
