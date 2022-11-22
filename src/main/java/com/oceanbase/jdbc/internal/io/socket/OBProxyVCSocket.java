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
package com.oceanbase.jdbc.internal.io.socket;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import com.oceanbase.jdbc.internal.logging.Logger;
import com.oceanbase.jdbc.internal.logging.LoggerFactory;

public class OBProxyVCSocket extends Socket {
    private static final int    DEFAULT_SND_BUFFER_LEN   = 16 * 1024;                               //16KB
    private static final int    DEFAULT_RCV_BUFFER_LEN   = 16 * 1024;                               //16KB
    private static final int    VC_SOCK_RD_WITH_TIMEOUT  = 1;
    private static final Logger logger                   = LoggerFactory
                                                             .getLoggerProxy(OBProxyVCSocket.class);

    private static final String DEFAULT_OBPROXY_LIB_FILE = "/u01/obproxy/lib/libobproxy_so.so";
    private int                 socketTimeout            = 0;
    private long                obproxyClientSessionId   = -1;

    static {
        if (!(System.getProperty("os.name").toLowerCase()).startsWith("win")) {
            /* Not support Windows*/
            System.out.println("load obproxy_so library");
            try {
                System.load(DEFAULT_OBPROXY_LIB_FILE);
            } catch (UnsatisfiedLinkError e) {
                System.loadLibrary("obproxy_so");
            }
        }
    }

    private final AtomicBoolean closeLock                = new AtomicBoolean();
    private final long          fd;
    private InputStream         is;
    private OutputStream        os;
    private boolean             connected;
    private byte[]              sendBuff;
    private byte[]              recvBuff;
    private int                 recvBuffDataSpos;
    private int                 recvBuffDataLen;
    private int                 sendBuffLen;
    private int                 recvBuffLen;
    private boolean             closed                   = true;

    public IOException InitProxyVCIOException(String info, Throwable cause) {
        IOException e = new IOException(info);
        if (cause != null) {
            e.initCause(cause);
        }
        return e;
    }

    public OBProxyVCSocket(String obproxyConfig, int sendBufferLen, int recvBufferLen)
                                                                                      throws IOException {

        logger
            .debug("OBProxyVCSocket start init with buffer len, obproxyConfig:{} ", obproxyConfig);
        if ((System.getProperty("os.name").toLowerCase()).startsWith("win")) {
            throw new IOException("OBProxyVC sockets are not supported on Windows");
        }
        if (recvBufferLen != 0) {
            this.recvBuffLen = recvBufferLen;
        } else {
            this.recvBuffLen = DEFAULT_RCV_BUFFER_LEN;
        }
        if (sendBufferLen != 0) {
            this.sendBuffLen = sendBufferLen;
        } else {
            this.sendBuffLen = DEFAULT_SND_BUFFER_LEN;
        }
        try {
            /**
             *  Memory alloced here to used by socket just like tcp socket's recvBuffer and snd_buffer in OS.
             *  It will be hold for all time in socket's life. The value of them are set by 'option.tcpRcvBuf'
             *  and 'option.tcpSndBuf', and default's value is 16KB if options not be set.
             */
            sendBuff = new byte[sendBuffLen];
            recvBuff = new byte[recvBuffLen];
            recvBuffDataSpos = 0;
            recvBuffDataLen = 0;

        } catch (Exception e) {
            throw new IOException("OBProxyVCSocket init buffer error", e.getCause());
        }
        closeLock.set(false);
        try {
            if (obproxyConfig == null) {
                throw new IOException("OBProxyVCSocket native socket() failed, config is null");
            } else {
                fd = socket(obproxyConfig);
            }
            logger.debug("OBProxyVCSocket end init fd:{} ", this.fd);
            if (fd == 0) {
                logger.debug("OBProxyVCSocket end init error fd:{}", this.fd);
                throw new IOException("OBProxyVCSocket init socket error with ret:" + fd);
            }

        } catch (IOException lee) {
            throw InitProxyVCIOException("OBProxyVCSocket native socket() failed", lee);
        }
    }

    public OBProxyVCSocket(String obproxyConfig) throws IOException {
        logger.debug("OBProxyVCSocket start init obproxyConfig:{} ", obproxyConfig);
        if ((System.getProperty("os.name").toLowerCase()).startsWith("win")) {
            throw new IOException("OBProxyVC sockets are not supported on Windows");
        }
        this.recvBuffLen = DEFAULT_RCV_BUFFER_LEN;
        this.sendBuffLen = DEFAULT_SND_BUFFER_LEN;
        try {
            sendBuff = new byte[sendBuffLen];
            recvBuff = new byte[recvBuffLen];
            recvBuffDataSpos = 0;
            recvBuffDataSpos = 0;
            recvBuffDataLen = 0;

        } catch (Exception e) {
            throw InitProxyVCIOException("OBProxyVCSocket init buffer error", e);
        }
        closeLock.set(false);
        try {
            if (obproxyConfig == null) {
                throw new IOException("OBProxyVCSocket native socket() failed, config is null");
            } else {
                fd = socket(obproxyConfig);
            }
            if (fd < 0) {
                logger.debug("OBProxyVCSocket end init error ret:{}", this.fd);
                throw new IOException("OBProxyVCSocket init socket error with ret:" + fd);
            }
        } catch (IOException lee) {
            throw InitProxyVCIOException("OBProxyVCSocket native socket() failed", lee);
        }
        logger.debug("OBProxyVCSocket start init obproxyConfig finish fd:{}", this.fd);
    }

    private static native long socket(String config) throws IOException;

    private static native int connect(long sockfd, int conn_timeout, int socketTimeout)
                                                                                       throws IOException;

    private static native int recv(long fd, byte[] buff, int offset, int count, int flags)
                                                                                          throws IOException;

    private static native int send(long fd, byte[] buffer, int offset, int count, int flags)
                                                                                            throws IOException;

    private static native int close(long fd) throws IOException;

    private static native int setClientId(long fd, long cs_id) throws IOException;

    public void setCsId(long csId) throws IOException {
        this.obproxyClientSessionId = csId;
        try {
            if (closed) {
                throw new SocketException("Socket closed");
            }
            setClientId(fd, csId);
        } catch (IOException e) {
            throw InitProxyVCIOException("native set_cs_id() failed", e);
        }
    }

    private String formatError(String info) {
        if (info != null) {
            return "ObproxyVCSocket(cs_id:" + obproxyClientSessionId + ", fd:" + fd + ") native "
                   + info + " function failed";
        } else {
            return "ObproxyVCSocket(cs_id:" + obproxyClientSessionId + ", fd:" + fd + ") failed";
        }
    }

    private static String formatError(IOException lee) {
        try {
            return ""; //TODO change msg for error code
        } catch (Throwable t) {
            return lee.getMessage();
        }
    }

    @Override
    public boolean isConnected() {
        return connected;
    }

    @Override
    public void close() throws IOException {
        if (!closeLock.getAndSet(true)) {
            if (closed) {
                throw new SocketException("Socket closed");
            }
            try {
                closed = true; //directly set closed status in
                close(fd);
            } catch (IOException lee) {
                throw InitProxyVCIOException(formatError("close"), lee);
            }
            connected = false;
        }
    }

    @Override
    public void connect(SocketAddress endpoint) throws IOException {
        connect(endpoint, 0);
    }

    public void connect(SocketAddress endpoint, int timeout) throws IOException {
        logger.debug("OBProxyVCSocket connect:{} ", this.fd);
        try {
            int ret = connect(fd, timeout, this.socketTimeout);
            if (ret != 0) {
                connected = false;
                logger.warn("fd {} native connect connect ret:{} ", this.fd, ret);
                throw new IOException("native connect() method error ret:" + ret);
            }
            connected = true;
        } catch (IOException lee) {
            close();
            throw InitProxyVCIOException(formatError("connect"), lee);
        }
        is = new OBProxyVCSocketInputStream();
        os = new OBProxyVCSocketOutputStream();
        closed = false;
        logger.debug("OBProxyVCSocket connect end");
    }

    public InputStream getInputStream() {
        return is;
    }

    public OutputStream getOutputStream() {
        return os;
    }

    public void setTcpNoDelay(boolean b) {
        // do nothing
    }

    public void setKeepAlive(boolean b) {
        // do nothing
    }

    public void setReceiveBufferSize(int size) {
        // do nothing
    }

    public void setSendBufferSize(int size) {
        // do nothing
    }

    public void setSoLinger(boolean b, int i) {
        // do nothing
    }

    public void setSoTimeout(int timeout) {
        // do nothing
        this.socketTimeout = timeout;
    }

    public void shutdownInput() {
        // do nothing
    }

    public void shutdownOutput() {
        // do nothing
    }

    class OBProxyVCSocketInputStream extends InputStream {
        public int readIntern(byte[] bytesEntry, int off, int len) throws IOException {
            int bytes = 0;
            int size = 0;
            int remainingLength = len;
            int flag = socketTimeout > 0 ? VC_SOCK_RD_WITH_TIMEOUT : 0;

            if (remainingLength > recvBuffDataLen) { //need read the data from obproxy
                if (recvBuffDataLen > 0) {
                    System.arraycopy(recvBuff, recvBuffDataSpos, bytesEntry, off, recvBuffDataLen);
                    off += recvBuffDataLen;
                    size += recvBuffDataLen;
                    remainingLength -= recvBuffDataLen;
                    recvBuffDataLen = 0;
                }

                int read_len = recvBuffLen;
                bytes = recv(fd, recvBuff, 0, read_len, flag);
                //                logger.debug("read recv from OBproxy(cs_id:{}): return:{}", obproxyClientSessionId, bytes);

                if (bytes < 0 || bytes > read_len) {
                    logger.error("read error: return:{}", bytes);
                    if (bytes != -8) {
                        throw new IOException("cs_id:" + obproxyClientSessionId + " "
                                              + "native recv method error ret:" + bytes);
                    } else {
                        throw new SocketTimeoutException("socket cs_id:" + obproxyClientSessionId
                                                         + " " + "timeout occured");
                    }
                } else {
                    recvBuffDataSpos = 0;
                    recvBuffDataLen = bytes;
                }
            }

            if (remainingLength > 0 && bytes >= 0) {
                int copy_len = recvBuffDataLen > remainingLength ? remainingLength
                    : recvBuffDataLen;
                System.arraycopy(recvBuff, recvBuffDataSpos, bytesEntry, off, copy_len);
                recvBuffDataSpos += copy_len;
                recvBuffDataLen -= copy_len;
                size += copy_len;
            }
            //            logger.debug("cs_id:{} readIntern return{} and remainingLength {}", obproxyClientSessionId, size, remainingLength);
            return size;
        }

        @Override
        public int read(byte[] bytesEntry, int off, int len) throws IOException {
            //            logger
            //                .debug(
            //                    "OBProxyVCSocket.OBProxyVCSocketInputStream(cs_id: {}, fd:{}) need read: offset:{} len:{} ",
            //                    obproxyClientSessionId, fd, off, len);

            if (closed) {
                throw new SocketException("Socket closed");
            }

            try {
                if (off > 0) {
                    int bytes = 0;
                    int remainingLength = len;
                    int size;

                    do {
                        size = readIntern(bytesEntry, off, remainingLength);
                        if (size > 0) {
                            off += size;
                            remainingLength -= size;
                            bytes += size;
                        } else {
                            bytes = size; //read error
                        }
                    } while ((remainingLength > 0) && (size > 0));
                    //                    logger.debug("cs_id:{} end read info : len:{}  bytes:{}", obproxyClientSessionId, len, bytes);
                    return bytes;
                } else {
                    return readIntern(bytesEntry, 0, len);
                }
            } catch (IOException lee) {
                close();
                //                logger
                //                    .error(
                //                        "OBProxyVCSocket.OBProxyVCSocketInputStream(cs_id:{}, fd:{}) exception: offset:{} len:{} ",
                //                        obproxyClientSessionId, fd, off, len);
                throw InitProxyVCIOException(formatError("read"), lee);
            }
        }

        @Override
        public int read() throws IOException {
            byte[] bytes = new byte[1];
            int bytesRead = read(bytes);
            if (bytesRead == 0) {
                return -1;
            }
            return bytes[0] & 0xff;
        }

        @Override
        public int read(byte[] bytes) throws IOException {
            return read(bytes, 0, bytes.length);
        }
    }

    class OBProxyVCSocketOutputStream extends OutputStream {

        @Override
        public void write(byte[] bytesEntry, int off, int len) throws IOException {
            int bytes;
            //            logger
            //                .debug(
            //                    "OBProxyVCSocket.OBProxyVCSocketOutputStream(cs_id:{}, fd:{}) need write: offset:{} len:{}",
            //                    obproxyClientSessionId, fd, off, len);

            if (closed) {
                throw new SocketException("Socket closed");
            }
            try {
                if (off > 0) {
                    int size;
                    int remainingLength = len;
                    //                    byte[] data = new byte[(len < 10240) ? len : 10240];
                    do {
                        size = (remainingLength < sendBuffLen) ? remainingLength : sendBuffLen;
                        System.arraycopy(bytesEntry, off, sendBuff, 0, size);
                        //                        logger.debug("write(cs_id:{}) before{} bytes", obproxyClientSessionId, size);
                        bytes = send(fd, sendBuff, 0, size, 0);
                        //                        logger.debug("write(cs_id:{}), {} bytes and return {}", obproxyClientSessionId, size, bytes);
                        if (bytes < 0) {
                            logger.error("write(cs_id:{}) error: return:{}",
                                obproxyClientSessionId, bytes);
                            throw new IOException("OBProxyVCSocket(cs_id:" + obproxyClientSessionId
                                                  + " , fd:" + fd + " native write error:" + bytes);
                        } else {
                            logger.debug("write(cs_id:{}) size {}", obproxyClientSessionId, bytes);
                        }
                        if (bytes > 0) {
                            off += bytes;
                            remainingLength -= bytes;
                        }
                    } while ((remainingLength > 0) && (bytes > 0));
                    logger.debug("cs_id:{} end write info : len:{}  size:{}",
                        obproxyClientSessionId, len, bytes);
                } else {
                    //                    logger.debug("write(cs_id:{}) before{} bytes oooo", obproxyClientSessionId, len);
                    bytes = send(fd, bytesEntry, 0, len, 0);
                    //                    logger.debug("write(cs_id:{}), {} bytes and return {} oooo", obproxyClientSessionId, len, bytes);
                    if (bytes < 0) {
                        logger.error("write(cs_id:{}) error: return:{}", obproxyClientSessionId,
                            bytes);
                        throw new IOException("OBProxyVCSocket(cs_id:" + obproxyClientSessionId
                                              + " , fd:" + fd + " native write error:" + bytes);
                    } else {
                        //                        logger.debug("write(cs_id:{}) info : len:{}, size:{}", obproxyClientSessionId, len, bytes);
                        //Do nothing
                    }
                }

                if (bytes != len) {
                    throw new IOException("OBProxyVCSocket(cs_id:" + obproxyClientSessionId
                                          + " , fd:" + fd + " can't write:" + len + ", but:"
                                          + bytes);
                }
            } catch (IOException lee) {
                close();
                logger
                    .error(
                        "OBProxyVCSocket.OBProxyVCSocketOutputStream(cs_id:{}, fd:{}) exception: offset:{} len:{} ",
                        obproxyClientSessionId, fd, off, len);
                throw InitProxyVCIOException(formatError("write"), lee);
            }
        }

        @Override
        public void write(int value) throws IOException {
            write(new byte[] { (byte) value });
        }

        @Override
        public void write(byte[] bytes) throws IOException {
            write(bytes, 0, bytes.length);
        }
    }
}
