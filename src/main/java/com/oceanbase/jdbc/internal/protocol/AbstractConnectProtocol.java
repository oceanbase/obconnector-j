/**
 * OceanBase Client for Java
 * <p>
 * Copyright (c) 2012-2014 Monty Program Ab.
 * Copyright (c) 2015-2020 MariaDB Corporation Ab.
 * Copyright (c) 2021 OceanBase.
 * <p>
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 * <p>
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 * <p>
 * You should have received a copy of the GNU Lesser General Public License along
 * with this library; if not, write to Monty Program Ab info@montyprogram.com.
 * <p>
 * This particular MariaDB Client for Java file is work
 * derived from a Drizzle-JDBC. Drizzle-JDBC file which is covered by subject to
 * the following copyright and notice provisions:
 * <p>
 * Copyright (c) 2009-2011, Marcus Eriksson
 * <p>
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 * Redistributions of source code must retain the above copyright notice, this list
 * of conditions and the following disclaimer.
 * <p>
 * Redistributions in binary form must reproduce the above copyright notice, this
 * list of conditions and the following disclaimer in the documentation and/or
 * other materials provided with the distribution.
 * <p>
 * Neither the name of the driver nor the names of its contributors may not be
 * used to endorse or promote products derived from this software without specific
 * prior written permission.
 * <p>
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
 */
package com.oceanbase.jdbc.internal.protocol;

import static java.sql.Connection.*;

import static com.oceanbase.jdbc.internal.com.send.parameters.LongDataParameterHolder.BLOB_STREAM_READ_BUF_SIZE;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

import com.oceanbase.jdbc.HostAddress;
import com.oceanbase.jdbc.OceanBaseConnection;
import com.oceanbase.jdbc.UrlParser;
import com.oceanbase.jdbc.authentication.AuthenticationPlugin;
import com.oceanbase.jdbc.authentication.AuthenticationPluginLoader;
import com.oceanbase.jdbc.credential.Credential;
import com.oceanbase.jdbc.credential.CredentialPlugin;
import com.oceanbase.jdbc.internal.com.Packet;
import com.oceanbase.jdbc.internal.com.read.Buffer;
import com.oceanbase.jdbc.internal.com.read.ErrorPacket;
import com.oceanbase.jdbc.internal.com.read.ReadInitialHandShakePacket;
import com.oceanbase.jdbc.internal.com.read.dao.Results;
import com.oceanbase.jdbc.internal.com.send.SendClosePacket;
import com.oceanbase.jdbc.internal.com.send.SendHandshakeResponsePacket;
import com.oceanbase.jdbc.internal.com.send.SendSslConnectionRequestPacket;
import com.oceanbase.jdbc.internal.com.send.authentication.OldPasswordPlugin;
import com.oceanbase.jdbc.internal.failover.FailoverProxy;
import com.oceanbase.jdbc.internal.io.LruTraceCache;
import com.oceanbase.jdbc.internal.io.input.DecompressPacketInputStream;
import com.oceanbase.jdbc.internal.io.input.Ob20PacketInputStream;
import com.oceanbase.jdbc.internal.io.input.PacketInputStream;
import com.oceanbase.jdbc.internal.io.input.StandardPacketInputStream;
import com.oceanbase.jdbc.internal.io.output.CompressPacketOutputStream;
import com.oceanbase.jdbc.internal.io.output.Ob20PacketOutputStream;
import com.oceanbase.jdbc.internal.io.output.PacketOutputStream;
import com.oceanbase.jdbc.internal.io.output.StandardPacketOutputStream;
import com.oceanbase.jdbc.internal.io.socket.OBProxyVCSocket;
import com.oceanbase.jdbc.internal.logging.Logger;
import com.oceanbase.jdbc.internal.logging.LoggerFactory;
import com.oceanbase.jdbc.internal.protocol.flt.FullLinkTrace;
import com.oceanbase.jdbc.internal.protocol.flt.OceanBaseProtocolV20;
import com.oceanbase.jdbc.internal.util.ServerPrepareStatementCache;
import com.oceanbase.jdbc.internal.util.Utils;
import com.oceanbase.jdbc.internal.util.constant.HaMode;
import com.oceanbase.jdbc.internal.util.constant.ParameterConstant;
import com.oceanbase.jdbc.internal.util.constant.ServerStatus;
import com.oceanbase.jdbc.internal.util.constant.Version;
import com.oceanbase.jdbc.internal.util.exceptions.ExceptionFactory;
import com.oceanbase.jdbc.internal.util.pool.GlobalStateInfo;
import com.oceanbase.jdbc.tls.TlsSocketPlugin;
import com.oceanbase.jdbc.tls.TlsSocketPluginLoader;
import com.oceanbase.jdbc.util.Options;

public abstract class AbstractConnectProtocol implements Protocol {

    private static final String           SESSION_QUERY                = ("SELECT @@max_allowed_packet,"
                                                                          + "@@system_time_zone,"
                                                                          + "@@time_zone,"
                                                                          + "@@auto_increment_increment,"
                                                                          + "@@tx_isolation AS tx_isolation,"
                                                                          + "@@session.tx_read_only AS tx_read_only");
    private static final String           SESSION_QUERY_ORACLE         = ("SELECT @@max_allowed_packet,"
                                                                          + "@@system_time_zone,"
                                                                          + "@@time_zone,"
                                                                          + "@@auto_increment_increment,"
                                                                          + "@@tx_isolation AS tx_isolation,"
                                                                          + "@@session.tx_read_only AS tx_read_only "
                                                                          + "from dual");
    private static final String           OB_VERSION_ORACLE_MODE       = ("select @@version_comment, @@version, ob_version() from dual where rownum <= 1");
    private static final String           OB_VERSION_MYSQL_MODE        = ("select @@version_comment, @@version, ob_version() limit 1");
    private static final String           IS_MASTER_QUERY              = "select @@innodb_read_only";
    protected static final String         CHECK_GALERA_STATE_QUERY     = "show status like 'wsrep_local_state'";
    private static final String           ALTER_SESSION_TIMEZONE_QUERY = "ALTER SESSION SET TIME_ZONE='";
    private static final Logger           logger                       = LoggerFactory
                                                                           .getLogger(AbstractConnectProtocol.class);
    protected final ReentrantLock         lock;
    protected final UrlParser             urlParser;
    protected final Options               options;
    protected final LruTraceCache         traceCache;
    protected String                      username;
    private final GlobalStateInfo         globalInfo;
    public boolean                        hasWarnings                  = false;
    public Results                        activeStreamingResult        = null;
    public short                          serverStatus;
    protected int                         autoIncrementIncrement;
    protected Socket                      socket;
    protected PacketOutputStream          writer;
    protected boolean                     readOnly                     = false;
    protected PacketInputStream           reader;
    protected FailoverProxy               proxy;
    protected volatile boolean            connected                    = false;
    protected boolean                     explicitClosed               = false;
    protected String                      database;
    protected long                        serverThreadId;
    protected ServerPrepareStatementCache serverPrepareStatementCache;
    protected boolean                     eofDeprecated                = false;
    protected long                        serverCapabilities;
    protected int                         socketTimeout;
    protected ExceptionFactory            exceptionFactory;
    protected final List<String>          galeraAllowedStates;
    private HostAddress                   currentHost;
    private boolean                       hostFailed;
    private boolean                       serverMariaDb;
    protected String                      serverVersion;
    private int                           majorVersion;
    private int                           minorVersion;
    private int                           patchVersion;
    private String                        obServerVersion;
    protected boolean                     supportStmtPrepareExecute;
    private boolean                       supportFetchWithOffset;
    private TimeZone                      timeZone;
    private TimeZone                      serverTimeZone;
    private boolean                       isOracleMode;                                                                                                    // is server in oracle mode
    private boolean                       importedTimeZoneTables;                                                                                          // is server time zone impoted
    public boolean                        autoCommit;
    public int                            transactionIsolationLevel    = 0;
    protected byte[]                      seed;
    long                                  clientCapabilities           = 0;
    String                                authenticationPluginType;
    byte                                  exchangeCharset;
    String                                host;

    protected boolean                     useOceanBaseProtocolV20;
    protected boolean                     enableFullLinkTrace;
    protected boolean                     useNewExtraInfo;
    OceanBaseProtocolV20                  ob20;
    FullLinkTrace                         fullLinkTrace;
    TimeTrace                             timeTrace;

    /**
     * Get a protocol instance.
     *
     * @param urlParser connection URL information
     * @param globalInfo server global variables information
     * @param lock the lock for thread synchronisation
     * @param traceCache trace cache
     */

    public AbstractConnectProtocol(final UrlParser urlParser, final GlobalStateInfo globalInfo,
                                   final ReentrantLock lock, LruTraceCache traceCache) {
        urlParser.auroraPipelineQuirks();
        this.lock = lock;
        this.urlParser = urlParser;
        this.options = urlParser.getOptions();
        this.database = (urlParser.getDatabase() == null ? "" : urlParser.getDatabase());
        this.username = (urlParser.getUsername() == null ? "" : urlParser.getUsername());
        this.globalInfo = globalInfo;
        if (options.cachePrepStmts && options.useServerPrepStmts) {
            serverPrepareStatementCache = ServerPrepareStatementCache.newInstance(
                options.prepStmtCacheSize, this);
        }
        galeraAllowedStates = urlParser.getOptions().galeraAllowedState == null ? Collections
            .emptyList() : Arrays.asList(urlParser.getOptions().galeraAllowedState.split(","));
        this.traceCache = traceCache;
    }

    private static void closeSocket(PacketInputStream packetInputStream,
                                    PacketOutputStream packetOutputStream, Socket socket) {
        try {
            try {
                long maxCurrentMillis = System.currentTimeMillis() + 10;
                socket.shutdownOutput();
                socket.setSoTimeout(3);
                InputStream is = socket.getInputStream();
                //noinspection StatementWithEmptyBody
                while (is.read() != -1 && System.currentTimeMillis() < maxCurrentMillis) {
                    // read byte
                }
            } catch (Throwable t) {
                // eat exception
            }
            packetOutputStream.close();
            packetInputStream.close();
        } catch (IOException e) {
            // eat
        } finally {
            try {
                socket.close();
            } catch (IOException e) {
                // socket closed, if any error, so not throwing error
            }
        }
    }

    private static Socket createSocket(final String host, final int port, final Options options)
                                                                                                throws IOException {
        Socket socket;
        socket = Utils.createSocket(options, host);
        socket.setTcpNoDelay(options.tcpNoDelay);

        if (options.socketTimeout != null) {
            socket.setSoTimeout(options.socketTimeout);
        }
        if (options.tcpKeepAlive) {
            socket.setKeepAlive(true);
        }
        if (options.tcpRcvBuf != null) {
            socket.setReceiveBufferSize(options.tcpRcvBuf);
        }
        if (options.tcpSndBuf != null) {
            socket.setSendBufferSize(options.tcpSndBuf);
        }
        if (options.tcpAbortiveClose) {
            socket.setSoLinger(true, 0);
        }

        // Bind the socket to a particular interface if the connection property
        // localSocketAddress has been defined.
        if (options.localSocketAddress != null) {
            InetSocketAddress localAddress = new InetSocketAddress(options.localSocketAddress, 0);
            socket.bind(localAddress);
        }

        if (!socket.isConnected()) {
            InetSocketAddress sockAddr;
            if (options.socksProxyHost != null && options.socksProxyPort > 0) {
                sockAddr = InetSocketAddress.createUnresolved(host, port);
            } else {
                sockAddr = options.pipe == null ? new InetSocketAddress(host, port) : null;
            }
            socket.connect(sockAddr, options.connectTimeout);
        }
        return socket;
    }

    private static long initializeClientCapabilities(final Options options,
                                                     final long serverCapabilities,
                                                     final String database) {
        long capabilities = OceanBaseCapabilityFlag.CLIENT_IGNORE_SPACE
                            | OceanBaseCapabilityFlag.CLIENT_PROTOCOL_41
                            | OceanBaseCapabilityFlag.CLIENT_TRANSACTIONS
                            | OceanBaseCapabilityFlag.CLIENT_SECURE_CONNECTION
                            | OceanBaseCapabilityFlag.CLIENT_MULTI_RESULTS
                            | OceanBaseCapabilityFlag.CLIENT_PS_MULTI_RESULTS
                            | OceanBaseCapabilityFlag.CLIENT_PLUGIN_AUTH
                            | OceanBaseCapabilityFlag.CLIENT_CONNECT_ATTRS
                            | OceanBaseCapabilityFlag.CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA
                            | OceanBaseCapabilityFlag.CLIENT_SESSION_TRACK
                            | OceanBaseCapabilityFlag.CLIENT_SUPPORT_ORACLE;

        if (options.allowLocalInfile) {
            capabilities |= OceanBaseCapabilityFlag.CLIENT_LOCAL_FILES;
        }

        // MySQL/MariaDB has two ways of calculating row count, eg for an UPDATE statement.
        // The default (and JDBC standard) is "found rows". The other option is "affected rows".
        // See https://jira.mariadb.org/browse/CONJ-384
        if (!options.useAffectedRows) {
            capabilities |= OceanBaseCapabilityFlag.CLIENT_FOUND_ROWS;
        }

        if (options.allowMultiQueries || (options.rewriteBatchedStatements)) {
            capabilities |= OceanBaseCapabilityFlag.CLIENT_MULTI_STATEMENTS;
        }

        if ((serverCapabilities & OceanBaseCapabilityFlag.CLIENT_DEPRECATE_EOF) != 0) {
            capabilities |= OceanBaseCapabilityFlag.CLIENT_DEPRECATE_EOF;
        }

        if (options.useCompression) {
            if ((serverCapabilities & OceanBaseCapabilityFlag.CLIENT_COMPRESS) == 0) {
                // ensure that server has compress capacity - MaxScale doesn't
                options.useCompression = false;
            } else {
                capabilities |= OceanBaseCapabilityFlag.CLIENT_COMPRESS;
            }
        }

        if (options.interactiveClient) {
            capabilities |= OceanBaseCapabilityFlag.CLIENT_INTERACTIVE;
        }

        // If a database is given, but createDatabaseIfNotExist is not defined or is false,
        // then just try to connect to the given database
        if (!database.isEmpty() && !options.createDatabaseIfNotExist) {
            capabilities |= OceanBaseCapabilityFlag.CLIENT_CONNECT_WITH_DB;
        }
        // tell ObServer if client support LobLocator.
        if (options.supportLobLocator) {
            capabilities |= OceanBaseCapabilityFlag.CLIENT_SUPPORT_LOB_LOCATOR;
        }
        return capabilities;
    }

    /**
     * Return possible protocols : values of option enabledSslProtocolSuites is set, or default to
     * "TLSv1,TLSv1.1". MariaDB versions &ge; 10.0.15 and &ge; 5.5.41 supports TLSv1.2 if compiled
     * with openSSL (default). MySQL community versions &ge; 5.7.10 is compile with yaSSL, so max TLS
     * is TLSv1.1.
     *
     * @param sslSocket current sslSocket
     * @throws SQLException if protocol isn't a supported protocol
     */
    private static void enabledSslProtocolSuites(SSLSocket sslSocket, Options options)
                                                                                      throws SQLException {
        if (options.enabledSslProtocolSuites != null) {
            List<String> possibleProtocols = Arrays.asList(sslSocket.getSupportedProtocols());
            String[] protocols = options.enabledSslProtocolSuites.split("[,;\\s]+");
            for (String protocol : protocols) {
                if (!possibleProtocols.contains(protocol)) {
                    throw new SQLException("Unsupported SSL protocol '"
                                           + protocol
                                           + "'. Supported protocols : "
                                           + possibleProtocols.toString().replace("[", "")
                                               .replace("]", ""));
                }
            }
            sslSocket.setEnabledProtocols(protocols);
        }
    }

    /**
     * Set ssl socket cipher according to options.
     *
     * @param sslSocket current ssl socket
     * @throws SQLException if a cipher isn't known
     */
    private static void enabledSslCipherSuites(SSLSocket sslSocket, Options options)
                                                                                    throws SQLException {
        if (options.enabledSslCipherSuites != null) {
            List<String> possibleCiphers = Arrays.asList(sslSocket.getSupportedCipherSuites());
            String[] ciphers = options.enabledSslCipherSuites.split("[,;\\s]+");
            for (String cipher : ciphers) {
                if (!possibleCiphers.contains(cipher)) {
                    throw new SQLException("Unsupported SSL cipher '"
                                           + cipher
                                           + "'. Supported ciphers : "
                                           + possibleCiphers.toString().replace("[", "")
                                               .replace("]", ""));
                }
            }
            sslSocket.setEnabledCipherSuites(ciphers);
        }
    }

    /** Closes socket and stream readers/writers Attempts graceful shutdown. */
    public void close() {
        boolean locked = false;
        if (lock != null) {
            locked = lock.tryLock();
        }
        try {
            this.connected = false;
            whyConnectedIsFalse();

            try {
                /* If a streaming result set is open, close it.*/
                skip();
            } catch (Exception e) {
                /* eat exception */
            }

            SendClosePacket.send(writer);
            closeSocket(reader, writer, socket);
            cleanMemory();
        } finally {
            if (locked) {
                lock.unlock();
            }
        }
    }

    /** Force closes socket and stream readers/writers. */
    public void abort() {
        this.explicitClosed = true;

        boolean lockStatus = false;
        if (lock != null) {
            lockStatus = lock.tryLock();
        }
        try {
            this.connected = false;
            whyConnectedIsFalse();

            abortActiveStream();

            if (!lockStatus) {
                // lock not available : query is running
                // force end by executing an KILL connection
                forceAbort();
                try {
                    socket.setSoTimeout(10);
                    socket.setSoLinger(true, 0);
                } catch (IOException ioException) {
                    // eat
                }
            } else {
                SendClosePacket.send(writer);
            }

            closeSocket(reader, writer, socket);
            cleanMemory();
        } finally {
            if (lockStatus) {
                lock.unlock();
            }
        }
    }

    private void forceAbort() {
        try (MasterProtocol copiedProtocol =
                     new MasterProtocol(urlParser, new GlobalStateInfo(), new ReentrantLock(), traceCache)) {
            copiedProtocol.setHostAddress(getHostAddress());
            copiedProtocol.connect();
            // no lock, because there is already a query running that possessed the lock.
            copiedProtocol.executeQuery("KILL " + serverThreadId);
        } catch (SQLException | IOException sqle) {
            // eat
        }
    }

    private void abortActiveStream() {
        try {
            /* If a streaming result set is open, abort it.*/
            if (activeStreamingResult != null) {
                activeStreamingResult.abort();
                activeStreamingResult = null;
            }
        } catch (Exception e) {
            /* eat exception */
        }
    }

    /**
     * Skip packets not read that are not needed. Packets are read according to needs. If some data
     * have not been read before next execution, skip it. <i>Lock must be set before using this
     * method</i>
     *
     * @throws SQLException exception
     */
    public void skip() throws SQLException {
        if (activeStreamingResult != null) {
            activeStreamingResult.loadFully(true, this);
            activeStreamingResult = null;
        }
    }

    protected void cleanMemory() {
        if (options.cachePrepStmts && options.useServerPrepStmts) {
            serverPrepareStatementCache.clear();
        }
        if (options.enablePacketDebug) {
            traceCache.clearMemory();
        }
    }

    public void setServerStatus(short serverStatus) {
        this.serverStatus = serverStatus;
    }

    /** Remove flag has more results. */
    public void removeHasMoreResults() {
        if (hasMoreResults()) {
            this.serverStatus = (short) (serverStatus ^ ServerStatus.MORE_RESULTS_EXISTS);
        }
    }

    /**
     * Connect to currentHost.
     *
     * @throws SQLException exception
     */
    public void connect() throws SQLException, IOException {

        try {
            createConnection(currentHost, username);
        } catch (SQLException exception) {
            throw ExceptionFactory.INSTANCE.create(
                String.format("Could not connect to %s. %s", currentHost, exception.getMessage()
                                                                          + getTraces()), "08000",
                exception);
        }
    }

    private void createConnection(HostAddress hostAddress, String username) throws SQLException,
                                                                           IOException {
        if (options.log) {
            timeTrace = new TimeTrace();
        }

        String host = hostAddress != null ? hostAddress.host : null;
        int port = hostAddress != null ? hostAddress.port : 3306;

        Credential credential;
        CredentialPlugin credentialPlugin = urlParser.getCredentialPlugin();
        if (credentialPlugin != null) {
            credential = credentialPlugin.initialize(options, username, hostAddress).get();
        } else {
            credential = new Credential(username, urlParser.getPassword());
        }

        this.socket = createSocket(host, port, options);
        assignStream(this.socket, options);

        try {
            // parse server greeting packet.
            final ReadInitialHandShakePacket greetingPacket = new ReadInitialHandShakePacket(reader);
            this.serverThreadId = greetingPacket.getServerThreadId();
            this.serverVersion = greetingPacket.getServerVersion();
            this.serverMariaDb = greetingPacket.isServerMariaDb();
            this.serverCapabilities = greetingPacket.getServerCapabilities();
            this.reader.setServerThreadId(serverThreadId, null);
            this.writer.setServerThreadId(serverThreadId, null);

            parseVersion(greetingPacket.getServerVersion());

            byte exchangeCharset = decideLanguage(greetingPacket.getServerLanguage() & 0xFF);
            long clientCapabilities = initializeClientCapabilities(options, serverCapabilities,
                database);
            exceptionFactory = ExceptionFactory.of(serverThreadId, options);

            sslWrapper(host, socket, options, greetingPacket.getServerCapabilities(),
                clientCapabilities, exchangeCharset, serverThreadId);
            if (Boolean.TRUE.equals(options.useSsl)) {
                if ((serverCapabilities & OceanBaseCapabilityFlag.CLIENT_SSL) == 0) {
                    exceptionFactory.create(
                        "Trying to connect with ssl, but ssl not enabled in the server", "08000");
                }
                clientCapabilities |= OceanBaseCapabilityFlag.CLIENT_SSL;
            }
            String authenticationPluginType = greetingPacket.getAuthenticationPluginType();
            if (credentialPlugin != null
                && credentialPlugin.defaultAuthenticationPluginType() != null) {
                authenticationPluginType = credentialPlugin.defaultAuthenticationPluginType();
            }
            seed = greetingPacket.getSeed();
            this.authenticationPluginType = authenticationPluginType;
            this.host = host;
            this.exchangeCharset = exchangeCharset;
            this.clientCapabilities = clientCapabilities;
            authenticationHandler(exchangeCharset, clientCapabilities, authenticationPluginType,
                greetingPacket.getSeed(), options, database, credential, host);

            compressionHandler(options);
            if (socket instanceof OBProxyVCSocket) {
                // set the client session it for the trace info
                ((OBProxyVCSocket) socket).setCsId(this.serverThreadId);
            }
        } catch (IOException ioException) {
            destroySocket();
            if (host == null) {
                throw ExceptionFactory.INSTANCE.create(
                    String.format("Could not connect to socket : %s", ioException.getMessage()),
                    "08000", ioException);
            }

            throw ExceptionFactory.INSTANCE.create(String.format("Could not connect to %s:%s : %s",
                host, socket.getPort(), ioException.getMessage()), "08000", ioException);
        } catch (SQLException sqlException) {
            destroySocket();
            throw sqlException;
        }

        connected = true;

        this.reader.setServerThreadId(this.serverThreadId, isMasterConnection());
        this.writer.setServerThreadId(this.serverThreadId, isMasterConnection());

        if (this.options.socketTimeout != null) {
            this.socketTimeout = this.options.socketTimeout;
        }
        if ((serverCapabilities & OceanBaseCapabilityFlag.CLIENT_DEPRECATE_EOF) != 0) {
            eofDeprecated = true;
        }

        postConnectionQueries();

        // validate galera state
        if (isMasterConnection() && !galeraAllowedStates.isEmpty()) {
            galeraStateValidation();
        }

        activeStreamingResult = null;
        hostFailed = false;
    }

    /** Closing socket in case of Connection error after socket creation. */
    public void destroySocket() {
        if (this.reader != null) {
            try {
                this.reader.close();
            } catch (IOException ee) {
                // eat exception
            }
        }
        if (this.writer != null) {
            try {
                this.writer.close();
            } catch (IOException ee) {
                // eat exception
            }
        }
        if (this.socket != null) {
            try {
                this.socket.close();
            } catch (IOException ee) {
                // eat exception
            }
        }
    }

    private void sslWrapper(final String host, final Socket socket, final Options options,
                            final long serverCapabilities, long clientCapabilities,
                            final byte exchangeCharset, long serverThreadId) throws SQLException,
                                                                            IOException {
        if (Boolean.TRUE.equals(options.useSsl)) {

            if ((serverCapabilities & OceanBaseCapabilityFlag.CLIENT_SSL) == 0) {
                exceptionFactory.create(
                    "Trying to connect with ssl, but ssl not enabled in the server", "08000");
            }
            clientCapabilities |= OceanBaseCapabilityFlag.CLIENT_SSL;
            SendSslConnectionRequestPacket.send(writer, clientCapabilities, exchangeCharset);
            TlsSocketPlugin socketPlugin = TlsSocketPluginLoader.get(options.tlsSocketType);
            SSLSocketFactory sslSocketFactory = socketPlugin.getSocketFactory(options);
            SSLSocket sslSocket = socketPlugin.createSocket(socket, sslSocketFactory);

            enabledSslProtocolSuites(sslSocket, options);
            enabledSslCipherSuites(sslSocket, options);

            sslSocket.setUseClientMode(true);
            sslSocket.startHandshake();

            // perform hostname verification
            // (rfc2818 indicate that if "client has external information as to the expected identity of
            // the server, the hostname check MAY be omitted")
            if (!options.disableSslHostnameVerification && !options.trustServerCertificate) {
                SSLSession session = sslSocket.getSession();
                try {
                    socketPlugin.verify(host, session, options, serverThreadId);
                } catch (SSLException ex) {
                    throw exceptionFactory
                        .create(
                            "SSL hostname verification failed : "
                                    + ex.getMessage()
                                    + "\nThis verification can be disabled using the option \"disableSslHostnameVerification\" "
                                    + "but won't prevent man-in-the-middle attacks anymore",
                            "08006");
                }
            }

            assignStream(sslSocket, options);
        }
    }

    private void authenticationHandler(byte exchangeCharset, long clientCapabilities,
                                       String authenticationPluginType, byte[] seed,
                                       Options options, String database, Credential credential,
                                       String host) throws SQLException, IOException {

        // send Client Handshake Response
        String clientIp = this.socket.getLocalAddress().getHostAddress();
        SendHandshakeResponsePacket.send(writer, credential, host, database, clientCapabilities,
            serverCapabilities, exchangeCharset, (byte) (Boolean.TRUE.equals(options.useSsl) ? 0x02
                : 0x01), options, authenticationPluginType, seed, clientIp);

        writer.permitTrace(false);

        Buffer buffer = reader.getPacket(false);
        AtomicInteger sequence = new AtomicInteger(reader.getLastPacketSeq());

        authentication_loop: while (true) {
            switch (buffer.getByteAt(0) & 0xFF) {
                case 0xFE:
                    // *************************************************************************************
                    // Authentication Switch Request see
                    // https://mariadb.com/kb/en/library/connection/#authentication-switch-request
                    // *************************************************************************************
                    sequence.set(reader.getLastPacketSeq());
                    AuthenticationPlugin authenticationPlugin;
                    if ((serverCapabilities & OceanBaseCapabilityFlag.CLIENT_PLUGIN_AUTH) != 0) {
                        buffer.readByte();
                        String plugin;
                        if (buffer.remaining() > 0) {
                            // AuthSwitchRequest packet.
                            plugin = buffer.readStringNullEnd(StandardCharsets.US_ASCII);
                            seed = buffer.readBytes(buffer.remaining());
                        } else {
                            // OldAuthSwitchRequest
                            plugin = OldPasswordPlugin.TYPE;
                            seed = Utils.copyWithLength(seed, 8);
                        }

                        // Authentication according to plugin.
                        // see AuthenticationProviderHolder for implement other plugin
                        authenticationPlugin = AuthenticationPluginLoader.get(plugin);
                    } else {
                        authenticationPlugin = new OldPasswordPlugin();
                        seed = Utils.copyWithLength(seed, 8);
                    }

                    if (authenticationPlugin.mustUseSsl() && options.useSsl == null) {
                        throw exceptionFactory
                            .create(
                                "Connector use a plugin that require SSL without enabling ssl. "
                                        + "For compatibility, this can still be disabled explicitly forcing "
                                        + "'useSsl=false' in connection string." + "plugin is = "
                                        + authenticationPlugin.type(), "08004", 1251);
                    }

                    authenticationPlugin.initialize(credential.getPassword(), seed, options);
                    buffer = authenticationPlugin.process(writer, reader, sequence);
                    break;

                case 0xFF:
                    // *************************************************************************************
                    // ERR_Packet
                    // see https://mariadb.com/kb/en/library/err_packet/
                    // *************************************************************************************
                    ErrorPacket errorPacket = new ErrorPacket(buffer);
                    if (credential.getPassword() != null && !credential.getPassword().isEmpty()
                        && options.passwordCharacterEncoding == null
                        && errorPacket.getErrorCode() == 1045
                        && "28000".equals(errorPacket.getSqlState())) {
                        // Access denied
                        throw exceptionFactory
                            .create(
                                String
                                    .format(
                                        "%s\nCurrent charset is %s"
                                                + ". If password has been set using other charset, consider "
                                                + "using option 'passwordCharacterEncoding'",
                                        errorPacket.getMessage(), Charset.defaultCharset()
                                            .displayName()), errorPacket.getSqlState(), errorPacket
                                    .getErrorCode());
                    }
                    throw exceptionFactory.create(errorPacket.getMessage(),
                        errorPacket.getSqlState(), errorPacket.getErrorCode());

                case 0x00:
                    // *************************************************************************************
                    // OK_Packet -> Authenticated !
                    // see https://mariadb.com/kb/en/library/ok_packet/
                    // *************************************************************************************
                    buffer.skipByte(); // 0x00 OkPacket Header
                    buffer.skipLengthEncodedNumeric(); // affectedRows
                    buffer.skipLengthEncodedNumeric(); // insertId
                    if ((serverCapabilities & OceanBaseCapabilityFlag.CLIENT_PROTOCOL_41) != 0) {
                        serverStatus = buffer.readShort();
                        buffer.skipBytes(2);
                    } else if ((serverCapabilities & OceanBaseCapabilityFlag.CLIENT_TRANSACTIONS) != 0) {
                        serverStatus = buffer.readShort();
                    }
                    if ((serverStatus & ServerStatus.OB_SERVER_USE_ORACLE_MODE) != 0) {
                        setOracleMode(true);
                    }

                    if (buffer.remaining() <= 0) {
                        break authentication_loop;
                    }
                    if ((serverCapabilities & OceanBaseCapabilityFlag.CLIENT_SESSION_TRACK) != 0) {
                        buffer.skipLengthEncodedBytes(); // human readable status information
                        if ((serverStatus & ServerStatus.SERVER_SESSION_STATE_CHANGED) != 0) {
                            // session state changes
                            int totalLength = (int) buffer.getLengthEncodedNumeric();
                            int readLength = 0;
                            while (readLength < totalLength) {
                                readLength++;
                                // read type of data
                                switch (buffer.readByte()) {
                                // read data of the changed session info
                                    case 0x00: // SESSION_TRACK_SYSTEM_VARIABLES
                                        // read length
                                        int type = buffer.readByte();
                                        readLength++;
                                        long dataLength;
                                        switch (type) {
                                            case 251:
                                                dataLength = -1;
                                                break;
                                            case 252:
                                                dataLength = 0xffff & buffer.readShort();
                                                readLength += 2;
                                                break;
                                            case 253:
                                                dataLength = 0xffffff & buffer.readInt3Bytes();
                                                readLength += 3;
                                                break;
                                            case 254:
                                                dataLength = buffer.readLong();
                                                readLength += 8;
                                                break;
                                            default:
                                                dataLength = type;
                                                break;
                                        }

                                        // read string
                                        String name = buffer
                                            .readStringLengthEncoded(StandardCharsets.UTF_8);
                                        String value = buffer
                                            .readStringLengthEncoded(StandardCharsets.UTF_8);
                                        readLength += dataLength;

                                        // TODO: process nls_format

                                        // process ob_capability_flag
                                        if (name.equals("ob_capability_flag")) {
                                            useOceanBaseProtocolV20 = ((Long.parseLong(value) & OceanBaseCapabilityFlag.OB_CAP_OB_PROTOCOL_V2) != 0);
                                            enableFullLinkTrace = useOceanBaseProtocolV20
                                                                  && ((Long.parseLong(value) & OceanBaseCapabilityFlag.OB_CAP_FULL_LINK_TRACE) != 0); // full link trace require for OB2.0 protocol
                                            useNewExtraInfo = ((Long.parseLong(value) & OceanBaseCapabilityFlag.OB_CAP_NEW_EXTRA_INFO) != 0);
                                        }
                                        break;
                                    case 0x01: // SESSION_TRACK_SCHEMA
                                    case 0x02: // SESSION_TRACK_STATE_CHANGE
                                    case 0x03: // SESSION_TRACK_GTIDS
                                        break;
                                }
                            }
                        }
                    } else {
                        buffer.skipLengthEncodedBytes(); // human readable status information
                    }
                    break authentication_loop;

                default:
                    throw exceptionFactory.create("unexpected data during authentication (header="
                                                  + (buffer.getByteAt(0) & 0xFF), "08000");
            }
        }
        writer.permitTrace(true);
    }

    private void compressionHandler(Options options) throws IOException {
        if (useOceanBaseProtocolV20) {
            ob20 = new OceanBaseProtocolV20(useNewExtraInfo);
            writer = new Ob20PacketOutputStream(writer.getOutputStream(), serverThreadId, options,
                ob20, timeTrace);
            reader = new Ob20PacketInputStream(socket.getInputStream(), serverThreadId, options,
                ob20, timeTrace);

            if (enableFullLinkTrace) {
                fullLinkTrace = new FullLinkTrace(useNewExtraInfo);
                ob20.setTraceInfo(fullLinkTrace.traceInfo);
            }
        } else if (options.useCompression) {
            writer = new CompressPacketOutputStream(writer.getOutputStream(), serverThreadId,
                options, timeTrace);
            reader = new DecompressPacketInputStream(
                ((StandardPacketInputStream) reader).getInputStream(), serverThreadId, options,
                timeTrace);

            if (options.enablePacketDebug) {
                writer.setTraceCache(traceCache);
                reader.setTraceCache(traceCache);
            }
        }
    }

    private void assignStream(Socket socket, Options options) throws SQLException {
        try {
            writer = new StandardPacketOutputStream(socket.getOutputStream(), serverThreadId,
                options, timeTrace);
            reader = new StandardPacketInputStream(socket.getInputStream(), serverThreadId,
                options, timeTrace);

            if (options.enablePacketDebug) {
                writer.setTraceCache(traceCache);
                reader.setTraceCache(traceCache);
            }
        } catch (IOException ioe) {
            destroySocket();
            throw ExceptionFactory.INSTANCE.create("Socket error: " + ioe.getMessage(), "08000",
                ioe);
        }
    }

    private void galeraStateValidation() throws SQLException {
        ResultSet rs;
        try {
            Results results = new Results();
            executeQuery(true, results, CHECK_GALERA_STATE_QUERY);
            results.commandEnd();
            rs = results.getResultSet();

        } catch (SQLException sqle) {
            throw ExceptionFactory.of((int) serverThreadId, options).create(
                "fail to validate Galera state");
        }

        if (rs == null || !rs.next()) {
            throw ExceptionFactory.of((int) serverThreadId, options).create(
                "fail to validate Galera state");
        }

        if (!galeraAllowedStates.contains(rs.getString(2))) {
            throw ExceptionFactory.of((int) serverThreadId, options).create(
                String.format("fail to validate Galera state (State is %s)", rs.getString(2)));
        }
    }

    protected void postConnectionQueries() throws SQLException {
        try {

            if (options.usePipelineAuth
                    && (options.socketTimeout == null
                    || options.socketTimeout == 0
                    || options.socketTimeout > 500)) {
                // set a timeout to avoid hang in case server doesn't support pipelining
                socket.setSoTimeout(500);
            }

            boolean mustLoadAdditionalInfo = true;
            if (globalInfo != null) {
                if (globalInfo.isAutocommit() == options.autocommit) {
                    mustLoadAdditionalInfo = false;
                }
            }
            if (mustLoadAdditionalInfo) {
                if (!options.connectProxy) {
                    Map<String, String> serverData = new TreeMap<>();
                    if (options.usePipelineAuth && !options.createDatabaseIfNotExist) {
                        try {
                            sendPipelineAdditionalData();
                            readPipelineAdditionalData(serverData);
                        } catch (SQLException sqle) {
                            if ("08".equals(sqle.getSQLState())) {
                                throw sqle;
                            }
                            // in case pipeline is not supported
                            // (proxy flush socket after reading first packet)
                            additionalData(serverData);
                        }
                    } else {
                        additionalData(serverData);
                    }
                    writer.setMaxAllowedPacket(Integer.parseInt(serverData.get("max_allowed_packet")));
                    autoIncrementIncrement = Integer.parseInt(serverData.get("auto_increment_increment"));
                    String tmp = serverData.get("tx_read_only");
                    if(tmp.equals("OFF") || tmp.equals("ON") ) {
                        readOnly = tmp.equals("OFF") ? false : true;
                    } else {
                        readOnly = Integer.parseInt(tmp) == 0 ? false : true;
                    }
                    if(!readOnly) {
                        tmp = "0";
                    } else {
                        tmp = "1";
                    }
                    Map<String, Integer> mapTransIsolationNameToValue = new HashMap<String, Integer>(8);
                    mapTransIsolationNameToValue.put("READ-UNCOMMITTED", TRANSACTION_READ_UNCOMMITTED);
                    mapTransIsolationNameToValue.put("READ-COMMITTED", TRANSACTION_READ_COMMITTED);
                    mapTransIsolationNameToValue.put("REPEATABLE-READ", TRANSACTION_REPEATABLE_READ);
                    mapTransIsolationNameToValue.put("SERIALIZABLE", TRANSACTION_SERIALIZABLE);
                    this.transactionIsolationLevel = mapTransIsolationNameToValue.get(serverData.get("tx_isolation"));
                    loadCalendar(serverData.get("time_zone"), serverData.get("system_time_zone"));
                }
            } else {
                writer.setMaxAllowedPacket((int) globalInfo.getMaxAllowedPacket());
                autoIncrementIncrement = globalInfo.getAutoIncrementIncrement();
                loadCalendar(globalInfo.getTimeZone(), globalInfo.getSystemTimeZone());
            }


            reader.setServerThreadId(this.serverThreadId, isMasterConnection());
            writer.setServerThreadId(this.serverThreadId, isMasterConnection());

            activeStreamingResult = null;
            hostFailed = false;

            if (options.usePipelineAuth) {
                // reset timeout to configured value
                if (options.socketTimeout != null) {
                    socket.setSoTimeout(options.socketTimeout);
                } else {
                    socket.setSoTimeout(0);
                }
            }
            if(!options.connectProxy) {
                setObServerVersion(getObVersion());
            }

            //Note that this blobSendChunkSize  cannot exceed the value of "maxAllowedPacket" and, if that is the case, then this value will be corrected automatically
            if(options.useServerPrepStmts && !isOracleMode) {
                int preferredBlobSendChunkSize = options.blobSendChunkSize;

                // LONG_DATA and Mysql Protocol io  packet header size
                int packetHeaderSize = BLOB_STREAM_READ_BUF_SIZE + 11;
                int allowedBlobSendChunkSize = Math.min(preferredBlobSendChunkSize,
                    writer.getMaxAllowedPacket()) - packetHeaderSize;

                if (allowedBlobSendChunkSize <= 0) {
                    throw new SQLException( "Connection setting too low for 'maxAllowedPacket'. "
                        + "When 'useServerPrepStmts=true', 'maxAllowedPacket' must be higher than "
                        + packetHeaderSize
                        + ". Check also 'max_allowed_packet' in MySQL configuration files.");
                }
                options.blobSendChunkSize = allowedBlobSendChunkSize;
            }

        } catch (SocketTimeoutException timeoutException) {
            destroySocket();
            String msg = "Socket error during post connection queries: " + timeoutException.getMessage();
            if (options.usePipelineAuth) {
                msg +=
                        "\nServer might not support pipelining, try disabling with option `usePipelineAuth` and `useBatchMultiSend`";
            }
            throw exceptionFactory.create(msg, "08000", timeoutException);
        } catch (IOException ioException) {
            destroySocket();
            throw exceptionFactory.create(
                    "Socket error during post connection queries: " + ioException.getMessage(),
                    "08000",
                    ioException);
        } catch (SQLException sqlException) {
            destroySocket();
            throw sqlException;
        }
    }

    /**
     * Send all additional needed values. Command are send one after the other, assuming that command
     * are less than 65k (minimum hosts TCP/IP buffer size)
     *
     * @throws IOException if socket exception occur
     */
    private void sendPipelineAdditionalData() throws IOException, SQLException {
        sendSessionInfos();
        getResult(new Results());
        //        if (!isOracleMode) { // oracle mode should use SHOW VARIABLES but not SELECT @@session_vars
        //            getResult(new Results());
        //        }
        sendCharsetVariables();
        getResult(new Results());
        if (!isOracleMode && this.getOptions().connectionCollation != null) {
            setCollationVariables();
            getResult(new Results());
        }
    }

    private void sendSessionInfos() throws IOException {
        // In JDBC, connection must start in autocommit mode
        // [CONJ-269] we cannot rely on serverStatus & ServerStatus.AUTOCOMMIT before this command to
        // avoid this command.
        // if autocommit=0 is set on server configuration, DB always send Autocommit on serverStatus
        // flag
        // after setting autocommit, we can rely on serverStatus value
        StringBuilder sessionOption = new StringBuilder("autocommit=")
            .append(options.autocommit ? "1" : "0");
        this.autoCommit = options.autocommit;
        /* FIXME oceanbase not suppport client session track yet
        if ((serverCapabilities & MariaDbServerCapabilities.CLIENT_SESSION_TRACK) != 0) {
          if (options.trackSchema) {
            sessionOption.append(", session_track_schema=1");
          }
          if (options.rewriteBatchedStatements) {
            sessionOption.append(", session_track_system_variables='auto_increment_increment' ");
          }
        }
        */

        if (options.jdbcCompliantTruncation) {
            sessionOption.append(", sql_mode = concat(@@sql_mode,',STRICT_TRANS_TABLES')");
        }

        if (options.sessionVariables != null && !options.sessionVariables.isEmpty()) {
            sessionOption.append(",").append(Utils.parseSessionVariables(options.sessionVariables));
        }

        writer.startPacket(0);
        writer.write(Packet.COM_QUERY);
        writer.write("set " + sessionOption.toString());
        writer.flush();
    }

    private void sendRequestSessionVariables() throws IOException {
        writer.startPacket(0);
        writer.write(Packet.COM_QUERY);
        if (isOracleMode) {
            writer.write(SESSION_QUERY_ORACLE.getBytes());
        } else {
            writer.write(SESSION_QUERY);
        }
        writer.flush();
    }

    private void sendCharsetVariables() throws IOException {
        writer.startPacket(0);
        writer.write(Packet.COM_QUERY);
        String str;
        if (this.getOptions().characterEncoding.toUpperCase(Locale.ROOT).equals("UTF-8")) {
            str = "utf8";
        } else {
            str = this.getOptions().characterEncoding;
        }
        writer.write("set names " + str);
        writer.flush();
    }

    private void setCollationVariables() throws IOException {
        writer.startPacket(0);
        writer.write(Packet.COM_QUERY);
        String str = this.getOptions().connectionCollation;
        writer.write("set collation_connection = " + str);
        writer.flush();
    }

    private void sendAlterSessionTimezone(String zoneId) throws IOException {
        writer.startPacket(0);
        writer.write(Packet.COM_QUERY);
        writer.write(ALTER_SESSION_TIMEZONE_QUERY);
        if (!this.importedTimeZoneTables && zoneId.startsWith("GMT")) {
            writer.write(zoneId.substring("GMT".length()).getBytes());
        } else {
            writer.write(zoneId.getBytes());
        }
        writer.write('\'');
        writer.flush();
    }

    private void readRequestSessionVariables(Map<String, String> serverData) throws SQLException {
        Results results = new Results();
        getResult(results);
        results.commandEnd();
        ResultSet resultSet = results.getResultSet();
        if (resultSet != null) {
            resultSet.next();
            serverData.put("max_allowed_packet", resultSet.getString(1));
            serverData.put("system_time_zone", resultSet.getString(2));
            serverData.put("time_zone", resultSet.getString(3));
            serverData.put("auto_increment_increment", resultSet.getString(4));
            serverData.put("tx_isolation", resultSet.getString(5));
            serverData.put("tx_read_only", resultSet.getString(6));

        } else {
            throw exceptionFactory.create(
                "Error reading SessionVariables results. Socket is connected ? "
                        + socket.isConnected(), "08000");
        }
    }

    private void sendCreateDatabaseIfNotExist(String quotedDb) throws IOException {
        writer.startPacket(0);
        writer.write(Packet.COM_QUERY);
        writer.write("CREATE DATABASE IF NOT EXISTS " + quotedDb);
        writer.flush();
    }

    private void sendUseDatabaseIfNotExist(String quotedDb) throws IOException {
        writer.startPacket(0);
        writer.write(Packet.COM_QUERY);
        writer.write("USE " + quotedDb);
        writer.flush();
    }

    private void readPipelineAdditionalData(Map<String, String> serverData) throws SQLException,
                                                                           IOException {

        SQLException resultingException = null;

        boolean canTrySessionWithShow = false;
        if (isOracleMode) { // oracle mode should use SHOW VARIABLES but not SELECT @@
            canTrySessionWithShow = true;
        } else {
            try {
                sendRequestSessionVariables();
                readRequestSessionVariables(serverData);
            } catch (SQLException sqlException) {
                if (resultingException == null) {
                    resultingException = exceptionFactory.create("could not load system variables",
                        "08000", sqlException);
                    canTrySessionWithShow = true;
                }
            }
        }

        if (canTrySessionWithShow) {
            // fallback in case of galera non primary nodes that permit only show / set command,
            // not SELECT when not part of quorum
            requestSessionDataWithShow(serverData);
            connected = true;
            return;
        }

        if (resultingException != null) {
            throw resultingException;
        }
        connected = true;
    }

    private void requestSessionDataWithShow(Map<String, String> serverData) throws SQLException {
        try {
            Results results = new Results();
            executeQuery(true, results, "SHOW VARIABLES WHERE Variable_name in ("
                                        + "'max_allowed_packet'," + "'system_time_zone',"
                                        + "'tx_read_only'," + "'tx_isolation'," + "'time_zone',"
                                        + "'auto_increment_increment')");
            results.commandEnd();
            ResultSet resultSet = results.getResultSet();
            if (resultSet != null) {
                while (resultSet.next()) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("server data {} = {}", resultSet.getString(1),
                            resultSet.getString(2));
                    }
                    serverData.put(resultSet.getString(1), resultSet.getString(2));
                }
            }

            if (serverData.size() < 4) {
                throw exceptionFactory.create("could not load system variables. socket connected: "
                                              + socket.isConnected(), "08000");
            }

        } catch (SQLException sqlException) {
            throw exceptionFactory.create("could not load system variables", "08000", sqlException);
        }
    }

    private void additionalData(Map<String, String> serverData) throws IOException, SQLException {

        sendSessionInfos();
        getResult(new Results());

        try {
            sendCharsetVariables();
            getResult(new Results());
        } catch (SQLException e) {
            throw e;
        }
        try {
            if (!isOracleMode && this.getOptions().connectionCollation != null) {
                setCollationVariables();
                getResult(new Results());
            }
            if (!options.connectProxy) {
                sendRequestSessionVariables();
                readRequestSessionVariables(serverData);
            }
        } catch (SQLException sqlException) {
            requestSessionDataWithShow(serverData);
        }

        // for aurora, check that connection is master
        sendPipelineCheckMaster();
        readPipelineCheckMaster();

        if (options.createDatabaseIfNotExist && !database.isEmpty() && !isOracleMode) {
            // Try to create the database if it does not exist
            String quotedDb = OceanBaseConnection.quoteIdentifier(this.database);
            sendCreateDatabaseIfNotExist(quotedDb);
            getResult(new Results());

            sendUseDatabaseIfNotExist(quotedDb);
            getResult(new Results());
        }
    }

    /**
     * Is the connection closed.
     *
     * @return true if the connection is closed
     */
    public boolean isClosed() {
        return !this.connected;
    }

    private boolean getServerTZTablesImported() throws IOException, SQLException {
        try {
            writer.startPacket(0);
            writer.write(Packet.COM_QUERY);
            writer
                .write("select * from SYS.ALL_VIRTUAL_TENANT_TIME_ZONE_NAME_REAL_AGENT where rownum < 2;");
            writer.flush();
            Results results = new Results();
            getResult(results);
            results.commandEnd();
            ResultSet resultSet = results.getResultSet();
            return resultSet.next() ? true : false;
        } catch (SQLException e) {
            return false;
        }
    }

    private String getObVersion() throws IOException, SQLException {
        try {
            writer.startPacket(0);
            writer.write(Packet.COM_QUERY);
            if (isOracleMode) {
                writer.write(OB_VERSION_ORACLE_MODE.getBytes());
            } else {
                writer.write(OB_VERSION_MYSQL_MODE.getBytes());
            }
            writer.flush();
            Results results = new Results();
            getResult(results);
            results.commandEnd();
            ResultSet resultSet = results.getResultSet();
            resultSet.next();
            String ret = resultSet.getString(3);// ob_version()
            resultSet.close();
            return ret;
        } catch (SQLException e) {
            return "";
        }
    }

    private void loadCalendar(final String srvTimeZone, final String srvSystemTimeZone)
                                                                                       throws SQLException {
        String tz = null;
        try {
            if (isOracleMode) {
                // use server time zone
                tz = srvTimeZone;
                if (tz == null) {
                    tz = srvTimeZone;
                    if ("SYSTEM".equals(tz)) {
                        tz = srvSystemTimeZone;
                    }
                }
                this.importedTimeZoneTables = getServerTZTablesImported();
                // handle custom timezone id
                if (tz != null && tz.length() >= 2 && (tz.startsWith("+") || tz.startsWith("-"))
                    && Character.isDigit(tz.charAt(1))) {
                    tz = "GMT" + tz;
                }

                TimeZone defaultTimezone = TimeZone.getDefault();
                String offsetId = defaultTimezone.toZoneId().getRules()
                    .getStandardOffset(Instant.now()).getId();
                // TODO oracle should alter session to local time_zone, need more test case
                if (tz != null) {
                    timeZone = Utils.getTimeZone(tz);
                    serverTimeZone = timeZone;
                    if (defaultTimezone.getRawOffset() != timeZone.getRawOffset()) {
                        if (!this.importedTimeZoneTables) {
                            sendAlterSessionTimezone(offsetId);
                        } else {
                            sendAlterSessionTimezone(defaultTimezone.getID());
                        }
                        getResult(new Results());
                        timeZone = defaultTimezone;
                    } else {
                        timeZone = defaultTimezone;
                    }
                } else {
                    if (!this.importedTimeZoneTables) {
                        sendAlterSessionTimezone(offsetId);
                    } else {
                        sendAlterSessionTimezone(defaultTimezone.getID());
                    }
                    getResult(new Results());
                    timeZone = defaultTimezone;
                }

            } else {
                tz = options.serverTimezone;
                if (tz == null) {
                    tz = srvTimeZone;
                    if ("SYSTEM".equals(tz)) {
                        tz = srvSystemTimeZone;
                    }
                }
                // handle custom timezone id
                if (tz != null && tz.length() >= 2 && (tz.startsWith("+") || tz.startsWith("-"))
                    && Character.isDigit(tz.charAt(1))) {
                    tz = "GMT" + tz;
                }

                timeZone = Utils.getTimeZone(tz);
            }
        } catch (SQLException e) {
            if (options.serverTimezone != null) {
                throw exceptionFactory
                    .create(
                        "The server time_zone '"
                                + tz
                                + "' defined in the 'serverTimezone' parameter cannot be parsed "
                                + "by java TimeZone implementation. See java.util.TimeZone#getAvailableIDs() for available TimeZone, depending on your "
                                + "JRE implementation.", "01S00");
            } else {
                throw exceptionFactory
                    .create(
                        "The server time_zone '"
                                + tz
                                + "' cannot be parsed. The server time zone must defined in the "
                                + "jdbc url string with the 'serverTimezone' parameter (or server time zone must be defined explicitly with "
                                + "sessionVariables=time_zone='Canada/Atlantic' for example).  See "
                                + "java.util.TimeZone#getAvailableIDs() for available TimeZone, depending on your JRE implementation.",
                        "01S00");
            }
        } catch (IOException e) {
            throw new SQLException("Alter session set time_zone exception", e);
        }
    }

    /**
     * Check that current connection is a master connection (not read-only).
     *
     * @return true if master
     * @throws SQLException if requesting infos for server fail.
     */
    public boolean checkIfMaster() throws SQLException {
        return isMasterConnection();
    }

    /**
     * Default collation used for string exchanges with server.
     *
     * @param serverLanguage server default collation
     * @return collation byte
     */
    private byte decideLanguage(int serverLanguage) {
        // return current server utf8mb4 collation
        if (serverLanguage == 45 // utf8mb4_general_ci
            || serverLanguage == 46 // utf8mb4_bin
            || (serverLanguage >= 224 && serverLanguage <= 247)) {
            return (byte) serverLanguage;
        }

        if (getMajorServerVersion() == 5 && getMinorServerVersion() <= 1) {
            // 5.1 version doesn't know 4 bytes utf8
            return (byte) 33; // utf8_general_ci
        }

        // if server language is utf8mb3, use utf8mb4 equivalent collation
        if (serverLanguage == 33) {
            // utf8mb4_general_ci
            return 45;
        }

        if (serverLanguage == 83) {
            // utf8mb4_bin
            return 46;
        }

        if (serverLanguage >= 192 && serverLanguage <= 215) {
            // equivalent utf8mb4 collation
            return (byte) (serverLanguage + 32);
        }

        return (byte) 224; // UTF8MB4_UNICODE_CI;
    }

    /**
     * Check that next read packet is a End-of-file packet.
     *
     * @throws SQLException if not a End-of-file packet
     * @throws IOException if connection error occur
     */
    public void readEofPacket() throws SQLException, IOException {
        Buffer buffer = reader.getPacket(true);
        switch (buffer.getByteAt(0)) {
            case Packet.EOF:
                buffer.skipByte();
                this.hasWarnings = buffer.readShort() > 0;
                this.serverStatus = buffer.readShort();
                break;

            case Packet.ERROR:
                ErrorPacket ep = new ErrorPacket(buffer);
                throw exceptionFactory.create("Could not connect: " + ep.getMessage(),
                    ep.getSqlState(), ep.getErrorCode());

            default:
                throw exceptionFactory.create("Unexpected packet type " + buffer.getByteAt(0)
                                              + " instead of EOF", "08000");
        }
    }

    /**
     * Check that next read packet is a End-of-file packet.
     *
     * @throws SQLException if not a End-of-file packet
     * @throws IOException if connection error occur
     */
    public void skipEofPacket() throws SQLException, IOException {
        Buffer buffer = reader.getPacket(true);
        switch (buffer.getByteAt(0)) {
            case Packet.EOF:
                break;

            case Packet.ERROR:
                ErrorPacket ep = new ErrorPacket(buffer);
                throw exceptionFactory.create("Could not connect: " + ep.getMessage(),
                    ep.getSqlState(), ep.getErrorCode());

            default:
                throw exceptionFactory.create("Unexpected packet type " + buffer.getByteAt(0)
                                              + " instead of EOF");
        }
    }

    public void setHostFailedWithoutProxy() {
        hostFailed = true;
        close();
    }

    public UrlParser getUrlParser() {
        return urlParser;
    }

    /**
     * Indicate if current protocol is a master protocol.
     *
     * @return is master flag
     */
    public boolean isMasterConnection() {
        return currentHost == null || ParameterConstant.TYPE_MASTER.equals(currentHost.type);
    }

    /**
     * Send query to identify if server is master.
     *
     * @throws IOException in case of socket error.
     */
    @Deprecated
    // FIXME DELETE IT
    private void sendPipelineCheckMaster() throws IOException {
        if (urlParser.getHaMode() == HaMode.AURORA) {
            writer.startPacket(0);
            writer.write(Packet.COM_QUERY);
            writer.write(IS_MASTER_QUERY);
            writer.flush();
        }
    }

    @Deprecated
    // FIXME DELETE IT
    public void readPipelineCheckMaster() throws SQLException {
        // nothing if not aurora
    }

    public boolean mustBeMasterConnection() {
        return true;
    }

    public boolean noBackslashEscapes() {
        return ((serverStatus & ServerStatus.NO_BACKSLASH_ESCAPES) != 0);
    }

    /**
     * Connect without proxy. (use basic failover implementation)
     *
     * @throws SQLException exception
     */
    public void connectWithoutProxy() throws SQLException {
        if (!isClosed()) {
            close();
        }

        List<HostAddress> hostAddresses = urlParser.getHostAddresses();
        LinkedList<HostAddress> hosts = new LinkedList<>(hostAddresses);

        if (urlParser.getHaMode().equals(HaMode.LOADBALANCE)) {
            Collections.shuffle(hosts);
        }

        // CONJ-293 : handle name-pipe without host
        if (hosts.isEmpty()) {
            if (options.pipe != null) {
                try {
                    createConnection(null, username);
                    return;
                } catch (SQLException | IOException exception) {
                    throw ExceptionFactory.INSTANCE.create(
                            String.format(
                                    "Could not connect to named pipe '%s' : %s%s",
                                    options.pipe, exception.getMessage(), getTraces()),
                            "08000",
                            exception);
                }
            } else {
                throw ExceptionFactory.INSTANCE.create(
                        "No host is defined and pipe option is not set. "
                                + "Check if connection string respect format "
                                + "(jdbc:(mysql|mariadb):[replication:|loadbalance:|sequential:|aurora:]//<hostDescription>[,<hostDescription>...]/[database][?<key1>=<value1>[&<key2>=<value2>]])",
                        "08000");
            }
        }

        // There could be several addresses given in the URL spec, try all of them, and throw exception
        // if all hosts
        // fail.
        while (!hosts.isEmpty()) {
            currentHost = hosts.poll();
            try {
                createConnection(currentHost, username);
                return;
            } catch (SQLException e) {
                if (hosts.isEmpty()) {
                    if (e.getSQLState() != null) {
                        throw ExceptionFactory.INSTANCE.create(
                                String.format(
                                        "Could not connect to %s : %s%s",
                                        HostAddress.toString(hostAddresses), e.getMessage(), getTraces()),
                                e.getSQLState(),
                                e.getErrorCode(),
                                e);
                    }
                    throw ExceptionFactory.INSTANCE.create(
                            String.format(
                                    "Could not connect to %s. %s%s", currentHost, e.getMessage(), getTraces()),
                            "08000",
                            e);
                }
            } catch (IOException e) {
                throw ExceptionFactory.INSTANCE.create(String.format("Could not connect to %s. %s%s", currentHost, e.getMessage(), getTraces()), "08000", e);
            }
        }
    }

    /**
     * Indicate for Old reconnection if can reconnect without throwing exception.
     *
     * @return true if can reconnect without issue
     */
    public boolean shouldReconnectWithoutProxy() {
        return (((serverStatus & ServerStatus.IN_TRANSACTION) == 0) && hostFailed && urlParser
            .getOptions().autoReconnect);
    }

    public String getServerVersion() {
        return serverVersion;
    }

    public void setObServerVersion(String version) {
        this.obServerVersion = version;
        if (isOracleMode && options.useServerPrepStmts) {
            if (options.useOraclePrepareExecute && Version.compare(obServerVersion, "2.2.76") >= 0) {
                supportStmtPrepareExecute = true;
            }
            if (options.useCursorOffset && Version.compare(obServerVersion, "2.2.5") >= 0) {
                supportFetchWithOffset = true;
            }
        }
    }

    public String getObServerVersion() {
        return obServerVersion;
    }

    public boolean supportStmtPrepareExecute() {
        return supportStmtPrepareExecute;
    }

    public boolean supportFetchWithOffset() {
        return supportFetchWithOffset;
    }

    public boolean useOceanBaseProtocolV20() {
        return useOceanBaseProtocolV20;
    }

    public void setFullLinkTraceModule(String module, String action) {
        if (!enableFullLinkTrace) {
            throw new UnsupportedOperationException("FullLinkTrace is not enabled.");
        }
        fullLinkTrace.traceInfo.appInfo.setModule(module, action);
    }

    public String getFullLinkTraceModule() {
        if (!enableFullLinkTrace) {
            throw new UnsupportedOperationException("FullLinkTrace is not enabled.");
        }
        return fullLinkTrace.traceInfo.appInfo.getModule();
    }

    public void setFullLinkTraceAction(String action) {
        if (!enableFullLinkTrace) {
            throw new UnsupportedOperationException("FullLinkTrace is not enabled.");
        }
        fullLinkTrace.traceInfo.appInfo.setAction(action);
    }

    public String getFullLinkTraceAction() {
        if (!enableFullLinkTrace) {
            throw new UnsupportedOperationException("FullLinkTrace is not enabled.");
        }
        return fullLinkTrace.traceInfo.appInfo.getAction();
    }

    public void setFullLinkTraceClientInfo(String clientInfo) {
        if (!enableFullLinkTrace) {
            throw new UnsupportedOperationException("FullLinkTrace is not enabled.");
        }
        fullLinkTrace.traceInfo.appInfo.setClientInfo(clientInfo);
    }

    public String getFullLinkTraceClientInfo() {
        if (!enableFullLinkTrace) {
            throw new UnsupportedOperationException("FullLinkTrace is not enabled.");
        }
        return fullLinkTrace.traceInfo.appInfo.getClientInfo();
    }

    public void setFullLinkTraceIdentifier(String clientIdentifier) {
        if (!enableFullLinkTrace) {
            throw new UnsupportedOperationException("FullLinkTrace is not enabled.");
        }
        fullLinkTrace.traceInfo.appInfo.setClientIdentifier(clientIdentifier);
    }

    public String getFullLinkTraceIdentifier() {
        if (!enableFullLinkTrace) {
            throw new UnsupportedOperationException("FullLinkTrace is not enabled.");
        }
        return fullLinkTrace.traceInfo.appInfo.getClientIdentifier();
    }

    // todo: remove below
    public byte getFullLinkTraceLevel() {
        if (!enableFullLinkTrace) {
            throw new UnsupportedOperationException("FullLinkTrace is not enabled.");
        }
        return fullLinkTrace.traceInfo.controlInfo.getLevel();
    }

    public double getFullLinkTraceSamplePercentage() {
        if (!enableFullLinkTrace) {
            throw new UnsupportedOperationException("FullLinkTrace is not enabled.");
        }
        return fullLinkTrace.traceInfo.controlInfo.getSamplePercentage();
    }

    public byte getFullLinkTraceRecordPolicy() {
        if (!enableFullLinkTrace) {
            throw new UnsupportedOperationException("FullLinkTrace is not enabled.");
        }
        return fullLinkTrace.traceInfo.controlInfo.getRecordPolicy();
    }

    public double getFullLinkTracePrintSamplePercentage() {
        if (!enableFullLinkTrace) {
            throw new UnsupportedOperationException("FullLinkTrace is not enabled.");
        }
        return fullLinkTrace.traceInfo.controlInfo.getPrintSamplePercentage();
    }

    public long getFullLinkTraceSlowQueryThreshold() {
        if (!enableFullLinkTrace) {
            throw new UnsupportedOperationException("FullLinkTrace is not enabled.");
        }
        return fullLinkTrace.traceInfo.controlInfo.getSlowQueryThreshold();
    }

    // todo: remove above

    public boolean getReadonly() {
        return readOnly;
    }

    public HostAddress getHostAddress() {
        return currentHost;
    }

    public void setHostAddress(HostAddress host) {
        this.currentHost = host;
        this.readOnly = ParameterConstant.TYPE_SLAVE.equals(this.currentHost.type);
    }

    public String getHost() {
        return (currentHost == null) ? null : currentHost.host;
    }

    public FailoverProxy getProxy() {
        return proxy;
    }

    public void setProxy(FailoverProxy proxy) {
        this.proxy = proxy;
    }

    public int getPort() {
        return (currentHost == null) ? 3306 : currentHost.port;
    }

    public String getDatabase() {
        return database;
    }

    public String getUsername() {
        return username;
    }

    public void parseVersion(String serverVersion) {
        int length = serverVersion.length();
        char car;
        int offset = 0;
        int type = 0;
        int val = 0;
        for (; offset < length; offset++) {
            car = serverVersion.charAt(offset);
            if (car < '0' || car > '9') {
                switch (type) {
                    case 0:
                        majorVersion = val;
                        break;
                    case 1:
                        minorVersion = val;
                        break;
                    case 2:
                        patchVersion = val;
                        return;
                    default:
                        break;
                }
                type++;
                val = 0;
            } else {
                val = val * 10 + car - 48;
            }
        }

        // serverVersion finished by number like "5.5.57", assign patchVersion
        if (type == 2) {
            patchVersion = val;
        }
    }

    public int getMajorServerVersion() {
        return majorVersion;
    }

    public int getMinorServerVersion() {
        return minorVersion;
    }

    /**
     * Utility method to check if database version is greater than parameters.
     *
     * @param major major version
     * @param minor minor version
     * @param patch patch version
     * @return true if version is greater than parameters
     */
    public boolean versionGreaterOrEqual(int major, int minor, int patch) {
        if (this.majorVersion > major) {
            return true;
        }

        if (this.majorVersion < major) {
            return false;
        }

        /*
         * Major versions are equal, compare minor versions
         */
        if (this.minorVersion > minor) {
            return true;
        }
        if (this.minorVersion < minor) {
            return false;
        }

        // Minor versions are equal, compare patch version.
        return this.patchVersion >= patch;
    }

    public boolean getPinGlobalTxToPhysicalConnection() {
        return this.options.pinGlobalTxToPhysicalConnection;
    }

    /**
     * Has warnings.
     *
     * @return true if as warnings.
     */
    public boolean hasWarnings() {
        lock.lock();
        try {
            return hasWarnings;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Is connected.
     *
     * @return true if connected
     */
    public boolean isConnected() {
        lock.lock();
        try {
            return connected;
        } finally {
            lock.unlock();
        }
    }

    public long getServerThreadId() {
        return serverThreadId;
    }

    @Override
    public Socket getSocket() {
        return socket;
    }

    public boolean isExplicitClosed() {
        return explicitClosed;
    }

    public TimeZone getTimeZone() {
        return timeZone;
    }

    public TimeZone getServerTimeZone() {
        return serverTimeZone;
    }

    public Options getOptions() {
        return options;
    }

    public void setHasWarnings(boolean hasWarnings) {
        this.hasWarnings = hasWarnings;
    }

    public Results getActiveStreamingResult() {
        return activeStreamingResult;
    }

    public void setActiveStreamingResult(Results activeStreamingResult) {
        this.activeStreamingResult = activeStreamingResult;
    }

    /** Remove exception result and since totally fetched, set fetch size to 0. */
    public void removeActiveStreamingResult() {
        if (this.activeStreamingResult != null) {
            this.activeStreamingResult.removeFetchSize();
            this.activeStreamingResult = null;
        }
    }

    @Override
    public ReentrantLock getLock() {
        return lock;
    }

    @Override
    public boolean hasMoreResults() {
        return (serverStatus & ServerStatus.MORE_RESULTS_EXISTS) != 0;
    }

    public ServerPrepareStatementCache prepareStatementCache() {
        return serverPrepareStatementCache;
    }

    public abstract void executeQuery(final String sql) throws SQLException;

    /**
     * Change Socket TcpNoDelay option.
     *
     * @param setTcpNoDelay value to set.
     */
    public void changeSocketTcpNoDelay(boolean setTcpNoDelay) {
        try {
            socket.setTcpNoDelay(setTcpNoDelay);
        } catch (SocketException socketException) {
            // eat exception
        }
    }

    public void changeSocketSoTimeout(int setSoTimeout) throws SocketException {
        this.socketTimeout = setSoTimeout;
        socket.setSoTimeout(this.socketTimeout);
    }

    public boolean isServerMariaDb() {
        return serverMariaDb;
    }

    public PacketInputStream getReader() {
        return reader;
    }

    public boolean isEofDeprecated() {
        return eofDeprecated;
    }

    public boolean sessionStateAware() {
        return (serverCapabilities & OceanBaseCapabilityFlag.CLIENT_SESSION_TRACK) != 0;
    }

    /**
     * Get a String containing readable information about last 10 send/received packets.
     *
     * @return String value
     */
    public String getTraces() {
        if (options.enablePacketDebug) {
            return traceCache.printStack();
        }
        return "";
    }

    @Override
    public boolean isOracleMode() {
        return isOracleMode;
    }

    public void setOracleMode(boolean oracleMode) {
        isOracleMode = oracleMode;
    }

    @Override
    public boolean isTZTablesImported() {
        return this.importedTimeZoneTables;
    }

    @Override
    public String getEncoding() {
        return this.options.characterEncoding;
    }

    public PacketOutputStream getWriter() {
        return writer;
    }

    public HostAddress getCurrentHost() {
        return currentHost;
    }

    public void startCallInterface() {
        if (timeTrace != null) {
            timeTrace.startCallInterface();
        }
    }

    public void endCallInterface(String message) {
        if (timeTrace != null) {
            timeTrace.endCallInterface(message);
        }
    }

    public TimeTrace getTimeTrace() {
        return timeTrace;
    }

    protected void whyConnectedIsFalse() {
        if (options.log) {
            StringBuilder sb = new StringBuilder();
            StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
            boolean first = true;
            for (StackTraceElement stackTraceElement : stackTrace) {
                if (first) {
                    first = false;
                    continue;
                }

                String str = stackTraceElement.toString();
                sb.append("    at ").append(str).append('\n');
            }
            logger.debug("set protocol.connected to false\n" + sb);
        }
    }

}
