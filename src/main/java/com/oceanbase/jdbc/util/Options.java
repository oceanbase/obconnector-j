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
package com.oceanbase.jdbc.util;

import java.lang.reflect.Field;
import java.sql.DriverManager;
import java.util.Objects;
import java.util.Properties;

@SuppressWarnings("ConstantConditions")
public class Options implements Cloneable {

    public static final int MIN_VALUE__MAX_IDLE_TIME = 60;

    public static final String                           ZERO_DATETIME_CONVERT_TO_NULL = "convertToNull";

    public static final String                           ZERO_DATETIME_EXCEPTION       = "exception";

    public static final String                           ZERO_DATETIME_ROUND           = "round";

    // standard options
    public String user;
    public String password;

    // divers
    public boolean trustServerCertificate;
    public String serverSslCert;
    public String trustStore;
    public String trustStoreType;
    public String keyStoreType;
    public String trustStorePassword;
    public String keyStore;
    public String keyStorePassword;
    public String keyPassword;
    public String enabledSslProtocolSuites;
    public boolean useFractionalSeconds = true;
    public boolean pinGlobalTxToPhysicalConnection;
    public String socketFactory;

    public int connectTimeout =
            DriverManager.getLoginTimeout() > 0 ? DriverManager.getLoginTimeout() * 1000 : 30_000;
    public String pipe;
    public String localSocket;
    public String sharedMemory;
    public String obProxySocket;
    public boolean tcpNoDelay = true;
    public boolean tcpKeepAlive = true;
    public Integer tcpRcvBuf;
    public Integer tcpSndBuf;
    public boolean tcpAbortiveClose;
    public String localSocketAddress;
    public Integer socketTimeout;
    public boolean allowMultiQueries;
    public boolean trackSchema = true;
    public boolean rewriteBatchedStatements;
    public boolean useCompression;
    public boolean interactiveClient;
    public String passwordCharacterEncoding;
    public boolean blankTableNameMeta;
    public String credentialType;
    public Boolean useSsl = null;
    public String enabledSslCipherSuites;
    public String sessionVariables;
    public boolean tinyInt1isBit = true;
    public boolean transformedBitIsBoolean = false;
    public boolean yearIsDateType = true;
    public boolean createDatabaseIfNotExist;
    public String serverTimezone;
    public boolean nullCatalogMeansCurrent = true;
    public boolean dumpQueriesOnException;
    public boolean useOldAliasMetadataBehavior;
    public boolean useMysqlMetadata;
    public boolean allowLocalInfile = false;
    public boolean cachePrepStmts = false;
    public int prepStmtCacheSize = 250;
    public int prepStmtCacheSqlLimit = 2048;
    public boolean useLegacyDatetimeCode = false;
    public boolean useAffectedRows;
    public boolean maximizeMysqlCompatibility;
    public boolean useServerPrepStmts;
    public boolean emulateUnsupportedPstmts = false;
    public boolean continueBatchOnError = true;
    public boolean jdbcCompliantTruncation = true;
    public boolean cacheCallableStmts = true;
    public int callableStmtCacheSize = 150;
    public String connectionAttributes;
    public Boolean useBatchMultiSend;
    public int useBatchMultiSendNumber = 100;
    public Boolean usePipelineAuth;
    public boolean enablePacketDebug;
    public boolean useBulkStmts;
    public boolean disableSslHostnameVerification;
    public boolean autocommit = true;
    public boolean includeInnodbStatusInDeadlockExceptions;
    public boolean includeThreadDumpInDeadlockExceptions;
    public String servicePrincipalName;
    public int defaultFetchSize;
    public Properties nonMappedOptions = new Properties();
    public String tlsSocketType;
    public boolean clobberStreamingResults;
    public int maxRows;
    public boolean useInformationSchema;

    // logging options
    public boolean log;
    public boolean profileSql;
    public int maxQuerySizeToLog = 1024;
    public Long slowQueryThresholdNanos;

    // HA options
    public boolean assureReadOnly = true;
    public boolean autoReconnect;
    public boolean failOnReadOnly;
    public int retriesAllDown = 120;
    public int validConnectionTimeout;
    public int loadBalanceBlacklistTimeout = 50;
    public int failoverLoopRetries = 120;
    public boolean allowMasterDownConnection;
    public String galeraAllowedState;

    // Pool options
    public boolean pool;
    public String poolName;
    public int maxPoolSize = 8;
    public Integer minPoolSize;
    public int maxIdleTime = 600;
    public boolean staticGlobal;
    public boolean registerJmxPool = true;
    public int poolValidMinDelay = 1000;
    public boolean useResetConnection;
    public boolean useReadAheadInput = true;

    // MySQL sha authentication
    public String serverRsaPublicKeyFile;
    public boolean allowPublicKeyRetrieval;

    public String characterEncoding = "utf8";
    public boolean useCursorFetch = false;
    public String socksProxyHost;
    public int socksProxyPort = 1080;
    public boolean autoDeserialize = false;
    public boolean enableQueryTimeouts = true;
    public String  loadBalanceStrategy = "random";
    public int     blobSendChunkSize = 1048576;
    public String  connectionCollation = null;
    public boolean allowNanAndInf = false;
    public String  zeroDateTimeBehavior = ZERO_DATETIME_EXCEPTION;

    // OceanBase extended options
    public boolean supportLobLocator = true;
    public boolean useObChecksum = true;
    public boolean useFormatExceptionMessage = false;
    public boolean allowSendParamTypes = false;
    public int complexDataCacheSize = 50;
    public boolean cacheComplexData = true;
    public boolean useSqlStringCache = false;
    public boolean useServerPsStmtChecksum = true;
    public boolean supportNameBinding = true;
    public boolean connectProxy = false;
    public boolean usePieceData = false;
    public int  pieceLength = 1048576;
    public boolean useOraclePrepareExecute = false;
    public int     maxBatchTotalParamsNum = 30000;
    public boolean useCursorOffset = false;
    public String  tnsnamesPath;
    public int     tnsnamesDetectionPeriod = 10;
    public String  serverAffinityOrder;
    public boolean useArrayBinding   = false;
    public boolean sendConnectionAttributes   = true;
    public boolean rewriteInsertByMultiQueries = false;
    public boolean useLocalXID = true;
    public boolean useOceanBaseProtocolV20 = true;
    public boolean enableFullLinkTrace = false;
    public String  defaultConnectionAttributesBanList = null;
    public boolean enableOb20Checksum = true;
    public int     ocpAccessInterval = 5;
    public int     httpConnectTimeout;
    public int     httpReadTimeout;
    public boolean loadBalanceHandleFailover = true;
    public int     compatibleOjdbcVersion = 6;
    public int     compatibleMysqlVersion = 5;

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        String newLine = System.getProperty("line.separator");
        result.append(this.getClass().getName());
        result.append(" Options {");
        result.append(newLine);

        Field[] fields = this.getClass().getDeclaredFields();
        for (Field field : fields) {
            result.append("  ");
            try {
                result.append(field.getName());
                result.append(": ");
                // requires access to private field:
                result.append(field.get(this));
            } catch (IllegalAccessException ex) {
                // ignore error
            }
            result.append(newLine);
        }
        result.append("}");
        return result.toString();
    }

    @SuppressWarnings("SimplifiableIfStatement")
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        Options opt = (Options) obj;

        if (trustServerCertificate != opt.trustServerCertificate) {
            return false;
        }
        if (useFractionalSeconds != opt.useFractionalSeconds) {
            return false;
        }
        if (pinGlobalTxToPhysicalConnection != opt.pinGlobalTxToPhysicalConnection) {
            return false;
        }
        if (tcpNoDelay != opt.tcpNoDelay) {
            return false;
        }
        if (tcpKeepAlive != opt.tcpKeepAlive) {
            return false;
        }
        if (tcpAbortiveClose != opt.tcpAbortiveClose) {
            return false;
        }
        if (blankTableNameMeta != opt.blankTableNameMeta) {
            return false;
        }
        if (allowMultiQueries != opt.allowMultiQueries) {
            return false;
        }
        if (rewriteBatchedStatements != opt.rewriteBatchedStatements) {
            return false;
        }
        if (useCompression != opt.useCompression) {
            return false;
        }
        if (interactiveClient != opt.interactiveClient) {
            return false;
        }
        if (useSsl != opt.useSsl) {
            return false;
        }
        if (tinyInt1isBit != opt.tinyInt1isBit) {
            return false;
        }
      if (transformedBitIsBoolean != opt.transformedBitIsBoolean) {
        return false;
      }
        if (yearIsDateType != opt.yearIsDateType) {
            return false;
        }
        if (createDatabaseIfNotExist != opt.createDatabaseIfNotExist) {
            return false;
        }
        if (nullCatalogMeansCurrent != opt.nullCatalogMeansCurrent) {
            return false;
        }
        if (dumpQueriesOnException != opt.dumpQueriesOnException) {
            return false;
        }
        if (useOldAliasMetadataBehavior != opt.useOldAliasMetadataBehavior) {
            return false;
        }
        if (allowLocalInfile != opt.allowLocalInfile) {
            return false;
        }
        if (cachePrepStmts != opt.cachePrepStmts) {
            return false;
        }
        if (useLegacyDatetimeCode != opt.useLegacyDatetimeCode) {
            return false;
        }
        if (useAffectedRows != opt.useAffectedRows) {
            return false;
        }
        if (maximizeMysqlCompatibility != opt.maximizeMysqlCompatibility) {
            return false;
        }
        if (useServerPrepStmts != opt.useServerPrepStmts) {
            return false;
        }
        if (continueBatchOnError != opt.continueBatchOnError) {
            return false;
        }
        if (jdbcCompliantTruncation != opt.jdbcCompliantTruncation) {
            return false;
        }
        if (cacheCallableStmts != opt.cacheCallableStmts) {
            return false;
        }
        if (useBatchMultiSendNumber != opt.useBatchMultiSendNumber) {
            return false;
        }
        if (enablePacketDebug != opt.enablePacketDebug) {
            return false;
        }
        if (includeInnodbStatusInDeadlockExceptions != opt.includeInnodbStatusInDeadlockExceptions) {
            return false;
        }
        if (includeThreadDumpInDeadlockExceptions != opt.includeThreadDumpInDeadlockExceptions) {
            return false;
        }
        if (defaultFetchSize != opt.defaultFetchSize) {
            return false;
        }
        if (useBulkStmts != opt.useBulkStmts) {
            return false;
        }
        if (disableSslHostnameVerification != opt.disableSslHostnameVerification) {
            return false;
        }
        if (log != opt.log) {
            return false;
        }
        if (profileSql != opt.profileSql) {
            return false;
        }
        if (assureReadOnly != opt.assureReadOnly) {
            return false;
        }
        if (autoReconnect != opt.autoReconnect) {
            return false;
        }
        if (failOnReadOnly != opt.failOnReadOnly) {
            return false;
        }
        if (allowMasterDownConnection != opt.allowMasterDownConnection) {
            return false;
        }
        if (retriesAllDown != opt.retriesAllDown) {
            return false;
        }
        if (validConnectionTimeout != opt.validConnectionTimeout) {
            return false;
        }
        if (loadBalanceBlacklistTimeout != opt.loadBalanceBlacklistTimeout) {
            return false;
        }
        if (failoverLoopRetries != opt.failoverLoopRetries) {
            return false;
        }
        if (pool != opt.pool) {
            return false;
        }
        if (staticGlobal != opt.staticGlobal) {
            return false;
        }
        if (registerJmxPool != opt.registerJmxPool) {
            return false;
        }
        if (useResetConnection != opt.useResetConnection) {
            return false;
        }
        if (useReadAheadInput != opt.useReadAheadInput) {
            return false;
        }
        if (maxPoolSize != opt.maxPoolSize) {
            return false;
        }
        if (maxIdleTime != opt.maxIdleTime) {
            return false;
        }
        if (poolValidMinDelay != opt.poolValidMinDelay) {
            return false;
        }
        if (!Objects.equals(user, opt.user)) {
            return false;
        }
        if (!Objects.equals(password, opt.password)) {
            return false;
        }
        if (!Objects.equals(serverSslCert, opt.serverSslCert)) {
            return false;
        }
        if (!Objects.equals(trustStore, opt.trustStore)) {
            return false;
        }
        if (!Objects.equals(trustStorePassword, opt.trustStorePassword)) {
            return false;
        }
        if (!Objects.equals(keyStore, opt.keyStore)) {
            return false;
        }
        if (!Objects.equals(keyStorePassword, opt.keyStorePassword)) {
            return false;
        }
        if (!Objects.equals(keyPassword, opt.keyPassword)) {
            return false;
        }
        if (enabledSslProtocolSuites != null) {
            if (!enabledSslProtocolSuites.equals(opt.enabledSslProtocolSuites)) {
                return false;
            }
        } else if (opt.enabledSslProtocolSuites != null) {
            return false;
        }
        if (!Objects.equals(socketFactory, opt.socketFactory)) {
            return false;
        }
        if (connectTimeout != opt.connectTimeout) {
            return false;
        }
        if (!Objects.equals(pipe, opt.pipe)) {
            return false;
        }
        if (!Objects.equals(localSocket, opt.localSocket)) {
            return false;
        }
        if (!Objects.equals(sharedMemory, opt.sharedMemory)) {
            return false;
        }
        if (!Objects.equals(obProxySocket, opt.obProxySocket)) {
            return false;
        }
        if (!Objects.equals(tcpRcvBuf, opt.tcpRcvBuf)) {
            return false;
        }
        if (!Objects.equals(tcpSndBuf, opt.tcpSndBuf)) {
            return false;
        }
        if (!Objects.equals(localSocketAddress, opt.localSocketAddress)) {
            return false;
        }
        if (!Objects.equals(socketTimeout, opt.socketTimeout)) {
            return false;
        }
        if (passwordCharacterEncoding != null) {
            if (!passwordCharacterEncoding.equals(opt.passwordCharacterEncoding)) {
                return false;
            }
        } else if (opt.passwordCharacterEncoding != null) {
            return false;
        }

        if (!Objects.equals(enabledSslCipherSuites, opt.enabledSslCipherSuites)) {
            return false;
        }
        if (!Objects.equals(sessionVariables, opt.sessionVariables)) {
            return false;
        }
        if (!Objects.equals(serverTimezone, opt.serverTimezone)) {
            return false;
        }
        if (prepStmtCacheSize != opt.prepStmtCacheSize) {
            return false;
        }
        if (prepStmtCacheSqlLimit != opt.prepStmtCacheSqlLimit) {
            return false;
        }
        if (callableStmtCacheSize != opt.callableStmtCacheSize) {
            return false;
        }
        if (!Objects.equals(connectionAttributes, opt.connectionAttributes)) {
            return false;
        }
        if (!Objects.equals(useBatchMultiSend, opt.useBatchMultiSend)) {
            return false;
        }
        if (!Objects.equals(usePipelineAuth, opt.usePipelineAuth)) {
            return false;
        }
        if (maxQuerySizeToLog != opt.maxQuerySizeToLog) {
            return false;
        }
        if (!Objects.equals(slowQueryThresholdNanos, opt.slowQueryThresholdNanos)) {
            return false;
        }
        if (autocommit != opt.autocommit) {
            return false;
        }
        if (!Objects.equals(poolName, opt.poolName)) {
            return false;
        }
        if (!Objects.equals(galeraAllowedState, opt.galeraAllowedState)) {
            return false;
        }
        if (!Objects.equals(credentialType, opt.credentialType)) {
            return false;
        }
        if (!Objects.equals(nonMappedOptions, opt.nonMappedOptions)) {
            return false;
        }
        if (!Objects.equals(tlsSocketType, opt.tlsSocketType)) {
            return false;
        }
        if (supportLobLocator != opt.supportLobLocator) {
            return false;
        }
        if (useObChecksum != opt.useObChecksum) {
            return false;
        }
        if (useFormatExceptionMessage != opt.useFormatExceptionMessage) {
            return false;
        }
        if (allowSendParamTypes != opt.allowSendParamTypes) {
            return false;
        }
        if (complexDataCacheSize != opt.complexDataCacheSize) {
            return false;
        }
        if (cacheComplexData != opt.cacheComplexData) {
            return false;
        }
        if (useSqlStringCache != opt.useSqlStringCache) {
            return false;
        }
        if (useServerPsStmtChecksum != opt.useServerPsStmtChecksum) {
            return false;
        }
        if (characterEncoding != opt.characterEncoding) {
            return false;
        }
        if (useCursorFetch != opt.useCursorFetch) {
            return false;
        }
        if (supportNameBinding != opt.supportNameBinding) {
            return false;
        }
        if (!Objects.equals(socksProxyHost, opt.socksProxyHost)) {
            return false;
        }
        if (socksProxyPort != opt.socksProxyPort) {
            return false;
        }
        if (connectProxy != opt.connectProxy) {
            return false;
        }
        if (usePieceData != opt.usePieceData) {
            return false;
        }
        if (pieceLength != opt.pieceLength) {
            return false;
        }
        if (useOraclePrepareExecute != opt.useOraclePrepareExecute) {
          return false;
        }
        if(autoDeserialize != opt.autoDeserialize) {
          return false;
        }
        if(enableQueryTimeouts != opt.enableQueryTimeouts) {
        return false;
      }
        if (maxBatchTotalParamsNum != opt.maxBatchTotalParamsNum) {
          return false;
        }
        if (emulateUnsupportedPstmts != opt.emulateUnsupportedPstmts) {
            return false;
        }
        if (useCursorOffset != opt.useCursorOffset) {
            return false;
        }
        if(tnsnamesPath != opt.tnsnamesPath) {
          return false;
        }
        if(tnsnamesDetectionPeriod != opt.tnsnamesDetectionPeriod) {
          return false;
        }
        if(loadBalanceStrategy != opt.loadBalanceStrategy) {
          return false;
        }
        if(serverAffinityOrder != opt.serverAffinityOrder) {
          return false;
        }
        if (blobSendChunkSize != opt.blobSendChunkSize) {
          return false;
        }
        if (connectionCollation != opt.connectionCollation) {
          return false;
        }
        if (defaultConnectionAttributesBanList != opt.defaultConnectionAttributesBanList) {
            return false;
        }
        if (useArrayBinding != opt.useArrayBinding) {
          return false;
        }
        if (sendConnectionAttributes != opt.sendConnectionAttributes) {
            return false;
        }
        if (rewriteInsertByMultiQueries != opt.rewriteInsertByMultiQueries) {
            return false;
        }
        if (useLocalXID != opt.useLocalXID) {
            return false;
        }
        if (useOceanBaseProtocolV20 != opt.useOceanBaseProtocolV20) {
            return false;
        }
        if (enableFullLinkTrace != opt.enableFullLinkTrace) {
            return false;
        }
        if (clobberStreamingResults != opt.clobberStreamingResults) {
            return false;
        }
        if (maxRows != opt.maxRows) {
            return false;
        }
        if (allowNanAndInf != opt.allowNanAndInf) {
            return false;
        }
        if (zeroDateTimeBehavior != opt.zeroDateTimeBehavior) {
            return false;
        }
        if (enableOb20Checksum != opt.enableOb20Checksum) {
            return false;
        }
        if(ocpAccessInterval != opt.ocpAccessInterval) {
            return false;
        }
        if(httpConnectTimeout != opt.httpConnectTimeout) {
            return false;
        }
        if(httpReadTimeout != opt.httpReadTimeout) {
            return false;
        }
        if(loadBalanceHandleFailover != opt.loadBalanceHandleFailover) {
            return false;
        }
        if(compatibleOjdbcVersion != opt.compatibleOjdbcVersion) {
            return false;
        }
        if(compatibleMysqlVersion != opt.compatibleMysqlVersion) {
            return false;
        }
        if(useInformationSchema != opt.useInformationSchema) {
            return false;
        }

        return Objects.equals(minPoolSize, opt.minPoolSize);
    }

    @SuppressWarnings("SimplifiableIfStatement")
    @Override
    public int hashCode() {
        int result = user != null ? user.hashCode() : 0;
        result = 31 * result + (password != null ? password.hashCode() : 0);
        result = 31 * result + (trustServerCertificate ? 1 : 0);
        result = 31 * result + (serverSslCert != null ? serverSslCert.hashCode() : 0);
        result = 31 * result + (trustStore != null ? trustStore.hashCode() : 0);
        result = 31 * result + (trustStorePassword != null ? trustStorePassword.hashCode() : 0);
        result = 31 * result + (keyStore != null ? keyStore.hashCode() : 0);
        result = 31 * result + (keyStorePassword != null ? keyStorePassword.hashCode() : 0);
        result = 31 * result + (keyPassword != null ? keyPassword.hashCode() : 0);
        result =
                31 * result + (enabledSslProtocolSuites != null ? enabledSslProtocolSuites.hashCode() : 0);
        result = 31 * result + (useFractionalSeconds ? 1 : 0);
        result = 31 * result + (pinGlobalTxToPhysicalConnection ? 1 : 0);
        result = 31 * result + (socketFactory != null ? socketFactory.hashCode() : 0);
        result = 31 * result + connectTimeout;
        result = 31 * result + (pipe != null ? pipe.hashCode() : 0);
        result = 31 * result + (localSocket != null ? localSocket.hashCode() : 0);
        result = 31 * result + (sharedMemory != null ? sharedMemory.hashCode() : 0);
        result = 31 * result + (obProxySocket != null ? obProxySocket.hashCode() : 0);
        result = 31 * result + (tcpNoDelay ? 1 : 0);
        result = 31 * result + (tcpKeepAlive ? 1 : 0);
        result = 31 * result + (tcpRcvBuf != null ? tcpRcvBuf.hashCode() : 0);
        result = 31 * result + (tcpSndBuf != null ? tcpSndBuf.hashCode() : 0);
        result = 31 * result + (tcpAbortiveClose ? 1 : 0);
        result = 31 * result + (localSocketAddress != null ? localSocketAddress.hashCode() : 0);
        result = 31 * result + (socketTimeout != null ? socketTimeout.hashCode() : 0);
        result = 31 * result + (allowMultiQueries ? 1 : 0);
        result = 31 * result + (rewriteBatchedStatements ? 1 : 0);
        result = 31 * result + (useCompression ? 1 : 0);
        result = 31 * result + (interactiveClient ? 1 : 0);
        result =
                31 * result
                        + (passwordCharacterEncoding != null ? passwordCharacterEncoding.hashCode() : 0);
        result = 31 * result + (useSsl ? 1 : 0);
        result = 31 * result + (enabledSslCipherSuites != null ? enabledSslCipherSuites.hashCode() : 0);
        result = 31 * result + (sessionVariables != null ? sessionVariables.hashCode() : 0);
        result = 31 * result + (tinyInt1isBit ? 1 : 0);
        result = 31 * result + (transformedBitIsBoolean ? 1 : 0);
        result = 31 * result + (yearIsDateType ? 1 : 0);
        result = 31 * result + (createDatabaseIfNotExist ? 1 : 0);
        result = 31 * result + (serverTimezone != null ? serverTimezone.hashCode() : 0);
        result = 31 * result + (nullCatalogMeansCurrent ? 1 : 0);
        result = 31 * result + (dumpQueriesOnException ? 1 : 0);
        result = 31 * result + (useOldAliasMetadataBehavior ? 1 : 0);
        result = 31 * result + (allowLocalInfile ? 1 : 0);
        result = 31 * result + (cachePrepStmts ? 1 : 0);
        result = 31 * result + prepStmtCacheSize;
        result = 31 * result + prepStmtCacheSqlLimit;
        result = 31 * result + (useLegacyDatetimeCode ? 1 : 0);
        result = 31 * result + (useAffectedRows ? 1 : 0);
        result = 31 * result + (maximizeMysqlCompatibility ? 1 : 0);
        result = 31 * result + (useServerPrepStmts ? 1 : 0);
        result = 31 * result + (continueBatchOnError ? 1 : 0);
        result = 31 * result + (jdbcCompliantTruncation ? 1 : 0);
        result = 31 * result + (cacheCallableStmts ? 1 : 0);
        result = 31 * result + callableStmtCacheSize;
        result = 31 * result + (connectionAttributes != null ? connectionAttributes.hashCode() : 0);
        result = 31 * result + (useBatchMultiSend != null ? useBatchMultiSend.hashCode() : 0);
        result = 31 * result + useBatchMultiSendNumber;
        result = 31 * result + (usePipelineAuth != null ? usePipelineAuth.hashCode() : 0);
        result = 31 * result + (enablePacketDebug ? 1 : 0);
        result = 31 * result + (includeInnodbStatusInDeadlockExceptions ? 1 : 0);
        result = 31 * result + (includeThreadDumpInDeadlockExceptions ? 1 : 0);
        result = 31 * result + (useBulkStmts ? 1 : 0);
        result = 31 * result + defaultFetchSize;
        result = 31 * result + (disableSslHostnameVerification ? 1 : 0);
        result = 31 * result + (log ? 1 : 0);
        result = 31 * result + (profileSql ? 1 : 0);
        result = 31 * result + maxQuerySizeToLog;
        result =
                31 * result + (slowQueryThresholdNanos != null ? slowQueryThresholdNanos.hashCode() : 0);
        result = 31 * result + (assureReadOnly ? 1 : 0);
        result = 31 * result + (autoReconnect ? 1 : 0);
        result = 31 * result + (failOnReadOnly ? 1 : 0);
        result = 31 * result + (allowMasterDownConnection ? 1 : 0);
        result = 31 * result + retriesAllDown;
        result = 31 * result + validConnectionTimeout;
        result = 31 * result + loadBalanceBlacklistTimeout;
        result = 31 * result + failoverLoopRetries;
        result = 31 * result + (pool ? 1 : 0);
        result = 31 * result + (registerJmxPool ? 1 : 0);
        result = 31 * result + (useResetConnection ? 1 : 0);
        result = 31 * result + (useReadAheadInput ? 1 : 0);
        result = 31 * result + (staticGlobal ? 1 : 0);
        result = 31 * result + (poolName != null ? poolName.hashCode() : 0);
        result = 31 * result + (galeraAllowedState != null ? galeraAllowedState.hashCode() : 0);
        result = 31 * result + maxPoolSize;
        result = 31 * result + (minPoolSize != null ? minPoolSize.hashCode() : 0);
        result = 31 * result + maxIdleTime;
        result = 31 * result + poolValidMinDelay;
        result = 31 * result + (autocommit ? 1 : 0);
        result = 31 * result + (credentialType != null ? credentialType.hashCode() : 0);
        result = 31 * result + (nonMappedOptions != null ? nonMappedOptions.hashCode() : 0);
        result = 31 * result + (tlsSocketType != null ? tlsSocketType.hashCode() : 0);
        // Add Oceanbase extended options
        result = 31 * result + (supportLobLocator ? 1 : 0);
        result = 31 * result + (useObChecksum ? 1 : 0);
        result = 31 * result + (useFormatExceptionMessage ? 1 : 0);
        result = 31 * result + (allowSendParamTypes ? 1 : 0);
        result = 31 * result + complexDataCacheSize;
        result = 31 * result + (cacheComplexData ? 1 : 0);
        result = 31 * result + (useSqlStringCache ? 1 : 0);
        result = 31 * result + (useServerPsStmtChecksum ? 1 : 0);
        result = 31 * result + (characterEncoding != null ? characterEncoding.hashCode() : 0);
        result = 31 * result + (useCursorFetch ? 1 : 0);
        result = 31 * result + (supportNameBinding ? 1 : 0);
        result = 31 * result + (socksProxyHost != null ? socksProxyHost.hashCode() : 0);
        result = 31 * result + socksProxyPort;
        result = 31 * result + (connectProxy ? 1 : 0);
        result = 31 * result + (usePieceData ? 1 : 0);
        result = 31 * result + pieceLength;
        result = 31 * result + (useOraclePrepareExecute ? 1 : 0);
        result = 31 * result + (autoDeserialize ? 1 : 0);
        result = 31 * result + (enableQueryTimeouts ? 1 : 0);
        result = 31 * result + (maxBatchTotalParamsNum);
        result = 31 * result + (emulateUnsupportedPstmts ? 1 : 0);
        result = 31 * result + (useCursorOffset ? 1 : 0);
        result = 31 * result + (tnsnamesPath != null ? tnsnamesPath.hashCode() : 0);
        result = 31 * result + tnsnamesDetectionPeriod;
        result = 31 * result + (loadBalanceStrategy != null ? loadBalanceStrategy.hashCode() : 0);
        result = 31 * result + (serverAffinityOrder != null ? serverAffinityOrder.hashCode() : 0);
        result = 31 * result + (blobSendChunkSize);
        result = 31 * result + (connectionCollation != null ? connectionCollation.hashCode() : 0);
        result = 31 * result + (defaultConnectionAttributesBanList != null ? defaultConnectionAttributesBanList.hashCode() : 0);
        result = 31 * result + (useArrayBinding ? 1 : 0);
        result = 31 * result + (sendConnectionAttributes ? 1 : 0);
        result = 31 * result + (rewriteInsertByMultiQueries ? 1 : 0);
        result = 31 * result + (useLocalXID ? 1 : 0);
        result = 31 * result + (useOceanBaseProtocolV20 ? 1 : 0);
        result = 31 * result + (enableFullLinkTrace ? 1 : 0);
        result = 31 * result + (clobberStreamingResults ? 1 : 0);
        result = 31 * result + maxRows;
        result = 31 * result + (allowNanAndInf ? 1 : 0);
        result = 31 * result + (zeroDateTimeBehavior != null ? zeroDateTimeBehavior.hashCode() : 0);
        result = 31 * result + (enableOb20Checksum ? 1 : 0);
        result = 31 * result + ocpAccessInterval;
        result = 31 * result + httpConnectTimeout;
        result = 31 * result + httpReadTimeout;
        result = 31 * result + (loadBalanceHandleFailover ? 1 : 0);
        result = 31 * result + compatibleOjdbcVersion;
        result = 31 * result + compatibleMysqlVersion;
        result = 31 * result + (useInformationSchema ? 1 : 0);
        return result;
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }
}
