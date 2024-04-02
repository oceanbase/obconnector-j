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
package com.oceanbase.jdbc.util;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Properties;

import com.oceanbase.jdbc.credential.CredentialPlugin;
import com.oceanbase.jdbc.internal.util.OptionUtils;
import com.oceanbase.jdbc.internal.util.constant.HaMode;

public enum DefaultOptions {
    USER("user", "2.0.1", "Database user name", false),
    PASSWORD("password", "2.0.1", "Password of database user", false),

    /**
     * The connect timeout value, in milliseconds, or zero for no timeout. Default: 30000 (30 seconds)
     * (was 0 before 2.1.2)
     */
    CONNECT_TIMEOUT(
            "connectTimeout",
            30_000,
            0,
            "2.0.1",
            "The connect timeout value, in milliseconds, or zero for no timeout.",
            false),
    PIPE(
            "pipe",
            "2.0.1",
            "On Windows, specify named pipe name to connect.",
            false),
    LOCAL_SOCKET(
            "localSocket",
            "2.0.1",
            "Permits connecting to the database via Unix domain socket, if the server allows it."
                    + " \nThe value is the path of Unix domain socket (i.e \"socket\" database parameter : "
                    + "select @@socket).",
            false),
    SHARED_MEMORY(
            "sharedMemory",
            "2.0.1",
            "Permits connecting to the database via shared memory, if the server allows "
                    + "it. \nThe value is the base name of the shared memory.",
            false),
    OBPROXY_SOCKET(
            "obProxySocket",
            "2.3.0",
            "Permits connecting to the database via ObProxy ClientVCSocket, It need be used with libobproxy_so.so."
                    + " \nThe value is the config url for lanuch a local OBProxy with dynamic libraray",
            false),
    TCP_NO_DELAY(
            "tcpNoDelay",
            Boolean.TRUE,
            "2.0.1",
            "Sets corresponding option on the connection socket.",
            false),
    TCP_ABORTIVE_CLOSE(
            "tcpAbortiveClose",
            Boolean.FALSE,
            "2.0.1",
            "Sets corresponding option on the connection " + "socket.",
            false),
    LOCAL_SOCKET_ADDRESS(
            "localSocketAddress",
            "2.0.1",
            "Hostname or IP address to bind the connection socket to a " + "local (UNIX domain) socket.",
            false),
    SOCKET_TIMEOUT(
            "socketTimeout",
            new Integer[] {10000, null, null, null, null, null},
            0,
            "2.0.1",
            "Defined the "
                    + "network socket timeout (SO_TIMEOUT) in milliseconds. Value of 0 disables this timeout. \n"
                    + "If the goal is to set a timeout for all queries, since MariaDB 10.1.1, the server has permitted a "
                    + "solution to limit the query time by setting a system variable, max_statement_time. The advantage is that"
                    + " the connection then is still usable.\n"
                    + "Default: 0 (standard configuration) or 10000ms (using \"aurora\" failover configuration).",
            false),
    INTERACTIVE_CLIENT(
            "interactiveClient",
            Boolean.FALSE,
            "2.0.1",
            "Session timeout is defined by the wait_timeout "
                    + "server variable. Setting interactiveClient to true will tell the server to use the interactive_timeout "
                    + "server variable.",
            false),
    DUMP_QUERY_ON_EXCEPTION(
            "dumpQueriesOnException",
            Boolean.FALSE,
            "2.0.1",
            "If set to 'true', an exception is thrown "
                    + "during query execution containing a query string.",
            false),
    USE_OLD_ALIAS_METADATA_BEHAVIOR(
            "useOldAliasMetadataBehavior",
            Boolean.FALSE,
            "2.0.1",
            "Metadata ResultSetMetaData.getTableName() returns the physical table name. \"useOldAliasMetadataBehavior\""
                    + " permits activating the legacy code that sends the table alias if set.",
            false),
    SESSION_VARIABLES(
            "sessionVariables",
            "2.0.1",
            "<var>=<value> pairs separated by comma, mysql session variables, "
                    + "set upon establishing successful connection.",
            false),
    CREATE_DATABASE_IF_NOT_EXISTS(
            "createDatabaseIfNotExist",
            Boolean.FALSE,
            "2.0.1",
            "the specified database in the url will be created if non-existent.It will not take effect in oracle mode.",
            false),
    SERVER_TIMEZONE(
            "serverTimezone",
            "2.0.1",
            "Defines the server time zone.\n"
                    + "to use only if the jre server has a different time implementation of the server.\n"
                    + "(best to have the same server time zone when possible).",
            false),
    NULL_CATALOG_MEANS_CURRENT(
            "nullCatalogMeansCurrent",
            Boolean.TRUE,
            "2.0.1",
            "DatabaseMetaData use current catalog" + " if null.",
            false),
    TINY_INT_IS_BIT(
            "tinyInt1isBit",
            Boolean.TRUE,
            "2.0.1",
            "Datatype mapping flag, handle Tiny as BIT(boolean).",
            false),
    YEAR_IS_DATE_TYPE(
            "yearIsDateType",
            Boolean.TRUE,
            "2.0.1",
            "Year is date type, rather than numerical.",
            false),
    USE_SSL(
            "useSsl",
            Boolean.FALSE,
            "2.0.1",
            "Force SSL on connection. (legacy alias \"useSSL\")",
            false),
    USER_COMPRESSION(
            "useCompression",
            Boolean.FALSE,
            "2.0.1",
            "Compresses the exchange with the database through gzip."
                    + " This permits better performance when the database is not in the same location.",
            false),
    ALLOW_MULTI_QUERIES(
            "allowMultiQueries",
            Boolean.FALSE,
            "2.0.1",
            "permit multi-queries like insert into ab (i) "
                    + "values (1); insert into ab (i) values (2).",
            false),
    REWRITE_BATCHED_STATEMENTS(
            "rewriteBatchedStatements",
            Boolean.FALSE,
            "2.0.1",
            "For insert queries, rewrite "
                    + "batchedStatement to execute in a single executeQuery.\n"
                    + "example:\n"
                    + "   insert into ab (i) values (?) with first batch values = 1, second = 2 will be rewritten\n"
                    + "   insert into ab (i) values (1), (2). \n"
                    + "\n"
                    + "If query cannot be rewriten in \"multi-values\", rewrite will use multi-queries : INSERT INTO "
                    + "TABLE(col1) VALUES (?) ON DUPLICATE KEY UPDATE col2=? with values [1,2] and [2,3]\" will be rewritten\n"
                    + "INSERT INTO TABLE(col1) VALUES (1) ON DUPLICATE KEY UPDATE col2=2;INSERT INTO TABLE(col1) VALUES (3) ON "
                    + "DUPLICATE KEY UPDATE col2=4\n"
                    + "\n"
                    + "when active, the useServerPrepStmts option is set to false",
            false),
    TCP_KEEP_ALIVE(
            "tcpKeepAlive",
            Boolean.TRUE,
            "2.0.1",
            "Sets corresponding option on the connection socket.",
            false),
    TCP_RCV_BUF(
            "tcpRcvBuf",
            (Integer) null,
            0,
            "2.0.1",
            "set buffer size for TCP buffer (SO_RCVBUF).",
            false),
    TCP_SND_BUF(
            "tcpSndBuf",
            (Integer) null,
            0,
            "2.0.1",
            "set buffer size for TCP buffer (SO_SNDBUF).",
            false),
    SOCKET_FACTORY(
            "socketFactory",
            "2.0.1",
            "to use a custom socket factory, set it to the full name of the class that"
                    + " implements javax.net.SocketFactory.",
            false),
    PIN_GLOBAL_TX_TO_PHYSICAL_CONNECTION(
            "pinGlobalTxToPhysicalConnection",
            Boolean.FALSE,
            "2.0.1",
            "",
            false),
    TRUST_SERVER_CERTIFICATE(
            "trustServerCertificate",
            Boolean.FALSE,
            "2.0.1",
            "When using SSL, do not check server's" + " certificate.",
            false),
    SERVER_SSL_CERT(
            "serverSslCert",
            "2.0.1",
            "Permits providing server's certificate in DER form, or server's CA"
                    + " certificate. The server will be added to trustStor. This permits a self-signed certificate to be trusted.\n"
                    + "Can be used in one of 3 forms : \n"
                    + "* serverSslCert=/path/to/cert.pem (full path to certificate)\n"
                    + "* serverSslCert=classpath:relative/cert.pem (relative to current classpath)\n"
                    + "* or as verbatim DER-encoded certificate string \"------BEGIN CERTIFICATE-----\" .",
            false),
    USE_FRACTIONAL_SECONDS(
            "useFractionalSeconds",
            Boolean.TRUE,
            "2.0.1",
            "Correctly handle subsecond precision in"
                    + " timestamps (feature available with MariaDB 5.3 and later).\n"
                    + "May confuse 3rd party components (Hibernate).",
            false),
    AUTO_RECONNECT(
            "autoReconnect",
            Boolean.FALSE,
            "2.0.1",
            "Driver must recreateConnection after a failover.",
            false),
    FAIL_ON_READ_ONLY(
            "failOnReadOnly",
            Boolean.FALSE,
            "2.0.1",
            "After a master failover and no other master found,"
                    + " back on a read-only host ( throw exception if not).",
            false),
    RETRY_ALL_DOWN(
            "retriesAllDown",
            120,
            0,
            "2.0.1",
            "When using loadbalancing, the number of times the driver should"
                    + " cycle through available hosts, attempting to connect.\n"
                    + "     * Between cycles, the driver will pause for 250ms if no servers are available.",
            false),
    FAILOVER_LOOP_RETRIES(
            "failoverLoopRetries",
            120,
            0,
            "2.0.1",
            "When using failover, the number of times the driver"
                    + " should cycle silently through available hosts, attempting to connect.\n"
                    + "     * Between cycles, the driver will pause for 250ms if no servers are available.\n"
                    + "     * if set to 0, there will be no silent reconnection",
            false),
    VALID_CONNECTION_TIMEOUT(
            "validConnectionTimeout",
            0,
            0,
            "2.0.1",
            "When in multiple hosts, after this time in"
                    + " second without used, verification that the connections haven't been lost.\n"
                    + "     * When 0, no verification will be done. Defaults to 0 (120 before 1.5.8 version)",
            false),
    LOAD_BALANCE_BLACKLIST_TIMEOUT(
            "loadBalanceBlacklistTimeout",
            50,
            0,
            "2.0.1",
            "time in second a server is" + " blacklisted after a connection failure.",
            false),
    CACHE_PREP_STMTS(
            "cachePrepStmts",
            Boolean.FALSE,
            "2.0.1",
            "enable/disable prepare Statement cache, default false.",
            false),
    PREP_STMT_CACHE_SIZE(
            "prepStmtCacheSize",
            250,
            0,
            "2.0.1",
            "This sets the number of prepared statements that the "
                    + "driver will cache per connection if \"cachePrepStmts\" is enabled.",
            false),
    PREP_STMT_CACHE_SQL_LIMIT(
            "prepStmtCacheSqlLimit",
            2048,
            0,
            "2.0.1",
            "This is the maximum length of a prepared SQL"
                    + " statement that the driver will cache  if \"cachePrepStmts\" is enabled.",
            false),
    ASSURE_READONLY(
            "assureReadOnly",
            Boolean.TRUE,
            "2.0.1",
            "Ensure that when Connection.setReadOnly(true) is called, host is in read-only mode by "
                    + "setting the session transaction to read-only.",
            false),
    USE_LEGACY_DATETIME_CODE(
            "useLegacyDatetimeCode",
            Boolean.TRUE,
            "2.0.1",
            "if true (default) store date/timestamps "
                    + "according to client time zone.\n"
                    + "if false, store all date/timestamps in DB according to server time zone, and time information (that is a"
                    + " time difference), doesn't take\n"
                    + "timezone in account.",
            false),
    MAXIMIZE_MYSQL_COMPATIBILITY(
            "maximizeMysqlCompatibility",
            Boolean.FALSE,
            "2.0.1",
            "maximize MySQL compatibility.\n"
                    + "when using jdbc setDate(), will store date in client timezone, not in server timezone when "
                    + "useLegacyDatetimeCode = false.\n"
                    + "default to false.",
            false),
    USE_SERVER_PREP_STMTS(
            "useServerPrepStmts",
            Boolean.FALSE,
            "2.0.1",
            "useServerPrepStmts must prepared statements be"
                    + " prepared on server side, or just faked on client side.\n"
                    + "     * if rewriteBatchedStatements is set to true, this options will be set to false.",
            false),
    TRUSTSTORE(
            "trustStore",
            "2.0.1",
            "File path of the trustStore file (similar to java System property "
                    + "\"javax.net.ssl.trustStore\"). (legacy alias trustCertificateKeyStoreUrl)\n"
                    + "Use the specified file for trusted root certificates.\n"
                    + "When set, overrides serverSslCert.",
            false),
    TRUST_CERTIFICATE_KEYSTORE_PASSWORD(
            "trustStorePassword",
            "2.0.1",
            "Password for the trusted root certificate file"
                    + " (similar to java System property \"javax.net.ssl.trustStorePassword\").\n"
                    + "(legacy alias trustCertificateKeyStorePassword).",
            false),
    KEYSTORE(
            "keyStore",
            "2.0.1",
            "File path of the keyStore file that contain client private key store and associate "
                    + "certificates (similar to java System property \"javax.net.ssl.keyStore\", but ensure that only the "
                    + "private key's entries are used).(legacy alias clientCertificateKeyStoreUrl).",
            false),
    KEYSTORE_PASSWORD(
            "keyStorePassword",
            "2.0.1",
            "Password for the client certificate keyStore (similar to java "
                    + "System property \"javax.net.ssl.keyStorePassword\").(legacy alias clientCertificateKeyStorePassword)",
            false),
    PRIVATE_KEYS_PASSWORD(
            "keyPassword",
            "2.0.1",
            "Password for the private key in client certificate keyStore. (only "
                    + "needed if private key password differ from keyStore password).",
            false),
    ENABLED_SSL_PROTOCOL_SUITES(
            "enabledSslProtocolSuites",
            "2.0.1",
            "Force TLS/SSL protocol to a specific set of TLS "
                    + "versions (comma separated list). \n"
                    + "Example : \"TLSv1, TLSv1.1, TLSv1.2\"\n"
                    + "(Alias \"enabledSSLProtocolSuites\" works too)",
            false),
    ENABLED_SSL_CIPHER_SUITES(
            "enabledSslCipherSuites",
            "2.0.1",
            "Force TLS/SSL cipher (comma separated list).\n"
                    + "Example : \"TLS_DHE_RSA_WITH_AES_256_GCM_SHA384, TLS_DHE_DSS_WITH_AES_256_GCM_SHA384\"",
            false),
    CONTINUE_BATCH_ON_ERROR(
            "continueBatchOnError",
            Boolean.TRUE,
            "2.0.1",
            "When executing batch queries, must batch " + "continue on error.",
            false),
    JDBC_COMPLIANT_TRUNCATION(
            "jdbcCompliantTruncation",
            Boolean.TRUE,
            "2.0.1",
            "Truncation error (\"Data truncated for"
                    + " column '%' at row %\", \"Out of range value for column '%' at row %\") will be thrown as error, and not as warning.",
            false),
    CACHE_CALLABLE_STMTS(
            "cacheCallableStmts",
            Boolean.TRUE,
            "2.0.1",
            "enable/disable callable Statement cache, default" + " true.",
            false),
    CALLABLE_STMT_CACHE_SIZE(
            "callableStmtCacheSize",
            150,
            0,
            "2.0.1",
            "This sets the number of callable statements "
                    + "that the driver will cache per VM if \"cacheCallableStmts\" is enabled.",
            false),
    CONNECTION_ATTRIBUTES(
            "connectionAttributes",
            "2.0.1",
            "When performance_schema is active, permit to send server "
                    + "some client information in a key;value pair format "
                    + "(example: connectionAttributes=key1:value1,key2,value2).\n"
                    + "Those informations can be retrieved on server within tables performance_schema.session_connect_attrs "
                    + "and performance_schema.session_account_connect_attrs.\n"
                    + "This can permit from server an identification of client/application",
            false),
    USE_BATCH_MULTI_SEND(
            "useBatchMultiSend",
            (Boolean) null,
            "2.0.1",
            "*Not compatible with aurora*\n"
                    + "Driver will can send queries by batch. \n"
                    + "If set to false, queries are sent one by one, waiting for the result before sending the next one. \n"
                    + "If set to true, queries will be sent by batch corresponding to the useBatchMultiSendNumber option value"
                    + " (default 100) or according to the max_allowed_packet server variable if the packet size does not permit"
                    + " sending as many queries. Results will be read later, avoiding a lot of network latency when the client"
                    + " and server aren't on the same host. \n"
                    + "\n"
                    + "This option is mainly effective when the client is distant from the server.",
            false),
    USE_BATCH_MULTI_SEND_NUMBER(
            "useBatchMultiSendNumber",
            100,
            1,
            "2.0.1",
            "When option useBatchMultiSend is active,"
                    + " indicate the maximum query send in a row before reading results.",
            false),
    LOGGING(
            "log",
            Boolean.FALSE,
            "2.0.1",
            "Enable log information. \n"
                    + "require Slf4j version > 1.4 dependency.\n"
                    + "Log level correspond to Slf4j logging implementation",
            false),
    PROFILE_SQL(
            "profileSql",
            Boolean.FALSE,
            "2.0.1",
            "log query execution time.",
            false),
    MAX_QUERY_LOG_SIZE(
            "maxQuerySizeToLog",
            1024,
            0,
            "2.0.1",
            "Max query log size.",
            false),
    SLOW_QUERY_TIME(
            "slowQueryThresholdNanos",
            null,
            0L,
            "2.0.1",
            "Will log query with execution time superior" + " to this value (if defined )",
            false),
    PASSWORD_CHARACTER_ENCODING(
            "passwordCharacterEncoding",
            "2.0.1",
            "Indicate password encoding charset. If not set," + " driver use platform's default charset.",
            false),
    PIPELINE_AUTH(
            "usePipelineAuth",
            (Boolean) null,
            "2.0.1",
            "*Not compatible with aurora*\n"
                    + "During connection, different queries are executed. When option is active those queries are send using"
                    + " pipeline (all queries are send, then only all results are reads), permitting faster connection "
                    + "creation",
            false),
    ENABLE_PACKET_DEBUG(
            "enablePacketDebug",
            Boolean.FALSE,
            "2.0.1",
            "Driver will save the last 16 MariaDB packet "
                    + "exchanges (limited to first 1000 bytes). Hexadecimal value of those packets will be added to stacktrace"
                    + " when an IOException occur.\n"
                    + "This option has no impact on performance but driver will then take 16kb more memory.",
            false),
    SSL_HOSTNAME_VERIFICATION(
            "disableSslHostnameVerification",
            Boolean.FALSE,
            "2.0.1",
            "When using ssl, the driver "
                    + "checks the hostname against the server's identity as presented in the server's certificate (checking "
                    + "alternative names or the certificate CN) to prevent man-in-the-middle attacks. This option permits "
                    + "deactivating this validation. Hostname verification is disabled when the trustServerCertificate "
                    + "option is set",
            false),
    USE_BULK_PROTOCOL(
            "useBulkStmts",
            Boolean.FALSE,
            "2.0.1",
            "Use dedicated COM_STMT_BULK_EXECUTE protocol for batch "
                    + "insert when possible. (batch without Statement.RETURN_GENERATED_KEYS and streams) to have faster batch. "
                    + "(significant only if server MariaDB >= 10.2.7)",
            false),
    AUTOCOMMIT(
            "autocommit",
            Boolean.TRUE,
            "2.0.1",
            "Set default autocommit value on connection initialization",
            false),
    POOL(
            "pool",
            Boolean.FALSE,
            "2.0.1",
            "Use pool. This option is useful only if not using a DataSource object, but "
                    + "only a connection object.",
            false),
    POOL_NAME(
            "poolName",
            "2.0.1",
            "Pool name that permits identifying threads. default: auto-generated as "
                    + "MariaDb-pool-<pool-index>",
            false),
    MAX_POOL_SIZE(
            "maxPoolSize",
            8,
            1,
            "2.0.1",
            "The maximum number of physical connections that the pool should " + "contain.",
            false),
    MIN_POOL_SIZE(
            "minPoolSize",
            (Integer) null,
            0,
            "2.0.1",
            "When connections are removed due to not being used for "
                    + "longer than than \"maxIdleTime\", connections are closed and removed from the pool. \"minPoolSize\" "
                    + "indicates the number of physical connections the pool should keep available at all times. Should be less"
                    + " or equal to maxPoolSize.",
            false),
    MAX_IDLE_TIME(
            "maxIdleTime",
            600,
            Options.MIN_VALUE__MAX_IDLE_TIME,
            "2.0.1",
            "The maximum amount of time in seconds"
                    + " that a connection can stay in the pool when not used. This value must always be below @wait_timeout"
                    + " value - 45s \n"
                    + "Default: 600 in seconds (=10 minutes), minimum value is 60 seconds",
            false),
    POOL_VALID_MIN_DELAY(
            "poolValidMinDelay",
            1000,
            0,
            "2.0.1",
            "When asking a connection to pool, the pool will "
                    + "validate the connection state. \"poolValidMinDelay\" permits disabling this validation if the connection"
                    + " has been borrowed recently avoiding useless verifications in case of frequent reuse of connections. "
                    + "0 means validation is done each time the connection is asked.",
            false),
    STATIC_GLOBAL(
            "staticGlobal",
            Boolean.FALSE,
            "2.0.1",
            "Indicates the values of the global variables "
                    + "max_allowed_packet, wait_timeout, autocommit, auto_increment_increment, time_zone, system_time_zone and"
                    + " tx_isolation) won't be changed, permitting the pool to create new connections faster.",
            false),
    REGISTER_POOL_JMX(
            "registerJmxPool",
            Boolean.TRUE,
            "2.0.1",
            "Register JMX monitoring pools.",
            false),
    USE_RESET_CONNECTION(
            "useResetConnection",
            Boolean.FALSE,
            "2.0.1",
            "When a connection is closed() "
                    + "(given back to pool), the pool resets the connection state. Setting this option, the prepare command "
                    + "will be deleted, session variables changed will be reset, and user variables will be destroyed when the"
                    + " server permits it (>= MariaDB 10.2.4, >= MySQL 5.7.3), permitting saving memory on the server if the "
                    + "application make extensive use of variables. Must not be used with the useServerPrepStmts option",
            false),
    ALLOW_MASTER_DOWN(
            "allowMasterDownConnection",
            Boolean.FALSE,
            "2.0.1",
            "When using master/slave configuration, "
                    + "permit to create connection when master is down. If no master is up, default connection is then a slave "
                    + "and Connection.isReadOnly() will then return true.",
            false),
    GALERA_ALLOWED_STATE(
            "galeraAllowedState",
            "2.0.1",
            "Usually, Connection.isValid just send an empty packet to "
                    + "server, and server send a small response to ensure connectivity. When this option is set, connector will"
                    + " ensure Galera server state \"wsrep_local_state\" correspond to allowed values (separated by comma). "
                    + "Example \"4,5\", recommended is \"4\". see galera state to know more.",
            false),
    USE_AFFECTED_ROWS(
            "useAffectedRows",
            Boolean.FALSE,
            "2.0.1",
            "If false (default), use \"found rows\" for the row count of statements. This corresponds to the JDBC standard.\n"
                    + "If true, use \"affected rows\" for the row count. This changes the behavior of, for example, UPDATE... ON DUPLICATE KEY statements.",
            false),
    INCLUDE_STATUS(
            "includeInnodbStatusInDeadlockExceptions",
            Boolean.FALSE,
            "2.0.1",
            "add \"SHOW ENGINE INNODB STATUS\" result to exception trace when having a deadlock exception",
            false),
    INCLUDE_THREAD_DUMP(
            "includeThreadDumpInDeadlockExceptions",
            Boolean.FALSE,
            "2.0.1",
            "add thread dump to exception trace when having a deadlock exception",
            false),
    READ_AHEAD(
            "useReadAheadInput",
            Boolean.TRUE,
            "2.0.1",
            "use a buffered inputSteam that read socket available data",
            false),
    KEY_STORE_TYPE(
            "keyStoreType",
            (String) null,
            "2.0.1",
            "indicate key store type (JKS/PKCS12). default is null, then using java default type",
            false),
    TRUST_STORE_TYPE(
            "trustStoreType",
            (String) null,
            "2.0.1",
            "indicate trust store type (JKS/PKCS12). default is null, then using java default type",
            false),
    SERVICE_PRINCIPAL_NAME(
            "servicePrincipalName",
            (String) null,
            "2.0.1",
            "when using GSSAPI authentication, SPN (Service Principal Name) use the server SPN information. When set, "
                    + "connector will use this value, ignoring server information",
            false),
    DEFAULT_FETCH_SIZE(
            "defaultFetchSize",
            0,
            0,
            "2.0.1",
            "The driver will call setFetchSize(n) with this value on all newly-created Statements",
            false),
    USE_COMPATIBLE_METADATA(
            "useCompatibleMetadata",
            Boolean.FALSE,
            "2.4.9",
            "force DatabaseMetadata.getDatabaseProductName() "
                    + "to return \"MySQL\" or \"Oracle\" as database, not real database type",
            false),
    BLANK_TABLE_NAME_META(
            "blankTableNameMeta",
            Boolean.FALSE,
            "2.0.1",
            "Resultset metadata getTableName always return blank. "
                    + "This option is mainly for ORACLE db compatibility",
            false),
    CREDENTIAL_TYPE(
            "credentialType",
            (String) null,
            "2.0.1",
            "Indicate the credential plugin type to use. Plugin must be present in classpath",
            false),
    SERVER_KEY_FILE(
            "serverRsaPublicKeyFile",
            (String) null,
            "2.0.1",
            "Indicate path to MySQL server public key file",
            false),
    ALLOW_SERVER_KEY_RETRIEVAL(
            "allowPublicKeyRetrieval",
            Boolean.FALSE,
            "2.0.1",
            "Permit to get MySQL server key retrieval",
            false),
    TLS_SOCKET_TYPE(
            "tlsSocketType",
            (String) null,
            "2.0.1",
            "Indicate TLS socket type implementation",
            false),
    TRACK_SCHEMA(
            "trackSchema",
            Boolean.TRUE,
            "2.0.1",
            "manage session_track_schema setting when server has CLIENT_SESSION_TRACK capability",
            false),
    // Oceanbase extended options
    SUPPORT_LOB_LOCATOR(
            "supportLobLocator",
            Boolean.TRUE,
            "2.0.1",
            "Lob locator switch for BLOB and CLOB type data",
            false),
    USE_OB_CHECKSUM(
            "useObChecksum",
            Boolean.TRUE,
            "2.0.1",
            "Support the calculation of checksum or not for MySQL Compress Protocol. "
                    + "If false, MySQL Compress protocol will not be used by server, even though client declares to use the compression protocol.",
            false),
    ALLOW_ALWAYS_SEND_PARAM_TYPES(
            "allowSendParamTypes",
            Boolean.FALSE,
            "2.0.1",
            "Store types of parameters in first package that is sent to the server",
            false),
    USE_FORMAT_EXCEPTION_MESSAGE(
            "useFormatExceptionMessage",
            Boolean.FALSE,
            "2.0.1",
            "Error message in ORACLE format used, such as 'ORA-'",
            false),
    COMPLEX_DATA_CACHE_SIZE(
            "complexDataCacheSize",
            50,
            0,
            "2.0.1",
            "Cached complex data size",
            false),
    CACHE_COMPLEX_DATA(
            "cacheComplexData",
            Boolean.TRUE,
            "2.0.1",
            "Whether to cache complex data",
            false),
    USE_SQL_STRING_CACHE(
            "useSqlStringCache",
            Boolean.FALSE,
            "2.0.1",
            "Cache sql sql strings into local jdbc memory",
            false),
    USE_SERVER_PS_STMT_CHECKSUM(
            "useServerPsStmtChecksum",
            Boolean.TRUE,
            "2.0.1",
            "Use prepare statement checksum to ensure the correctness of the mysql protocol",
            false),
    CHARACTER_ENCODING(
            "characterEncoding",
            "utf8",
            "2.0.1",
            "Support mysql url option characterEncoding",
            false),
    USE_CURSOR_FETCH(
            "useCursorFetch",
            Boolean.FALSE,
            "2.1.0",
            "Indicate driver to fetch data from server by bunch of fetchSize rows. This permit to avoid having to fetch all results from server.",
            false),
    SUPPORT_NAME_BINDING(
            "supportNameBinding",
            Boolean.TRUE,
            "2.2.2",
            "Oracle name binding switch the apis such as setIntAtName and registerOutParameterAtName",
            false),
    SOCKS_PROXY_HOST(
            "socksProxyHost",
            (String) null,
            "2.2.3",
            "Name or IP address of SOCKS host to connect through.",
            false),
    SOCKS_PROXY_PORT(
            "socksProxyPort",
            1080,
            0,
            "2.2.3",
            "Port of SOCKS server.",
            false),
    CONNECT_PROXY(
            "connectProxy",
            Boolean.FALSE,
            "2.2.3",
            "Indicate driver to connect to ob proxy ",
            false),
    PIECE_LENGTH(
            "pieceLength",
            1048576,
            0,
            "2.2.6",
            "The size of data sent each time when COM_STMT_SEND_PIECE_DATA protocol is used in Oracle mode",
            false),
    USE_PIECE_DATA(
            "usePieceData",
            Boolean.FALSE,
            "2.2.6",
            "Use COM_STMT_SEND_PIECE_DATA protocol to set InputStream and Reader parameters in Oracle mode",
            false),
    USE_ORACLE_PREPARE_EXECUTE(
            "useOraclePrepareExecute",
            Boolean.FALSE,
            "2.2.6",
            "Oracle mode preparedStatement don't communicate with server until execute using COM_STMT_PREPARE_EXECUTE",
            false),
    AUTO_DESERIALIZE(
            "autoDeserialize",
            Boolean.FALSE,
            "2.2.7",
            "NA",
            false),
    MAX_BATCH_TOTOAL_PARAMS_NUM(
            "maxBatchTotalParamsNum",
            30000,
            0,
            "2.2.7",
            "When using executeBatch, the maximum number of spliced parameters",
            false),
    EMULATE_UNSUPPORTED_PSTMTS(
            "emulateUnsupportedPstmts",
            Boolean.FALSE,
            "2.2.8",
            "If ps is abnormal, it will be degraded to the text protocol, otherwise it throws exception",
            true),
    ENABLE_QUERY_TIMEOUT(
            "enableQueryTimeouts",
            Boolean.TRUE,
            "2.2.8",
            "When enabled, query timeouts set via Statement.setQueryTimeout() use a shared java.util.Timer instance for scheduling. Even if the timeout doesn't expire before the query is processed, there will be memory used by the TimerTask for the given timeout which won't be reclaimed until the time the timeout would have expired if it hadn't been cancelled by the driver. High-load environments might want to consider disabling this functionality.",
            true),
    USE_CURSOR_OFFSET(
            "useCursorOffset",
            Boolean.FALSE,
            "2.2.8",
            "Use special COM_STMT_FETCH protocol for Oracle mode to fetch rows of specific position",
            false),
    BLOB_SEND_CHUNK_SIZE(
            "blobSendChunkSize",
            1024*1204,
            0,
            "2.2.9",
            "Chunk size to use when sending BLOB/CLOBs via ServerPreparedStatements. Note that this value cannot exceed the value of \"maxAllowedPacket\" and, if that is the case, then this value will be corrected automatically.",
            false),
    TNSNAMES_PATH(
            "tnsnamesPath",
            (String) null,
            "2.2.9",
            "NA",
            false),
    TNSNAMES_DETECTION_PERIOD(
            "tnsnamesDetectionPeriod",
            10,
            0,
            "2.2.9",
            "NA",
            false),
    LOAD_BALANCE_STRATEGY(
            "loadBalanceStrategy",
            (String) "random",
            "2.2.9",
            "NA",
            false),
    SERVER_AFFINITY_ORDER (
            "serverAffinityOrder",
            (String) null,
            "2.2.9",
            "NA",
            false),
    TRANSFORMED_BIT_BOOLEAN(
            "transformedBitIsBoolean",
            Boolean.FALSE,
            "2.2.9",
            "If the driver converts TINYINT(1) to a different type, should it use BOOLEAN instead of BIT for future compatibility with MySQL-5.0, as MySQL-5.0 has a BIT type?",
            false),
    CONNECTION_COLLATION (
            "connectionCollation",
            (String) null,
            "2.2.9",
            "Instructs the server to set session system variable 'collation_connection' to the specified collation name and set 'character_set_client' and 'character_set_connection' to the corresponding character set. ",
            false),
    USE_ARRAY_BINDING(
            "useArrayBinding",
            Boolean.FALSE,
            "2.2.10",
            "Oracle mode Use array binding to reduce network round-trips and increase performance.",
            false),
    SEND_CONNECTION_ATTRIBUTES(
            "sendConnectionAttributes",
            Boolean.TRUE,
            "2.2.10",
            "Used to determine whether to send connection extended attributes.",
            false),
    REWRITE_INSERT_BY_MULTI_QUERIES (
            "rewriteInsertByMultiQueries",
            Boolean.FALSE,
            "2.2.10.2",
            "Force to rewrite the sql as multiple INSERT queries which are separated by semi-colons",
            false),
    USE_LOCAL_XID (
            "useLocalXID",
            Boolean.TRUE,
            "2.2.10.4",
            "Construct XID entity locally",
            false),
    USE_OCEANBASE_PROTOCOLV20(
            "useOceanBaseProtocolV20",
            Boolean.TRUE,
            "2.4.0",
            "Use v20 protocol to transmit data",
            false),
    ENABLE_FULL_LINK_TRACE(
            "enableFullLinkTrace",
            Boolean.FALSE,
            "2.4.0",
            "Based on OceanBase v2.0 protocol",
            false),
    CLOBBER_STREAMING_RESULTS(
            "clobberStreamingResults",
            Boolean.FALSE,
            "2.4.1",
            "This will cause a streaming result set to be automatically closed, and any outstanding data still streaming from the server to be discarded if another query is executed before all the data has been read from the server.",
            false),
    MAX_ROWS(
            "maxRows",
            0,
            0,
            "2.4.1",
            "The maximum number of rows to return. The default \"0\" means return all rows.",
            false),
    ZERO_DATE_TIME_BEHAVIOR(
            "zeroDateTimeBehavior",
            Options.ZERO_DATETIME_EXCEPTION,
            "2.4.1",
            "How Mysql Mode to represent invalid dates?  Valid values are convertToNull,exception or round.",
            false),
    ALLOW_NAN_AND_INF(
            "allowNanAndInf",
            Boolean.FALSE,
            "2.4.1",
            "Should PreparedStatement.setDouble() allow NaN or +/- INF values?",
            false),
    DEFAULT_CONNECTION_ATTRIBUTES_BAN_LIST (
            "defaultConnectionAttributesBanList",
            (String) null,
            "2.4.1",
            "The list of default connectionAttributes that will not be sent to the server when sendConnectionAttributes = true.",
            false),
    ENABLE_OB20_CHECKSUM(
            "enableOb20Checksum",
            Boolean.TRUE,
            "2.4.1",
            "Calculate header checksum and tail checksum in OceanBase v2.0 protocol packet",
            false),
    OCP_ACCESS_INTERVAL(
            "ocpAccessInterval",
            5,
            0,
            "2.4.2",
            "Interval time in minutes between accesses to OCP.",
            false),
    HTTP_CONNECT_TIMEOUT(
            "httpConnectTimeout",
            0,
            0,
            "2.4.2",
            "Sets a specified timeout value, in milliseconds, to be used when opening a communications link to the resource referenced by this URLConnection. A timeout of zero is interpreted as an infinite timeout.",
            false),
    HTTP_READ_TIMEOUT(
            "httpReadTimeout",
            0,
            0,
            "2.4.2",
            "A non-zero value specifies the timeout in milliseconds when reading from Input stream when an URLConnection is established to a resource. A timeout of zero is interpreted as an infinite timeout.",
            false),
    LOAD_BALANCE_HANDLE_FAILOVER(
            "loadBalanceHandleFailover",
            Boolean.TRUE,
            "2.4.2",
            "This parameter is used to control whether a new connection is created to replace the old one when an exception occurs when using loadbalance.",
            false),
    COMPATIBLE_OJDBC_VERSION(
            "compatibleOjdbcVersion",
            6,
            0,
            "2.4.5",
            "compatible ojdbc version 6/8",
            false),
    COMPATIBLE_MYSQL_VERSION(
            "compatibleMysqlVersion",
            5,
            0,
            "2.4.5",
            "compatible mysql-jdbc version 5/8",
            false),
    USE_INFORMATION_SCHEMA(
            "useInformationSchema",
            Boolean.FALSE,
            "2.4.5",
            "Should the driver use the INFORMATION_SCHEMA to derive information used by 'DatabaseMetaData'? Default is \"true\" when connecting to MySQL 8.0.3+, otherwise default is \"false\".",
            false),
    N_CHARACTER_ENCODING(
            "nCharacterEncoding",
            (String) null,
            "2.4.6",
            "The National character set used by OceanBase, which determines how server and client encode and decode the NCHAR and NVARCHAR2 data types on the connection, can be set to utf16, or utf8.",
            false),
    EMPTY_STRINGS_CONVERT_TO_ZERO(
            "emptyStringsConvertToZero",
            Boolean.TRUE,
            "2.4.7",
            "Should the driver allow conversions from empty string fields to numeric values of 0?",
            false),
    USE_LOB_LOCATOR_V2(
            "useLobLocatorV2",
            Boolean.TRUE,
            "2.4.7",
            "Set to true to use lob v2, false to use the original lob",
            false),
    EXTEND_ORACLE_RESULTSET_CLASS(
            "extendOracleResultSetClass",
            Boolean.FALSE,
            "2.4.7",
            "Provides how to use streaming result set and complete result sets under the ps protocol",
            false),
    LOG_TRACE_TIME(
            "logTraceTime",
            1000,
            0,
            "2.4.7",
            "If option 'log' is true, CallInterface or SendRequest and ReceiveResponse which take longer than 'logTraceTime'(in milliseconds) will be logged.",
            false),
    ORACLE_CHANGE_READ_ONLY(
            "oracleChangeReadOnlyToRepeatableRead",
            Boolean.FALSE,
            "2.4.7",
            "Used in Connection.setReadOnly(boolean readOnly) to put this connection in repeatable-read mode when readOnly is true.",
            false),
    ALLOW_LOAD_LOCAL_INFILE(
            "allowLoadLocalInfile",
            Boolean.FALSE,
            "2.4.8",
            "Permit loading data from file",
            false),
    ENCLOSE_PARAM_IN_PARENTHESES(
            "encloseParamInParentheses",
            Boolean.TRUE,
            "2.4.8",
            "When oracle mode binds a parameter through text protocol, enclose the parameter values in parentheses.",
            false),
    REMARKS_REPORTING(
            "remarksReporting",
            Boolean.FALSE,
            "2.4.9",
            "add options for getTable interface query associated with two views all_objects and all_tab_comments",
            false
    );

  private final String optionName;
  private final String description;
  private final boolean required;
  private final Object objType;
  private final Object defaultValue;
  private final Object minValue;
  private final Object maxValue;

  DefaultOptions(
      final String optionName,
      final String implementationVersion,
      String description,
      boolean required) {
    this.optionName = optionName;
    this.description = description;
    this.required = required;
    objType = String.class;
    defaultValue = null;
    minValue = null;
    maxValue = null;
  }

  DefaultOptions(
      final String optionName,
      final String defaultValue,
      final String implementationVersion,
      String description,
      boolean required) {
    this.optionName = optionName;
    this.description = description;
    this.required = required;
    objType = String.class;
    this.defaultValue = defaultValue;
    minValue = null;
    maxValue = null;
  }

  DefaultOptions(
          final String optionName,
          final int defaultValue,
          final String implementationVersion,
          String description,
          boolean required) {
    this.optionName = optionName;
    this.description = description;
    this.required = required;
    objType = String.class;
    this.defaultValue = Integer.valueOf(defaultValue);
    minValue = null;
    maxValue = null;
  }

  DefaultOptions(
      final String optionName,
      final Boolean defaultValue,
      final String implementationVersion,
      String description,
      boolean required) {
    this.optionName = optionName;
    this.objType = Boolean.class;
    this.defaultValue = defaultValue;
    this.description = description;
    this.required = required;
    minValue = null;
    maxValue = null;
  }

  DefaultOptions(
      final String optionName,
      final Integer defaultValue,
      final Integer minValue,
      final String implementationVersion,
      String description,
      boolean required) {
    this.optionName = optionName;
    this.objType = Integer.class;
    this.defaultValue = defaultValue;
    this.minValue = minValue;
    this.maxValue = Integer.MAX_VALUE;
    this.description = description;
    this.required = required;
  }

  DefaultOptions(
      final String optionName,
      final Long defaultValue,
      final Long minValue,
      final String implementationVersion,
      String description,
      boolean required) {
    this.optionName = optionName;
    this.objType = Long.class;
    this.defaultValue = defaultValue;
    this.minValue = minValue;
    this.maxValue = Long.MAX_VALUE;
    this.description = description;
    this.required = required;
  }

  DefaultOptions(
      final String optionName,
      final Integer[] defaultValue,
      final Integer minValue,
      final String implementationVersion,
      String description,
      boolean required) {
    this.optionName = optionName;
    this.objType = Integer.class;
    this.defaultValue = defaultValue;
    this.minValue = minValue;
    this.maxValue = Integer.MAX_VALUE;
    this.description = description;
    this.required = required;
  }

  public static Options defaultValues(final HaMode haMode) {
    return parse(haMode, "", new Properties());
  }

  /**
   * Generate an Options object with default value corresponding to High Availability mode.
   *
   * @param haMode current high Availability mode
   * @param pool is for pool
   * @return Options object initialized
   */
  public static Options defaultValues(HaMode haMode, boolean pool) {
    Properties properties = new Properties();
    properties.setProperty("pool", String.valueOf(pool));
    Options options = parse(haMode, "", properties);
    postOptionProcess(options, null);
    return options;
  }

  /**
   * Parse additional properties.
   *
   * @param haMode current haMode.
   * @param urlParameters options defined in url
   * @param options initial options
   */
  public static void parse(final HaMode haMode, final String urlParameters, final Options options) {
    Properties prop = new Properties();
    parse(haMode, urlParameters, prop, options);
    postOptionProcess(options, null);
  }

  private static Options parse(
      final HaMode haMode, final String urlParameters, final Properties properties) {
    Options options = parse(haMode, urlParameters, properties, null);
    postOptionProcess(options, null);
    return options;
  }

  public static String[] getUrlParameters(String urlParameters) {
    char last_ch = urlParameters.charAt(0);
    ArrayList<String> paramlist  = new ArrayList();
    int start = 0;
    int i = 0;
    int status = 0; //NORMAL
    for(i = 0; i < urlParameters.length(); i++) {
      char ch = urlParameters.charAt(i);
      if (ch != '&' && ch != '\\' && ch != '"' && ch != '\'') {
        last_ch = ch;
        continue;
      }
      switch (status) {
        case 0: {
          if (ch == '\'' && last_ch != '\\') {
            status = 1; //single-quote
          } else if (ch == '\"' && last_ch != '\"') {
            status = 2; //double-quote
          } else if (ch == '&') {
            paramlist.add(urlParameters.substring(start, i));
            start = i + 1;
          }
          break;
        }
        case 1: {
          if (ch == '\'' && last_ch != '\\') {
            status = 0;
          }
          break;
        }
        case 2: {
          if (ch == '\"' && last_ch != '\\') {
            status = 0;
          }
          break;
        }
        default:
          last_ch = ch; //do nothing
      }
      last_ch = ch;
    }
    if (i > start) {
      paramlist.add(urlParameters.substring(start, i));
    }
    return (String[]) paramlist.toArray(new String[paramlist.size()]);
  }

  /**
   * Parse additional properties .
   *
   * @param haMode current haMode.
   * @param urlParameters options defined in url
   * @param properties options defined by properties
   * @param options initial options
   * @return options
   */
  public static Options parse(
      final HaMode haMode,
      final String urlParameters,
      final Properties properties,
      final Options options) {
    if (urlParameters != null && !urlParameters.isEmpty()) {

      String[] parameters = getUrlParameters(urlParameters);
//      String[] parameters = urlParameters.split("&");
      urlParameters.split("&");
      for (String parameter : parameters) {
        int pos = parameter.indexOf('=');
        if (pos == -1) {
          if (!properties.containsKey(parameter)) {
            properties.setProperty(parameter, "");
          }
        } else {
          if (!properties.containsKey(parameter.substring(0, pos))) {
            properties.setProperty(parameter.substring(0, pos), parameter.substring(pos + 1));
          }
        }
      }
    }
    return parse(haMode, properties, options);
  }

  private static Options parse(
      final HaMode haMode, final Properties properties, final Options paramOptions) {
    final Options options = paramOptions != null ? paramOptions : new Options();

    try {
      // Option object is already initialized to default values.
      // loop on properties,
      // - check DefaultOption to check that property value correspond to type (and range)
      // - set values
      for (final String key : properties.stringPropertyNames()) {
        final String propertyValue = properties.getProperty(key);
        final DefaultOptions o = OptionUtils.OPTIONS_MAP.get(key);
        if (o != null && propertyValue != null) {
          final Field field = Options.class.getField(o.optionName);
          if (o.objType.equals(String.class)) {
            field.set(options, propertyValue);
          } else if (o.objType.equals(Boolean.class)) {
            switch (propertyValue.toLowerCase()) {
              case "":
              case "1":
              case "true":
                field.set(options, Boolean.TRUE);
                break;

              case "0":
              case "false":
                field.set(options, Boolean.FALSE);
                break;

              default:
                throw new IllegalArgumentException(
                    "Optional parameter "
                        + o.optionName
                        + " must be boolean (true/false or 0/1) was \""
                        + propertyValue
                        + "\"");
            }
          } else if (o.objType.equals(Integer.class)) {
            try {
              final Integer value = Integer.parseInt(propertyValue);
              assert o.minValue != null;
              assert o.maxValue != null;
              if (value.compareTo((Integer) o.minValue) < 0
                  || value.compareTo((Integer) o.maxValue) > 0) {
                throw new IllegalArgumentException(
                    "Optional parameter "
                        + o.optionName
                        + " must be greater or equal to "
                        + o.minValue
                        + (((Integer) o.maxValue != Integer.MAX_VALUE)
                            ? " and smaller than " + o.maxValue
                            : " ")
                        + ", was \""
                        + propertyValue
                        + "\"");
              }
              field.set(options, value);
            } catch (NumberFormatException n) {
              throw new IllegalArgumentException(
                  "Optional parameter "
                      + o.optionName
                      + " must be Integer, was \""
                      + propertyValue
                      + "\"");
            }
          } else if (o.objType.equals(Long.class)) {
            try {
              final Long value = Long.parseLong(propertyValue);
              assert o.minValue != null;
              assert o.maxValue != null;
              if (value.compareTo((Long) o.minValue) < 0
                  || value.compareTo((Long) o.maxValue) > 0) {
                throw new IllegalArgumentException(
                    "Optional parameter "
                        + o.optionName
                        + " must be greater or equal to "
                        + o.minValue
                        + (((Long) o.maxValue != Long.MAX_VALUE)
                            ? " and smaller than " + o.maxValue
                            : " ")
                        + ", was \""
                        + propertyValue
                        + "\"");
              }
              field.set(options, value);
            } catch (NumberFormatException n) {
              throw new IllegalArgumentException(
                  "Optional parameter "
                      + o.optionName
                      + " must be Long, was \""
                      + propertyValue
                      + "\"");
            }
          }
        } else {
          // keep unknown option:
          // those might be used in authentication or identity plugin
          options.nonMappedOptions.setProperty(key, properties.getProperty(key));
        }
      }

      // field with multiple default according to HA_MODE
      if (options.socketTimeout == null) {
        options.socketTimeout = ((Integer[]) SOCKET_TIMEOUT.defaultValue)[haMode.ordinal()];
      }
    } catch (NoSuchFieldException | IllegalAccessException n) {
      n.printStackTrace();
    } catch (SecurityException s) {
      // only for jws, so never thrown
      throw new IllegalArgumentException("Security too restrictive : " + s.getMessage());
    }

    return options;
  }

  /**
   * Option initialisation end : set option value to a coherent state.
   *
   * @param options options
   * @param credentialPlugin credential plugin
   */
  public static void postOptionProcess(final Options options, CredentialPlugin credentialPlugin) {
    if (options.enableFullLinkTrace) {
      options.useOceanBaseProtocolV20 = true;
    }

    if (options.usePieceData) {
      options.useOraclePrepareExecute = true;
      options.useCursorFetch = true;
    }

    // cursor functionality is available only for server-side prepared statements
    if (options.useCursorFetch || options.useOraclePrepareExecute) {
      options.useServerPrepStmts = true;
    }

    // pipe cannot use read and write socket simultaneously
    if (options.pipe != null) {
      options.useBatchMultiSend = false;
      options.usePipelineAuth = false;
    }

    // if min pool size default to maximum pool size if not set
    if (options.pool) {
      options.minPoolSize = options.minPoolSize == null ? options.maxPoolSize : Math.min(options.minPoolSize, options.maxPoolSize);
    }

    // if fetchSize is set to less than 0, default it to 0
    if (options.defaultFetchSize < 0) {
      options.defaultFetchSize = 0;
    }

    if (credentialPlugin != null && credentialPlugin.mustUseSsl()) {
      options.useSsl = Boolean.TRUE;
    }
  }

  /**
   * Generate parameter String equivalent to options.
   *
   * @param options options
   * @param haMode high availability Mode
   * @param sb String builder
   */
  public static void propertyString(
      final Options options, final HaMode haMode, final StringBuilder sb) {
    try {
      boolean first = true;
      for (DefaultOptions o : DefaultOptions.values()) {
        final Object value = Options.class.getField(o.optionName).get(options);

        if (value != null && !value.equals(o.defaultValue)) {
          if (first) {
            first = false;
            sb.append('?');
          } else {
            sb.append('&');
          }
          sb.append(o.optionName).append('=');
          if (o.objType.equals(String.class)) {
            sb.append((String) value);
          } else if (o.objType.equals(Boolean.class)) {
            sb.append(((Boolean) value).toString());
          } else if (o.objType.equals(Integer.class) || o.objType.equals(Long.class)) {
            sb.append(value);
          }
        }
      }
    } catch (NoSuchFieldException | IllegalAccessException n) {
      n.printStackTrace();
    }
  }

  public String getOptionName() {
    return optionName;
  }

  public String getDescription() {
    return description;
  }

  public boolean isRequired() {
    return required;
  }
}
