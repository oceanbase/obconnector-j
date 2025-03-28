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
package com.oceanbase.jdbc;

import java.io.Closeable;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;

import javax.sql.*;

import com.oceanbase.jdbc.internal.util.constant.HaMode;
import com.oceanbase.jdbc.internal.util.exceptions.ExceptionFactory;
import com.oceanbase.jdbc.internal.util.pool.Pool;
import com.oceanbase.jdbc.internal.util.pool.Pools;
import com.oceanbase.jdbc.util.DefaultOptions;
import com.oceanbase.jdbc.util.Options;

public class OceanBasePoolDataSource extends OceanBaseDataSource implements
                                                                ConnectionPoolDataSource,
                                                                XADataSource, Closeable,
                                                                AutoCloseable {

    private UrlParser poolUrlParser;
    private Pool      pool;
    private String    poolName;
    private Integer   maxPoolSize;
    private Integer   minPoolSize;
    private Integer   maxIdleTime;
    private Boolean   staticGlobal;
    private Integer   poolValidMinDelay;
    private Boolean   enablePool;

    /**
     * Constructor.
     *
     * @param hostname hostname (ipv4, ipv6, dns name)
     * @param port server port
     * @param database database name
     */
    public OceanBasePoolDataSource(String hostname, int port, String database) {
        super(hostname, port, database);
    }

    public OceanBasePoolDataSource(String url) {
        super(url);
    }

    /** Default constructor. hostname will be localhost, port 3306. */
    public OceanBasePoolDataSource() {
    }

    /**
     * Sets the database name.
     *
     * @param database the name of the database
     * @throws SQLException if error in URL
     */
    public void setDatabaseName(String database) throws SQLException {
        checkNotInitialized();
        this.database = database;
    }

    private void checkNotInitialized() throws SQLException {
        if (pool != null) {
            throw new SQLException("can not perform a configuration change once initialized");
        }
    }

    /**
     * Sets the username.
     *
     * @param user the username
     * @throws SQLException if error in URL
     */
    public void setUser(String user) throws SQLException {
        checkNotInitialized();
        this.user = user;
    }

    /**
     * Sets the password.
     *
     * @param password the password
     * @throws SQLException if error in URL
     */
    public void setPassword(String password) throws SQLException {
        checkNotInitialized();
        this.password = password;
    }

    /**
     * Sets the database port.
     *
     * @param port the port
     * @throws SQLException if error in URL
     */
    public void setPort(int port) throws SQLException {
        checkNotInitialized();
        this.port = port;
    }

    /**
     * Returns the port number.
     *
     * @return the port number
     */
    public int getPortNumber() {
        return getPort();
    }

    /**
     * Sets the port number.
     *
     * @param port the port
     * @throws SQLException if error in URL
     * @see #setPort
     */
    public void setPortNumber(int port) throws SQLException {
        checkNotInitialized();
        if (port > 0) {
            setPort(port);
        }
    }

    /**
     * Sets the connection string URL.
     *
     * @param url the connection string
     * @throws SQLException if error in URL
     */
    public void setUrl(String url) throws SQLException {
        checkNotInitialized();
        this.url = url;
    }

    /**
     * Sets the server name.
     *
     * @param serverName the server name
     * @throws SQLException if error in URL
     */
    public void setServerName(String serverName) throws SQLException {
        checkNotInitialized();
        hostname = serverName;
    }

    /**
     * Attempts to establish a physical database connection that can be used as a pooled connection.
     *
     * @return a <code>PooledConnection</code> object that is a physical connection to the database
     *     that this <code>ConnectionPoolDataSource</code> object represents
     * @throws SQLException if a database access error occurs if the JDBC driver does not support this
     *     method
     */
    public PooledConnection getPooledConnection() throws SQLException {
        if (poolUrlParser == null) {
            initializeUrlParser();
        }
        if (enablePool != null && !enablePool) {
            return new OceanBasePooledConnection((OceanBaseConnection) super.getConnection());
        }
        if (pool == null) {
            initializePool();
        }
        return pool.getPooledConnection();
    }

    /**
     * Attempts to establish a physical database connection that can be used as a pooled connection.
     *
     * @param user the database user on whose behalf the connection is being made
     * @param password the user's password
     * @return a <code>PooledConnection</code> object that is a physical connection to the database
     *     that this <code>ConnectionPoolDataSource</code> object represents
     * @throws SQLException if a database access error occurs
     */
    public PooledConnection getPooledConnection(String user, String password) throws SQLException {
        if (poolUrlParser == null) {
            initializeUrlParser();
        }
        if (enablePool != null && !enablePool) {
            return new OceanBasePooledConnection((OceanBaseConnection) super.getConnection(user,
                password));
        }

        if (pool == null) {
            this.user = user;
            this.password = password;

            initializePool();
            return pool.getPooledConnection();
        }

        if ((poolUrlParser.getUsername() != null ? poolUrlParser.getUsername().equals(user)
            : user == null)
            && (poolUrlParser.getPassword() != null ? poolUrlParser.getPassword().equals(password)
                : (password == null || password.isEmpty()))) {
            return pool.getPooledConnection();
        }

        // username / password are different from the one already used to initialize pool
        // -> return a real new PooledConnection.
        return new OceanBasePooledConnection((OceanBaseConnection) super.getConnection(user,
            password));
    }

    /**
     * Retrieves the log writer for this <code>DataSource</code> object.
     *
     * <p>The log writer is a character output stream to which all logging and tracing messages for
     * this data source will be printed. This includes messages printed by the methods of this object,
     * messages printed by methods of other objects manufactured by this object, and so on. Messages
     * printed to a data source specific log writer are not printed to the log writer associated with
     * the <code>java.sql.DriverManager</code> class.
     *
     * <p>When a <code>DataSource</code> object is created, the log writer is initially null; in other
     * words, the default is for logging to be disabled.
     *
     * @return the log writer for this data source or null if logging is disabled
     * @see #setLogWriter
     */
    public PrintWriter getLogWriter() {
        return null;
    }

    /**
     * Sets the log writer for this <code>DataSource</code> object to the given <code>
     * java.io.PrintWriter</code> object.
     *
     * <p>The log writer is a character output stream to which all logging and tracing messages for
     * this data source will be printed. This includes messages printed by the methods of this object,
     * messages printed by methods of other objects manufactured by this object, and so on. Messages
     * printed to a data source- specific log writer are not printed to the log writer associated with
     * the <code>java.sql.DriverManager</code> class. When a <code>DataSource</code> object is created
     * the log writer is initially null; in other words, the default is for logging to be disabled.
     *
     * @param out the new log writer; to disable logging, set to null
     * @see #getLogWriter
     * @since 1.4
     */
    public void setLogWriter(final PrintWriter out) {
        // not implemented
    }

    /**
     * Gets the maximum time in seconds that this data source can wait while attempting to connect to
     * a database. A value of zero means that the timeout is the default system timeout if there is
     * one; otherwise, it means that there is no timeout. When a <code>DataSource</code> object is
     * created, the login timeout is initially zero.
     *
     * @return the data source login time limit
     * @see #setLoginTimeout
     * @since 1.4
     */
    public int getLoginTimeout() {
        if (connectTimeoutInMs != null) {
            return connectTimeoutInMs / 1000;
        }
        return (poolUrlParser != null) ? poolUrlParser.getOptions().connectTimeout / 1000 : 0;
    }

    /**
     * Sets the maximum time in seconds that this data source will wait while attempting to connect to
     * a database. A value of zero specifies that the timeout is the default system timeout if there
     * is one; otherwise, it specifies that there is no timeout. When a <code>DataSource</code> object
     * is created, the login timeout is initially zero.
     *
     * @param seconds the data source login time limit
     * @throws SQLException if a database access error occurs.
     * @see #getLoginTimeout
     * @since 1.4
     */
    @Override
    public void setLoginTimeout(final int seconds) throws SQLException {
        checkNotInitialized();
        connectTimeoutInMs = seconds * 1000;
    }

    /**
     * Returns an object that implements the given interface to allow access to non-standard methods,
     * or standard methods not exposed by the proxy.
     *
     * <p>If the receiver implements the interface then the result is the receiver or a proxy for the
     * receiver. If the receiver is a wrapper and the wrapped object implements the interface then the
     * result is the wrapped object or a proxy for the wrapped object. Otherwise return the the result
     * of calling <code>unwrap</code> recursively on the wrapped object or a proxy for that result. If
     * the receiver is not a wrapper and does not implement the interface, then an <code>SQLException
     * </code> is thrown.
     *
     * @param iface A Class defining an interface that the result must implement.
     * @return an object that implements the interface. May be a proxy for the actual implementing
     *     object.
     * @throws SQLException If no object found that implements the interface
     * @since 1.6
     */
    public <T> T unwrap(final Class<T> iface) throws SQLException {
        try {
            if (isWrapperFor(iface)) {
                return iface.cast(this);
            } else {
                throw new SQLException(
                    "The receiver is not a wrapper and does not implement the interface " + iface.getName());
            }
        } catch (Exception e) {
            throw new SQLException(
                "The receiver is not a wrapper and does not implement the interface " + iface.getName());
        }
    }

    /**
     * Returns true if this either implements the interface argument or is directly or indirectly a
     * wrapper for an object that does. Returns false otherwise. If this implements the interface then
     * return true, else if this is a wrapper then return the result of recursively calling <code>
     * isWrapperFor</code> on the wrapped object. If this does not implement the interface and is not
     * a wrapper, return false. This method should be implemented as a low-cost operation compared to
     * <code>unwrap</code> so that callers can use this method to avoid expensive <code>unwrap</code>
     * calls that may fail. If this method returns true then calling <code>unwrap</code> with the same
     * argument should succeed.
     *
     * @param interfaceOrWrapper a Class defining an interface.
     * @return true if this implements the interface or directly or indirectly wraps an object that
     *     does.
     * @throws SQLException if an error occurs while determining whether this is a wrapper for an
     *     object with the given interface.
     * @since 1.6
     */
    public boolean isWrapperFor(final Class<?> interfaceOrWrapper) throws SQLException {
        return interfaceOrWrapper.isInstance(this);
    }

    @Override
    public XAConnection getXAConnection() throws SQLException {
        return new OceanBaseXaConnection((OceanBaseConnection) getConnection());
    }

    @Override
    public XAConnection getXAConnection(String user, String password) throws SQLException {
        return new OceanBaseXaConnection((OceanBaseConnection) getConnection(user, password));
    }

    public Logger getParentLogger() {
        return null;
    }

    /**
     * For testing purpose only.
     *
     * @return current url parser.
     */
    protected UrlParser getUrlParser() {
        return poolUrlParser;
    }

    public String getPoolName() {
        return (pool != null) ? pool.getPoolTag() : poolName;
    }

    public void setPoolName(String poolName) throws SQLException {
        checkNotInitialized();
        this.poolName = poolName;
    }

    /**
     * Pool maximum connection size.
     *
     * @return current value.
     */
    public int getMaxPoolSize() {
        if (maxPoolSize == null) {
            return 8;
        }
        return maxPoolSize;
    }

    public void setMaxPoolSize(int maxPoolSize) throws SQLException {
        checkNotInitialized();
        this.maxPoolSize = maxPoolSize;
    }

    /**
     * Get minimum pool size (pool will grow at creation untile reaching this size). Null mean use the
     * pool maximum pool size.
     *
     * @return current value.
     */
    public int getMinPoolSize() {
        if (minPoolSize == null) {
            return getMaxPoolSize();
        }
        return minPoolSize;
    }

    public void setMinPoolSize(int minPoolSize) throws SQLException {
        checkNotInitialized();
        this.minPoolSize = minPoolSize;
    }

    /**
     * Max time a connection can be idle.
     *
     * @return current value.
     */
    public int getMaxIdleTime() {
        if (maxIdleTime == null) {
            return 600;
        }
        return maxIdleTime;
    }

    public void setMaxIdleTime(int maxIdleTime) throws SQLException {
        checkNotInitialized();
        this.maxIdleTime = maxIdleTime;
    }

    public Boolean getStaticGlobal() {
        return staticGlobal;
    }

    public void setStaticGlobal(Boolean staticGlobal) {
        this.staticGlobal = staticGlobal;
    }

    /**
     * If connection has been used in less time than poolValidMinDelay, then no connection validation
     * will be done (0=mean validation every time).
     *
     * @return current value of poolValidMinDelay
     */
    public Integer getPoolValidMinDelay() {
        if (poolValidMinDelay == null) {
            return 1000;
        }
        return poolValidMinDelay;
    }

    public void setPoolValidMinDelay(Integer poolValidMinDelay) {
        this.poolValidMinDelay = poolValidMinDelay;
    }

    private synchronized void initializeUrlParser() throws SQLException {

        if (url != null && !url.isEmpty()) {
            Properties props = new Properties();
            if (user != null) {
                props.setProperty("user", user);
            }
            if (password != null) {
                props.setProperty("password", password);
            }
            if (poolName != null) {
                props.setProperty("poolName", poolName);
            }

            if (database != null) {
                props.setProperty("database", database);
            }
            if (maxPoolSize != null) {
                props.setProperty("maxPoolSize", String.valueOf(maxPoolSize));
            }
            if (minPoolSize != null) {
                props.setProperty("minPoolSize", String.valueOf(minPoolSize));
            }
            if (maxIdleTime != null) {
                props.setProperty("maxIdleTime", String.valueOf(maxIdleTime));
            }
            if (connectTimeoutInMs != null) {
                props.setProperty("connectTimeout", String.valueOf(connectTimeoutInMs));
            }
            if (staticGlobal != null) {
                props.setProperty("staticGlobal", String.valueOf(staticGlobal));
            }
            if (poolValidMinDelay != null) {
                props.setProperty("poolValidMinDelay", String.valueOf(poolValidMinDelay));
            }

            poolUrlParser = UrlParser.parse(url, props);
            enablePool = poolUrlParser.getOptions().pool;

        } else {

            Options options = DefaultOptions.defaultValues(HaMode.NONE);
            options.user = user;
            options.password = password;
            options.poolName = poolName;

            if (maxPoolSize != null) {
                options.maxPoolSize = maxPoolSize;
            }
            if (minPoolSize != null) {
                options.minPoolSize = minPoolSize;
            }
            if (maxIdleTime != null) {
                options.maxIdleTime = maxIdleTime;
            }
            if (staticGlobal != null) {
                options.staticGlobal = staticGlobal;
            }
            if (connectTimeoutInMs != null) {
                options.connectTimeout = connectTimeoutInMs;
            }
            if (poolValidMinDelay != null) {
                options.poolValidMinDelay = poolValidMinDelay;
            }
            if (enablePool != null) {
                options.pool = enablePool;
            } else {
                enablePool = false;
            }

            poolUrlParser = new UrlParser(database, Collections.singletonList(new HostAddress(
                (hostname == null || hostname.isEmpty()) ? "localhost" : hostname,
                port == null ? 3306 : port)), options, HaMode.NONE);
        }
    }

    /** Close datasource. */
    public void close() {
        try {
            if (pool != null) {
                pool.close();
            }
        } catch (InterruptedException interrupted) {
            // eat
        }
    }

    /**
     * Initialize pool.
     *
     * @throws SQLException if connection string has error
     */
    private synchronized void initializePool() throws SQLException {
        if (pool == null) {
            pool = Pools.retrievePool(poolUrlParser);
        }
    }

    /**
     * Get current idle threads. !! For testing purpose only !!
     *
     * @return current thread id's
     */
    public List<Long> testGetConnectionIdleThreadIds() {
        return pool.testGetConnectionIdleThreadIds();
    }

    /**
     * Permit to create test that doesn't wait for maxIdleTime minimum value of 60 seconds. !! For
     * testing purpose only !!
     *
     * @param maxIdleTime forced value of maxIdleTime option.
     * @throws SQLException if connection string has error
     */
    public void testForceMaxIdleTime(int maxIdleTime) throws SQLException {
        initializeUrlParser();
        poolUrlParser.getOptions().maxIdleTime = maxIdleTime;
        pool = Pools.retrievePool(poolUrlParser);
    }

    /**
     * Get pool. !! For testing purpose only !!
     *
     * @return pool
     */
    public Pool testGetPool() {
        return pool;
    }

}
