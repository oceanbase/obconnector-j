package com.alipay.oceanbase.jdbc;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import javax.sql.*;

public class OceanBasePooledConnection implements PooledConnection {

    private final OceanBaseConnection           connection;
    private final List<ConnectionEventListener> connectionEventListeners;
    private final List<StatementEventListener>  statementEventListeners;
    private final AtomicLong                    lastUsed;

    /**
     * Constructor.
     *
     * @param connection connection to retrieve connection options
     */
    public OceanBasePooledConnection(OceanBaseConnection connection) {
    this.connection = connection;
    connection.pooledConnection = this;
    statementEventListeners = new CopyOnWriteArrayList<>();
    connectionEventListeners = new CopyOnWriteArrayList<>();
    lastUsed = new AtomicLong(System.nanoTime());
  }

    /**
     * Creates and returns a <code>Connection</code> object that is a handle for the physical
     * connection that this <code>PooledConnection</code> object represents. The connection pool
     * manager calls this method when an application has called the method <code>
     * DataSource.getConnection</code> and there are no <code>PooledConnection</code> objects
     * available. See the {@link PooledConnection interface description} for more information.
     *
     * @return a <code>Connection</code> object that is a handle to this <code>PooledConnection</code>
     *     object
     */
    public OceanBaseConnection getConnection() {
        return connection;
    }

    /**
     * Closes the physical connection that this <code>PooledConnection</code> object represents. An
     * application never calls this method directly; it is called by the connection pool module, or
     * manager. <br>
     * See the {@link PooledConnection interface description} for more information.
     *
     * @throws SQLException if a database access error occurs
     */
    public void close() throws SQLException {
        connection.pooledConnection = null;
        connection.close();
    }

    /**
     * Abort connection.
     *
     * @param executor executor
     * @throws SQLException if a database access error occurs
     */
    public void abort(Executor executor) throws SQLException {
        connection.pooledConnection = null;
        connection.abort(executor);
    }

    /**
     * Registers the given event failover so that it will be notified when an event occurs on this
     * <code>PooledConnection</code> object.
     *
     * @param listener a component, usually the connection pool manager, that has implemented the
     *     <code>ConnectionEventListener</code> interface and wants to be notified when the connection
     *     is closed or has an error
     * @see #removeConnectionEventListener
     */
    public void addConnectionEventListener(ConnectionEventListener listener) {
        connectionEventListeners.add(listener);
    }

    /**
     * Removes the given event failover from the list of components that will be notified when an
     * event occurs on this <code>PooledConnection</code> object.
     *
     * @param listener a component, usually the connection pool manager, that has implemented the
     *     <code>ConnectionEventListener</code> interface and been registered with this <code>
     *     PooledConnection</code> object as a failover
     * @see #addConnectionEventListener
     */
    public void removeConnectionEventListener(ConnectionEventListener listener) {
        connectionEventListeners.remove(listener);
    }

    /**
     * Registers a <code>StatementEventListener</code> with this <code>PooledConnection</code> object.
     * Components that wish to be notified when <code>PreparedStatement</code>s created by the
     * connection are closed or are detected to be invalid may use this method to register a <code>
     * StatementEventListener</code> with this <code>PooledConnection</code> object. <br>
     *
     * @param listener an component which implements the <code>StatementEventListener</code> interface
     *     that is to be registered with this <code>PooledConnection</code> object <br>
     */
    public void addStatementEventListener(StatementEventListener listener) {
        statementEventListeners.add(listener);
    }

    /**
     * Removes the specified <code>StatementEventListener</code> from the list of components that will
     * be notified when the driver detects that a <code>PreparedStatement</code> has been closed or is
     * invalid. <br>
     *
     * @param listener the component which implements the <code>StatementEventListener</code>
     *     interface that was previously registered with this <code>PooledConnection</code> object
     *     <br>
     */
    public void removeStatementEventListener(StatementEventListener listener) {
        statementEventListeners.remove(listener);
    }

    /**
     * Fire statement close event to listeners.
     *
     * @param st statement
     */
    public void fireStatementClosed(Statement st) {
        if (st instanceof PreparedStatement) {
            StatementEvent event = new StatementEvent(this, (PreparedStatement) st);
            for (StatementEventListener listener : statementEventListeners) {
                listener.statementClosed(event);
            }
        }
    }

    /**
     * Fire statement error to listeners.
     *
     * @param st statement
     * @param ex exception
     */
    public void fireStatementErrorOccured(Statement st, SQLException ex) {
        if (st instanceof PreparedStatement) {
            StatementEvent event = new StatementEvent(this, (PreparedStatement) st, ex);
            for (StatementEventListener listener : statementEventListeners) {
                listener.statementErrorOccurred(event);
            }
        }
    }

    /** Fire Connection close to listening listeners. */
    public void fireConnectionClosed() {
        ConnectionEvent event = new ConnectionEvent(this);
        for (ConnectionEventListener listener : connectionEventListeners) {
            listener.connectionClosed(event);
        }
    }

    /**
     * Fire connection error to listening listeners.
     *
     * @param ex exception
     */
    public void fireConnectionErrorOccured(SQLException ex) {
        ConnectionEvent event = new ConnectionEvent(this, ex);
        for (ConnectionEventListener listener : connectionEventListeners) {
            listener.connectionErrorOccurred(event);
        }
    }

    /**
     * Indicate if there are any registered listener.
     *
     * @return true if no listener.
     */
    public boolean noStmtEventListeners() {
        return statementEventListeners.isEmpty();
    }

    /**
     * Indicate last time this pool connection has been used.
     *
     * @return current last used time (nano).
     */
    public AtomicLong getLastUsed() {
        return lastUsed;
    }

    /** Set last poolConnection use to now. */
    public void lastUsedToNow() {
        lastUsed.set(System.nanoTime());
    }
}
