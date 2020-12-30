package com.alipay.oceanbase.jdbc.internal.util.dao;

import java.util.concurrent.atomic.AtomicBoolean;

import com.alipay.oceanbase.jdbc.internal.com.read.resultset.ColumnDefinition;
import com.alipay.oceanbase.jdbc.internal.ColumnType;
import com.alipay.oceanbase.jdbc.internal.protocol.Protocol;

public class ServerPrepareResult implements PrepareResult {

    private final ColumnDefinition[] columns;
    private final ColumnDefinition[] parameters;
    private final String             sql;
    private final AtomicBoolean      inCache      = new AtomicBoolean();
    private int                      statementId;
    private ColumnType[]             parameterTypeHeader;
    private Protocol                 unProxiedProtocol;
    // share indicator
    private volatile int             shareCounter = 1;
    private volatile boolean         isBeingDeallocate;

    /**
     * PrepareStatement Result object.
     *
     * @param sql query
     * @param statementId server statement Id.
     * @param columns columns information
     * @param parameters parameters information
     * @param unProxiedProtocol indicate the protocol on which the prepare has been done
     */
    public ServerPrepareResult(String sql, int statementId, ColumnDefinition[] columns,
                               ColumnDefinition[] parameters, Protocol unProxiedProtocol) {
        this.sql = sql;
        this.statementId = statementId;
        this.columns = columns;
        this.parameters = parameters;
        this.unProxiedProtocol = unProxiedProtocol;
        this.parameterTypeHeader = new ColumnType[parameters.length];
    }

    public void resetParameterTypeHeader() {
        this.parameterTypeHeader = new ColumnType[parameters.length];
    }

    /**
     * Update information after a failover.
     *
     * @param statementId new statement Id
     * @param unProxiedProtocol the protocol on which the prepare has been done
     */
    public void failover(int statementId, Protocol unProxiedProtocol) {
        this.statementId = statementId;
        this.unProxiedProtocol = unProxiedProtocol;
        this.parameterTypeHeader = new ColumnType[parameters.length];
        this.shareCounter = 1;
        this.isBeingDeallocate = false;
    }

    public void setAddToCache() {
        inCache.set(true);
    }

    public void setRemoveFromCache() {
        inCache.set(false);
    }

    /**
     * Increment share counter.
     *
     * @return true if can be used (is not been deallocate).
     */
    public synchronized boolean incrementShareCounter() {

        if (isBeingDeallocate) {
            return false;
        }

        shareCounter++;
        return true;
    }

    public synchronized void decrementShareCounter() {
        shareCounter--;
    }

    /**
     * Asked if can be deallocate (is not shared in other statement and not in cache) Set deallocate
     * flag to true if so.
     *
     * @return true if can be deallocate
     */
    public synchronized boolean canBeDeallocate() {
        if (shareCounter > 0 || isBeingDeallocate) {
            return false;
        }
        if (!inCache.get()) {
            isBeingDeallocate = true;
            return true;
        }
        return false;
    }

    public int getParamCount() {
        return parameters.length;
    }

    // for unit test
    public synchronized int getShareCounter() {
        return shareCounter;
    }

    public int getStatementId() {
        return statementId;
    }

    public ColumnDefinition[] getColumns() {
        return columns;
    }

    public ColumnDefinition[] getParameters() {
        return parameters;
    }

    public Protocol getUnProxiedProtocol() {
        return unProxiedProtocol;
    }

    public String getSql() {
        return sql;
    }

    public ColumnType[] getParameterTypeHeader() {
        return parameterTypeHeader;
    }
}
