package com.alipay.oceanbase.jdbc.internal.com.read.dao;

import java.sql.ResultSet;

import com.alipay.oceanbase.jdbc.internal.com.read.resultset.SelectResultSet;
import com.alipay.oceanbase.jdbc.internal.protocol.Protocol;

public class CmdInformationSingle implements CmdInformation {

    private final long insertId;
    private final int  autoIncrement;
    private long       updateCount;

    /**
     * Object containing update / insert ids, optimized for only one result.
     *
     * @param insertId auto generated id.
     * @param updateCount update count
     * @param autoIncrement connection auto increment value.
     */
    public CmdInformationSingle(long insertId, long updateCount, int autoIncrement) {
        this.insertId = insertId;
        this.updateCount = updateCount;
        this.autoIncrement = autoIncrement;
    }

    @Override
    public int[] getUpdateCounts() {
        return new int[] { (int) updateCount };
    }

    @Override
    public int[] getServerUpdateCounts() {
        return new int[] { (int) updateCount };
    }

    @Override
    public long[] getLargeUpdateCounts() {
        return new long[] { updateCount };
    }

    @Override
    public int getUpdateCount() {
        return (int) updateCount;
    }

    @Override
    public long getLargeUpdateCount() {
        return updateCount;
    }

    @Override
    public void addErrorStat() {
        // not expected
    }

    @Override
    public void reset() {
        // not expected
    }

    @Override
    public void addResultSetStat() {
        // not expected
    }

    /**
     * Get generated Keys.
     *
     * @param protocol current protocol
     * @param sql SQL command
     * @return a resultSet containing the single insert ids.
     */
    public ResultSet getGeneratedKeys(Protocol protocol, String sql) {
        if (insertId == 0) {
            return SelectResultSet.createEmptyResultSet();
        }
        if (updateCount > 1 && sql != null && !isDuplicateKeyUpdate(sql)) {
            long[] insertIds = new long[(int) updateCount];
            for (int i = 0; i < updateCount; i++) {
                insertIds[i] = insertId + i * autoIncrement;
            }
            return SelectResultSet.createGeneratedData(insertIds, protocol, true);
        }
        return SelectResultSet.createGeneratedData(new long[] { insertId }, protocol, true);
    }

    private boolean isDuplicateKeyUpdate(String sql) {
        return sql.matches("(?i).*ON\\s+DUPLICATE\\s+KEY\\s+UPDATE.*");
    }

    @Override
    public ResultSet getBatchGeneratedKeys(Protocol protocol) {
        return getGeneratedKeys(protocol, null);
    }

    public int getCurrentStatNumber() {
        return 1;
    }

    @Override
    public boolean moreResults() {
        updateCount = RESULT_SET_VALUE;
        return false;
    }

    public boolean isCurrentUpdateCount() {
        return updateCount != RESULT_SET_VALUE;
    }

    @Override
    public void addSuccessStat(long updateCount, long insertId) {
        // cannot occur
    }

    public void setRewrite(boolean rewritten) {
        // no need
    }
}
