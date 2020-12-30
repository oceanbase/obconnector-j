package com.alipay.oceanbase.jdbc.internal.com.read.dao;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

import com.alipay.oceanbase.jdbc.internal.com.read.resultset.SelectResultSet;
import com.alipay.oceanbase.jdbc.internal.protocol.Protocol;

public class CmdInformationMultiple implements CmdInformation {

    private final ArrayList<Long> insertIds;
    private final ArrayList<Long> updateCounts;
    private final int             expectedSize;
    private final int             autoIncrement;
    private int                   insertIdNumber = 0;
    private int                   moreResults;
    private boolean               hasException;
    private boolean               rewritten;

    /**
     * Object containing update / insert ids, optimized for only multiple result.
     *
     * @param expectedSize expected batch size.
     * @param autoIncrement connection auto increment value.
     */
    public CmdInformationMultiple(int expectedSize, int autoIncrement) {
    insertIds = new ArrayList<>(expectedSize);
    updateCounts = new ArrayList<>(expectedSize);
    this.expectedSize = expectedSize;
    this.autoIncrement = autoIncrement;
  }

    @Override
    public void addErrorStat() {
        hasException = true;
        updateCounts.add((long) Statement.EXECUTE_FAILED);
    }

    /**
     * Clear error state, used for clear exception after first batch query, when fall back to
     * per-query execution.
     */
    @Override
    public void reset() {
        insertIds.clear();
        updateCounts.clear();
        insertIdNumber = 0;
        moreResults = 0;
        hasException = false;
        rewritten = false;
    }

    public void addResultSetStat() {
        updateCounts.add((long) RESULT_SET_VALUE);
    }

    @Override
    public void addSuccessStat(long updateCount, long insertId) {
        insertIds.add(insertId);
        insertIdNumber += updateCount;
        updateCounts.add(updateCount);
    }

    @Override
    public int[] getServerUpdateCounts() {
        int[] ret = new int[updateCounts.size()];

        Iterator<Long> iterator = updateCounts.iterator();
        int pos = 0;
        while (iterator.hasNext()) {
            ret[pos++] = iterator.next().intValue();
        }

        return ret;
    }

    @Override
    public int[] getUpdateCounts() {
        if (rewritten) {
            int[] ret = new int[expectedSize];
            Arrays.fill(ret, hasException ? Statement.EXECUTE_FAILED : Statement.SUCCESS_NO_INFO);
            return ret;
        }
        int[] ret = new int[Math.max(updateCounts.size(), expectedSize)];

        Iterator<Long> iterator = updateCounts.iterator();
        int pos = 0;
        while (iterator.hasNext()) {
            ret[pos++] = iterator.next().intValue();
        }

        // in case of Exception
        while (pos < ret.length) {
            ret[pos++] = Statement.EXECUTE_FAILED;
        }

        return ret;
    }

    @Override
    public long[] getLargeUpdateCounts() {
        if (rewritten) {
            long[] ret = new long[expectedSize];
            Arrays.fill(ret, hasException ? Statement.EXECUTE_FAILED : Statement.SUCCESS_NO_INFO);
            return ret;
        }

        long[] ret = new long[Math.max(updateCounts.size(), expectedSize)];

        Iterator<Long> iterator = updateCounts.iterator();
        int pos = 0;
        while (iterator.hasNext()) {
            ret[pos++] = iterator.next();
        }

        // in case of Exception
        while (pos < ret.length) {
            ret[pos++] = Statement.EXECUTE_FAILED;
        }

        return ret;
    }

    @Override
    public int getUpdateCount() {
        if (moreResults >= updateCounts.size()) {
            return -1;
        }
        return updateCounts.get(moreResults).intValue();
    }

    @Override
    public long getLargeUpdateCount() {
        if (moreResults >= updateCounts.size()) {
            return -1;
        }
        return updateCounts.get(moreResults);
    }

    @Override
    public ResultSet getBatchGeneratedKeys(Protocol protocol) {
        long[] ret = new long[insertIdNumber];
        int position = 0;
        long insertId;
        Iterator<Long> idIterator = insertIds.iterator();
        for (Long updateCount : updateCounts) {
            if (updateCount != Statement.EXECUTE_FAILED && updateCount != RESULT_SET_VALUE
                && (insertId = idIterator.next()) > 0) {
                for (int i = 0; i < updateCount; i++) {
                    ret[position++] = insertId + i * autoIncrement;
                }
            }
        }
        return SelectResultSet.createGeneratedData(ret, protocol, true);
    }

    /**
     * Return GeneratedKeys containing insert ids. Insert ids are calculated using autoincrement
     * value.
     *
     * @param protocol current protocol
     * @param sql SQL command
     * @return a resultSet with insert ids.
     */
    public ResultSet getGeneratedKeys(Protocol protocol, String sql) {
        long[] ret = new long[insertIdNumber];
        int position = 0;
        long insertId;
        Iterator<Long> idIterator = insertIds.iterator();
        Iterator<Long> updateIterator = updateCounts.iterator();

        for (int element = 0; element <= moreResults; element++) {
            long updateCount = updateIterator.next();
            if (updateCount != Statement.EXECUTE_FAILED && updateCount != RESULT_SET_VALUE
                && (insertId = idIterator.next()) > 0 && element == moreResults) {
                for (int i = 0; i < updateCount; i++) {
                    ret[position++] = insertId + i * autoIncrement;
                }
            }
        }
        return SelectResultSet.createGeneratedData(ret, protocol, true);
    }

    public int getCurrentStatNumber() {
        return updateCounts.size();
    }

    @Override
    public boolean moreResults() {
        return moreResults++ < updateCounts.size() - 1
               && updateCounts.get(moreResults) == RESULT_SET_VALUE;
    }

    @Override
    public boolean isCurrentUpdateCount() {
        return updateCounts.get(moreResults) != RESULT_SET_VALUE;
    }

    public void setRewrite(boolean rewritten) {
        this.rewritten = rewritten;
    }
}
