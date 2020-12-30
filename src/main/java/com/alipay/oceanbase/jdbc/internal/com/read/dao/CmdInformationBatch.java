package com.alipay.oceanbase.jdbc.internal.com.read.dao;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.alipay.oceanbase.jdbc.internal.com.read.resultset.SelectResultSet;
import com.alipay.oceanbase.jdbc.internal.protocol.Protocol;

public class CmdInformationBatch implements CmdInformation {

  private final Queue<Long> insertIds = new ConcurrentLinkedQueue<>();
  private final Queue<Long> updateCounts = new ConcurrentLinkedQueue<>();
  private final int expectedSize;
  private final int autoIncrement;
  private int insertIdNumber = 0;
  private boolean hasException;
  private boolean rewritten;

  /**
   * CmdInformationBatch is similar to CmdInformationMultiple, but knowing it's for batch, doesn't
   * take take of moreResult. That permit to use ConcurrentLinkedQueue, and then when option
   * "useBatchMultiSend" is set and batch is interrupted, will permit to reading thread to keep
   * connection in a correct state without any ConcurrentModificationException.
   *
   * @param expectedSize expected batch size.
   * @param autoIncrement connection auto increment value.
   */
  public CmdInformationBatch(int expectedSize, int autoIncrement) {
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
    hasException = false;
    rewritten = false;
  }

  public void addResultSetStat() {
    this.updateCounts.add((long) RESULT_SET_VALUE);
  }

  @Override
  public void addSuccessStat(long updateCount, long insertId) {
    insertIds.add(insertId);
    insertIdNumber += updateCount;
    updateCounts.add(updateCount);
  }

  @Override
  public int[] getUpdateCounts() {
    if (rewritten) {
      int[] ret = new int[expectedSize];
      int resultValue;
      if (hasException) {
        resultValue = Statement.EXECUTE_FAILED;
      } else if (expectedSize == 1) {
        resultValue = updateCounts.element().intValue();
      } else {
        resultValue = 0;
        Iterator<Long> iterator = updateCounts.iterator();
        while (iterator.hasNext()) {
          if (iterator.next().intValue() != 0) {
            resultValue = Statement.SUCCESS_NO_INFO;
          }
        }
      }
      Arrays.fill(ret, resultValue);
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
  public long[] getLargeUpdateCounts() {
    if (rewritten) {
      long[] ret = new long[expectedSize];
      long resultValue;
      if (hasException) {
        resultValue = Statement.EXECUTE_FAILED;
      } else if (expectedSize == 1) {
        resultValue = updateCounts.element().longValue();
      } else {
        resultValue = 0;
        Iterator<Long> iterator = updateCounts.iterator();
        while (iterator.hasNext()) {
          if (iterator.next().intValue() != 0) {
            resultValue = Statement.SUCCESS_NO_INFO;
          }
        }
      }
      Arrays.fill(ret, resultValue);
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
    Long updateCount = updateCounts.peek();
    return (updateCount == null) ? -1 : updateCount.intValue();
  }

  @Override
  public long getLargeUpdateCount() {
    Long updateCount = updateCounts.peek();
    return (updateCount == null) ? -1 : updateCount;
  }

  @Override
  public ResultSet getBatchGeneratedKeys(Protocol protocol) {
    long[] ret = new long[insertIdNumber];
    int position = 0;
    long insertId;
    Iterator<Long> idIterator = insertIds.iterator();
    for (Long updateCountLong : updateCounts) {
      int updateCount = updateCountLong.intValue();
      if (updateCount != Statement.EXECUTE_FAILED
          && updateCount != RESULT_SET_VALUE
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

    for (Long updateCountLong : updateCounts) {
      int updateCount = updateCountLong.intValue();
      if (updateCount != Statement.EXECUTE_FAILED
          && updateCount != RESULT_SET_VALUE
          && (insertId = idIterator.next()) > 0) {
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
    return false;
  }

  @Override
  public boolean isCurrentUpdateCount() {
    return false;
  }

  public void setRewrite(boolean rewritten) {
    this.rewritten = rewritten;
  }
}
