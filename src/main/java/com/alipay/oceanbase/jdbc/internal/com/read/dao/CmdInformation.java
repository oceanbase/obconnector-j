package com.alipay.oceanbase.jdbc.internal.com.read.dao;

import java.sql.ResultSet;
import com.alipay.oceanbase.jdbc.internal.protocol.Protocol;

public interface CmdInformation {

    int RESULT_SET_VALUE = -1;

    int[] getUpdateCounts();

    int[] getServerUpdateCounts();

    long[] getLargeUpdateCounts();

    int getUpdateCount();

    long getLargeUpdateCount();

    void addSuccessStat(long updateCount, long insertId);

    void addErrorStat();

    void reset();

    void addResultSetStat();

    ResultSet getGeneratedKeys(Protocol protocol, String sql);

    ResultSet getBatchGeneratedKeys(Protocol protocol);

    int getCurrentStatNumber();

    boolean moreResults();

    boolean isCurrentUpdateCount();

    void setRewrite(boolean rewritten);
}
