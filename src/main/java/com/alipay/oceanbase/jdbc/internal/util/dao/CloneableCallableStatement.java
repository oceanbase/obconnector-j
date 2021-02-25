package com.alipay.oceanbase.jdbc.internal.util.dao;

import com.alipay.oceanbase.jdbc.OceanBaseConnection;

import java.sql.CallableStatement;

public interface CloneableCallableStatement extends CallableStatement {

    CloneableCallableStatement clone(OceanBaseConnection connection)
                                                                    throws CloneNotSupportedException;
}
