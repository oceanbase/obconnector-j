package com.alipay.oceanbase.jdbc.internal.util.dao;

import java.sql.CallableStatement;

import com.alipay.oceanbase.jdbc.OceanBaseConnection;

public interface CloneableCallableStatement extends CallableStatement {

    CloneableCallableStatement clone(OceanBaseConnection connection)
                                                                    throws CloneNotSupportedException;
}
