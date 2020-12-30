package com.alipay.oceanbase.jdbc.internal.util.dao;

public interface PrepareResult {

    String getSql();

    int getParamCount();
}
