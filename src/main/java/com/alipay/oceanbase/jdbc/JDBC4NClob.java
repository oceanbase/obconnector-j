package com.alipay.oceanbase.jdbc;

import java.sql.NClob;

public class JDBC4NClob extends Clob implements NClob {

    JDBC4NClob(String charDataInit, ExceptionInterceptor exceptionInterceptor) {
        super(charDataInit, exceptionInterceptor);
    }
}
