package com.alipay.oceanbase.jdbc;

import java.security.cert.Extension;
import java.sql.Connection;
import java.sql.SQLException;

public interface ExceptionInterceptor extends Extension {
    SQLException interceptException(SQLException sqlEx, Connection conn);
}
