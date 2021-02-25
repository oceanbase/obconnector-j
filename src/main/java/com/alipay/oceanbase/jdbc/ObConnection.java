package com.alipay.oceanbase.jdbc;

import java.sql.SQLException;

public interface ObConnection extends MysqlConnection {
    void setSessionTimeZone(String zoneID) throws SQLException;

    String getSessionTimeZone();
}
