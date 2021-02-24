package com.alipay.oceanbase.jdbc;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Compatible with jdbc1.X interface class, mainly used to add oracle mode interfaces
 */
public interface ConnectionImpl extends Connection {
    void setSessionTimeZone(String zoneID) throws SQLException;

    String getSessionTimeZone();

}
