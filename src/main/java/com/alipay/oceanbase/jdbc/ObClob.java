package com.alipay.oceanbase.jdbc;

import java.sql.Clob;
import java.sql.SQLException;

public interface ObClob extends Clob {
    ObLobLocator getLocator();

    void setLocator(ObLobLocator locator);

    void trimBlobToServer(int len) throws SQLException;

    void updateClobToServer(long writeAt, byte[] bytes, int offset, int length) throws SQLException;
}
