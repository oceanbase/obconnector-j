package com.alipay.oceanbase.jdbc;

import java.sql.Blob;
import java.sql.SQLException;

public interface ObBlob extends Blob {
    ObLobLocator getLocator();

    void setLocator(ObLobLocator locator);

    void updateBlobToServer(long writeAt, byte[] bytes, int offset, int length) throws SQLException;

    void trimBlobToServer(int len) throws SQLException;
}
