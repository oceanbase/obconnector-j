package com.alipay.oceanbase.obproxy.mysql.jdbc;

import java.sql.*;

import com.alipay.oceanbase.jdbc.internal.util.DeRegister;

public class Driver extends com.alipay.oceanbase.jdbc.Driver {

    static {
        try {
            DriverManager.registerDriver(new com.alipay.oceanbase.jdbc.Driver(), new DeRegister());
        } catch (SQLException e) {
            throw new RuntimeException("Could not register driver", e);
        }
    }

    public Driver() throws SQLException {
        // Required for Class.forName().newInstance()
    }
}
