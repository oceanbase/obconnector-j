package com.oceanbase.jdbc;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Enumeration;
import java.util.Properties;

public class DbmsApplicationInfoProvider implements ClientInfoProvider {

    /**
     * reference: <a href="https://www.oceanbase.com/docs/common-oceanbase-database-cn-1000000003981100">DBMS_APPLICATION_INFO</a>
     */
    private static final String GET_CLIENT_INFO_SQL = "CALL DBMS_APPLICATION_INFO.READ_CLIENT_INFO(?)";
    private static final String SET_CLIENT_INFO_SQL = "CALL DBMS_APPLICATION_INFO.SET_CLIENT_INFO(?)";
    CallableStatement           getClientInfoStmt;
    CallableStatement           setClientInfoStmt;
    PreparedStatement           setClientInfoStmtForMysql;
    Properties                  propertiesForMysql;

    @Override
    public synchronized void initialize(Connection conn, Properties configurationProps) throws SQLException {
        if (((OceanBaseConnection) conn).getProtocol().isOracleMode()) {
            this.getClientInfoStmt = conn.prepareCall(GET_CLIENT_INFO_SQL);
            this.setClientInfoStmt = conn.prepareCall(SET_CLIENT_INFO_SQL);
        } else {
            this.setClientInfoStmtForMysql = conn.prepareStatement(SET_CLIENT_INFO_SQL);
            propertiesForMysql = new Properties();
        }
    }

    @Override
    public synchronized void destroy() throws SQLException {
        if (getClientInfoStmt != null) {
            getClientInfoStmt.close();
        }
        if (setClientInfoStmt != null) {
            setClientInfoStmt.close();
        }
        if (setClientInfoStmtForMysql != null) {
            setClientInfoStmtForMysql.close();
        }
        getClientInfoStmt = null;
        setClientInfoStmt = null;
        setClientInfoStmtForMysql = null;
    }

    @Override
    public synchronized Properties getClientInfo(Connection conn) throws SQLException {
        if (!((OceanBaseConnection) conn).getProtocol().isOracleMode()) {
            return (Properties) propertiesForMysql.clone();
        }
        Properties properties = new Properties();
        getClientInfoStmt.registerOutParameter(1, Types.VARCHAR);
        getClientInfoStmt.execute();
        properties.put("clientInfo", getClientInfoStmt.getString(1));
        return properties;
    }

    @Override
    public synchronized String getClientInfo(Connection conn, String name) throws SQLException {
        if (!((OceanBaseConnection) conn).getProtocol().isOracleMode()) {
            return propertiesForMysql.getProperty("clientInfo");
        }
        getClientInfoStmt.registerOutParameter(1, Types.VARCHAR);
        getClientInfoStmt.execute();
        return getClientInfoStmt.getString(1);
    }

    @Override
    public synchronized void setClientInfo(Connection conn, String name, String value) throws SQLClientInfoException {
        try {
            if (!((OceanBaseConnection) conn).getProtocol().isOracleMode()) {
                setClientInfoStmtForMysql.setString(1, value);
                setClientInfoStmtForMysql.execute();
                propertiesForMysql.put("clientInfo", value);
                return;
            }
            setClientInfoStmt.setString(1, value);
            setClientInfoStmt.execute();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public synchronized void setClientInfo(Connection conn, Properties properties) throws SQLClientInfoException {
        Enumeration<?> names = properties.propertyNames();
        while (names.hasMoreElements()) {
            String name = (String) names.nextElement();
            String value = properties.getProperty(name);
            setClientInfo(conn, name, value);
        }
    }

}
