package com.alipay.oceanbase.jdbc.internal.osgi;

import java.sql.SQLException;
import java.util.Properties;
import javax.sql.ConnectionPoolDataSource;
import javax.sql.DataSource;
import javax.sql.XADataSource;

import com.alipay.oceanbase.jdbc.Driver;
import com.alipay.oceanbase.jdbc.OceanBasePoolDataSource;
import com.alipay.oceanbase.jdbc.OceanBaseDataSource;
import org.osgi.service.jdbc.DataSourceFactory;

public class OceanBaseDataSourceFactory implements DataSourceFactory {

    @Override
    public DataSource createDataSource(Properties props) throws SQLException {
        if (props != null
            && (props.containsKey(JDBC_MIN_POOL_SIZE) || props.containsKey(JDBC_MAX_POOL_SIZE) || props
                .containsKey(JDBC_MAX_IDLE_TIME))) {
            return createPoolDataSource(props);
        } else {
            return createBasicDataSource(props);
        }
    }

    @Override
    public ConnectionPoolDataSource createConnectionPoolDataSource(Properties props)
                                                                                    throws SQLException {
        if (props != null
            && (props.containsKey(JDBC_MIN_POOL_SIZE) || props.containsKey(JDBC_MAX_POOL_SIZE) || props
                .containsKey(JDBC_MAX_IDLE_TIME))) {
            return createPoolDataSource(props);
        } else {
            return createBasicDataSource(props);
        }
    }

    @Override
    public XADataSource createXADataSource(Properties props) throws SQLException {
        if (props != null
            && (props.containsKey(JDBC_MIN_POOL_SIZE) || props.containsKey(JDBC_MAX_POOL_SIZE) || props
                .containsKey(JDBC_MAX_IDLE_TIME))) {
            return createPoolDataSource(props);
        } else {
            return createBasicDataSource(props);
        }
    }

    @Override
    public Driver createDriver(Properties props) throws SQLException {
        return new Driver();
    }

    private OceanBaseDataSource createBasicDataSource(Properties props) throws SQLException {
        OceanBaseDataSource dataSource = new OceanBaseDataSource();

        if (props.containsKey(JDBC_URL)) {
            dataSource.setUrl(props.getProperty(JDBC_URL));
        }
        if (props.containsKey(JDBC_SERVER_NAME)) {
            dataSource.setServerName(props.getProperty(JDBC_SERVER_NAME));
        }
        if (props.containsKey(JDBC_PORT_NUMBER)) {
            try {
                dataSource.setPortNumber(Integer.parseInt(props.getProperty(JDBC_PORT_NUMBER)));
            } catch (NumberFormatException nfe) {
                throw new SQLException("Port format must be integer, but value is '"
                                       + props.getProperty(JDBC_PORT_NUMBER) + "'");
            }
        }
        if (props.containsKey(JDBC_USER)) {
            dataSource.setUser(props.getProperty(JDBC_USER));
        }
        if (props.containsKey(JDBC_PASSWORD)) {
            dataSource.setPassword(props.getProperty(JDBC_PASSWORD));
        }
        if (props.containsKey(JDBC_DATABASE_NAME)) {
            dataSource.setDatabaseName(props.getProperty(JDBC_DATABASE_NAME));
        }
        return dataSource;
    }

    private OceanBasePoolDataSource createPoolDataSource(Properties props) throws SQLException {
        OceanBasePoolDataSource dataSource = new OceanBasePoolDataSource();
        if (props.containsKey(JDBC_URL)) {
            dataSource.setUrl(props.getProperty(JDBC_URL));
        }
        if (props.containsKey(JDBC_SERVER_NAME)) {
            dataSource.setServerName(props.getProperty(JDBC_SERVER_NAME));
        }
        if (props.containsKey(JDBC_PORT_NUMBER)) {
            try {
                dataSource.setPortNumber(Integer.parseInt(props.getProperty(JDBC_PORT_NUMBER)));
            } catch (NumberFormatException nfe) {
                throw new SQLException("Port number format must be integer, but value is '"
                                       + props.getProperty(JDBC_PORT_NUMBER) + "'");
            }
        }
        if (props.containsKey(JDBC_USER)) {
            dataSource.setUser(props.getProperty(JDBC_USER));
        }
        if (props.containsKey(JDBC_PASSWORD)) {
            dataSource.setPassword(props.getProperty(JDBC_PASSWORD));
        }
        if (props.containsKey(JDBC_DATABASE_NAME)) {
            dataSource.setDatabaseName(props.getProperty(JDBC_DATABASE_NAME));
        }
        if (props.containsKey(JDBC_MAX_IDLE_TIME)) {
            try {
                dataSource.setMaxIdleTime(Integer.parseInt(props.getProperty(JDBC_MAX_IDLE_TIME)));
            } catch (NumberFormatException nfe) {
                throw new SQLException("Max idle time format must be integer, but value is '"
                                       + props.getProperty(JDBC_MAX_IDLE_TIME) + "'");
            }
        }
        if (props.containsKey(JDBC_MAX_POOL_SIZE)) {
            try {
                dataSource.setMaxPoolSize(Integer.parseInt(props.getProperty(JDBC_MAX_POOL_SIZE)));
            } catch (NumberFormatException nfe) {
                throw new SQLException("Max pool size format must be integer, but value is '"
                                       + props.getProperty(JDBC_MAX_POOL_SIZE) + "'");
            }
        }
        if (props.containsKey(JDBC_MIN_POOL_SIZE)) {
            try {
                dataSource.setMinPoolSize(Integer.parseInt(props.getProperty(JDBC_MIN_POOL_SIZE)));
            } catch (NumberFormatException nfe) {
                throw new SQLException("Min pool size format must be integer, but value is '"
                                       + props.getProperty(JDBC_MIN_POOL_SIZE) + "'");
            }
        }
        return dataSource;
    }
}
