package com.alipay.oceanbase.jdbc;

import java.lang.reflect.Field;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import com.alipay.oceanbase.jdbc.internal.util.DeRegister;
import com.alipay.oceanbase.jdbc.internal.util.constant.HaMode;
import com.alipay.oceanbase.jdbc.internal.util.constant.Version;
import com.alipay.oceanbase.jdbc.util.DefaultOptions;
import com.alipay.oceanbase.jdbc.util.Options;

public final class Driver implements java.sql.Driver {

    static {
        try {
            DriverManager.registerDriver(new Driver(), new DeRegister());
        } catch (SQLException e) {
            throw new RuntimeException("Could not register driver", e);
        }
    }

    /**
     * Connect to the given connection string.
     *
     * @param url the url to connect to
     * @return a connection
     * @throws SQLException if it is not possible to connect
     */
    public Connection connect(final String url, final Properties props) throws SQLException {

        UrlParser urlParser = UrlParser.parse(url, props);
        if (urlParser == null || urlParser.getHostAddresses() == null) {
            return null;
        } else {
            return OceanBaseConnection.newConnection(urlParser, null);
        }
    }

    /**
     * returns true if the driver can accept the url.
     *
     * @param url the url to test
     * @return true if the url is valid for this driver
     */
    @Override
    public boolean acceptsURL(String url) {
        return UrlParser.acceptsUrl(url);
    }

    /**
     * Get the property info.
     *
     * @param url the url to get properties for
     * @param info the info props
     * @return all possible connector options
     * @throws SQLException if there is a problem getting the property info
     */
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
    Options options;
    if (url != null && !url.isEmpty()) {
      UrlParser urlParser = UrlParser.parse(url, info);
      if (urlParser == null || urlParser.getOptions() == null) {
        return new DriverPropertyInfo[0];
      }
      options = urlParser.getOptions();
    } else {
      options = DefaultOptions.parse(HaMode.NONE, "", info, null);
    }

    List<DriverPropertyInfo> props = new ArrayList<>();
    for (DefaultOptions o : DefaultOptions.values()) {
      try {
        Field field = Options.class.getField(o.getOptionName());
        Object value = field.get(options);
        DriverPropertyInfo propertyInfo =
            new DriverPropertyInfo(field.getName(), value == null ? null : value.toString());
        propertyInfo.description = o.getDescription();
        propertyInfo.required = o.isRequired();
        props.add(propertyInfo);
      } catch (NoSuchFieldException | IllegalAccessException e) {
        // eat error
      }
    }
    return props.toArray(new DriverPropertyInfo[props.size()]);
  }

    /**
     * gets the major version of the driver.
     *
     * @return the major versions
     */
    public int getMajorVersion() {
        return Version.majorVersion;
    }

    /**
     * gets the minor version of the driver.
     *
     * @return the minor version
     */
    public int getMinorVersion() {
        return Version.minorVersion;
    }

    /**
     * checks if the driver is jdbc compliant.
     *
     * @return true since the driver is not compliant
     */
    public boolean jdbcCompliant() {
        return true;
    }

    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Use logging parameters for enabling logging.");
    }
}
