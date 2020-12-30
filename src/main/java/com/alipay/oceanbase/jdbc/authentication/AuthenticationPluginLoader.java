package com.alipay.oceanbase.jdbc.authentication;

import java.sql.SQLException;
import java.util.ServiceLoader;

import com.alipay.oceanbase.jdbc.Driver;

public class AuthenticationPluginLoader {

    private static ServiceLoader<AuthenticationPlugin> loader = ServiceLoader.load(
                                                                  AuthenticationPlugin.class,
                                                                  Driver.class.getClassLoader());

    /**
     * Get authentication plugin from type String. Customs authentication plugin can be added
     * implementing AuthenticationPlugin and registering new type in resources services.
     *
     * @param type authentication plugin type
     * @return Authentication plugin corresponding to type
     * @throws SQLException if no authentication plugin in classpath have indicated type
     */
    public static AuthenticationPlugin get(String type) throws SQLException {
        if (type == null || type.isEmpty()) {
            return null;
        }

        for (AuthenticationPlugin implClass : loader) {
            if (type.equals(implClass.type())) {
                return implClass;
            }
        }
        throw new SQLException(
            "Client does not support authentication protocol requested by server. "
                    + "plugin type was = '" + type + "'", "08004", 1251);
    }
}
