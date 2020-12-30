package com.alipay.oceanbase.jdbc.credential;

import java.sql.SQLException;
import java.util.ServiceLoader;

import com.alipay.oceanbase.jdbc.Driver;

/**
 * Provider to handle plugin authentication. This can allow library users to override our default
 * Authentication provider.
 */
public class CredentialPluginLoader {

    private static ServiceLoader<CredentialPlugin> loader = ServiceLoader.load(
                                                              CredentialPlugin.class,
                                                              Driver.class.getClassLoader());

    /**
     * Get current Identity plugin according to option `identityType`.
     *
     * @param type identity plugin type
     * @return identity plugin
     * @throws SQLException if no identity plugin found with this type is in classpath
     */
    public static CredentialPlugin get(String type) throws SQLException {
        if (type == null || type.isEmpty()) {
            return null;
        }

        for (CredentialPlugin implClass : loader) {
            if (type.equals(implClass.type())) {
                return implClass;
            }
        }
        throw new SQLException("No identity plugin registered with the type \"" + type + "\".",
            "08004", 1251);
    }
}
