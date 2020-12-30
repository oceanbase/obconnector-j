package com.alipay.oceanbase.jdbc.tls;

import java.sql.SQLException;
import java.util.ServiceLoader;

import com.alipay.oceanbase.jdbc.internal.protocol.tls.DefaultTlsSocketPlugin;

public class TlsSocketPluginLoader {

    /**
     * Get authentication plugin from type String. Customs authentication plugin can be added
     * implementing AuthenticationPlugin and registering new type in resources services.
     *
     * @param type authentication plugin type
     * @return Authentication plugin corresponding to type
     * @throws SQLException if no authentication plugin in classpath have indicated type
     */
    public static TlsSocketPlugin get(String type) throws SQLException {
        if (type == null || type.isEmpty()) {
            return new DefaultTlsSocketPlugin();
        }
        ServiceLoader<TlsSocketPlugin> loader = ServiceLoader.load(TlsSocketPlugin.class);
        for (TlsSocketPlugin implClass : loader) {
            if (type.equals(implClass.type())) {
                return implClass;
            }
        }
        throw new SQLException("Client has not found any TLS factory plugin with name '" + type
                               + "'.", "08004", 1251);
    }
}
