package com.alipay.oceanbase.jdbc.internal.osgi;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Dictionary;
import java.util.Hashtable;

import com.alipay.oceanbase.jdbc.Driver;
import com.alipay.oceanbase.jdbc.internal.util.constant.Version;
import com.alipay.oceanbase.jdbc.internal.util.scheduler.SchedulerServiceProviderHolder;
import com.alipay.oceanbase.jdbc.OceanBaseDatabaseMetaData;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.jdbc.DataSourceFactory;

/** The MariaDbActivator registers the JDBC Service with the OSGi-Framework. */
public class OceanBaseActivator implements BundleActivator {

    private ServiceRegistration<DataSourceFactory> service;

    @Override
  public void start(BundleContext context) throws Exception {
    Dictionary<String, Object> properties = new Hashtable<>();
    properties.put(DataSourceFactory.OSGI_JDBC_DRIVER_CLASS, Driver.class.getName());
    properties.put(DataSourceFactory.OSGI_JDBC_DRIVER_NAME, OceanBaseDatabaseMetaData.DRIVER_NAME);
    properties.put(DataSourceFactory.OSGI_JDBC_DRIVER_VERSION, Version.version);
    service =
        context.registerService(
            DataSourceFactory.class, new OceanBaseDataSourceFactory(), properties);
  }

    @Override
    public void stop(BundleContext context) throws Exception {

        if (service != null) {
            service.unregister();
            service = null;
        }
        try {
            DriverManager.getDriver("jdbc:oceanbase:");
            SchedulerServiceProviderHolder.close();
        } catch (SQLException sqle) {
            // eat "No suitable driver"
        }
    }
}
