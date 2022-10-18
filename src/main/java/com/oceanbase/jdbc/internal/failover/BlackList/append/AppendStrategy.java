package com.oceanbase.jdbc.internal.failover.BlackList.append;

import java.util.Properties;

import com.oceanbase.jdbc.HostAddress;

public interface AppendStrategy {

    boolean needToAppend(HostAddress hostAddress, Properties properties);
}
