package com.oceanbase.jdbc.internal.failover.BlackList.append;

import java.util.Properties;

import com.oceanbase.jdbc.HostAddress;
import com.oceanbase.jdbc.internal.failover.BlackList.append.AppendStrategy;

public class NormalAppend implements AppendStrategy {

    @Override
    public String toString() {
        return "NormalAppend{}";
    }

    public String toJson() {
        return "\"APPEND_STRATEGY\":{\"NAME\":\"NORMAL\"}";
    }

    @Override
    public boolean needToAppend(HostAddress hostAddress, Properties properties) {
        return true;
    }
}
