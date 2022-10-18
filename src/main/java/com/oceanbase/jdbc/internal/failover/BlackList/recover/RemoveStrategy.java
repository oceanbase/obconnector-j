package com.oceanbase.jdbc.internal.failover.BlackList.recover;

import com.oceanbase.jdbc.internal.failover.utils.HostStateInfo;

public interface RemoveStrategy {
    public boolean needToChangeStateInfo(HostStateInfo hostStateInfo);
}
