package com.oceanbase.jdbc.internal.failover.BlackList.recover;

import com.oceanbase.jdbc.internal.failover.utils.HostStateInfo;

public interface RemoveStrategy {

    String toJson();

    boolean needToChangeStateInfo(HostStateInfo hostStateInfo);
}
