package com.alipay.oceanbase.jdbc.internal.util.scheduler;

import java.util.concurrent.ScheduledThreadPoolExecutor;

public class FixedSizedSchedulerImpl extends ScheduledThreadPoolExecutor {

    /**
     * Create scheduler with fixed size.
     *
     * @param corePoolSize core pool size
     * @param poolName name of pool to identify threads
     */
    public FixedSizedSchedulerImpl(int corePoolSize, String poolName) {
        super(corePoolSize, new OceanBaseThreadFactory(poolName));
    }
}
