package com.alipay.oceanbase.jdbc.internal.util.scheduler;

import java.util.concurrent.ScheduledThreadPoolExecutor;

public class DynamicSizedSchedulerImpl extends ScheduledThreadPoolExecutor implements
                                                                          DynamicSizedSchedulerInterface {

    /**
     * Initialize a scheduler with dynamic pool size.
     *
     * @param corePoolSize initial Core pool size
     * @param poolName name of pool to identify threads
     * @param maximumPoolSize maximum pool size
     */
    public DynamicSizedSchedulerImpl(int corePoolSize, String poolName, int maximumPoolSize) {
        super(corePoolSize, new OceanBaseThreadFactory(poolName));
        setMaximumPoolSize(maximumPoolSize);
    }

    @Override
    public void setPoolSize(int newSize) {
        synchronized (this) {
            super.setCorePoolSize(newSize);
        }
    }
}
