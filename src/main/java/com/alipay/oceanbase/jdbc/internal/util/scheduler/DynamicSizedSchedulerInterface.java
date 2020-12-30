package com.alipay.oceanbase.jdbc.internal.util.scheduler;

import java.util.concurrent.ScheduledExecutorService;

public interface DynamicSizedSchedulerInterface extends ScheduledExecutorService {

    /**
     * Update the pool thread count with this new value.
     *
     * @param newSize New pool size that is superior to 0
     */
    void setPoolSize(int newSize);
}
