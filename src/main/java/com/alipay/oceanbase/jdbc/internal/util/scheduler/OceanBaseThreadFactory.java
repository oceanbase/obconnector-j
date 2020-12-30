package com.alipay.oceanbase.jdbc.internal.util.scheduler;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class OceanBaseThreadFactory implements ThreadFactory {

    // start from DefaultThread factory to get security groups and what not
    private final ThreadFactory parentFactory = Executors.defaultThreadFactory();
    private final AtomicInteger threadId      = new AtomicInteger();
    private final String        threadName;

    public OceanBaseThreadFactory(String threadName) {
        this.threadName = threadName;
    }

    @Override
    public Thread newThread(Runnable runnable) {
        Thread result = parentFactory.newThread(runnable);
        result.setName(threadName + "-" + threadId.incrementAndGet());
        result.setDaemon(true); // set as daemon so that mariaDb wont hold up shutdown

        return result;
    }
}
