package com.alipay.oceanbase.jdbc.internal.util;

import static org.junit.Assert.*;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.alipay.oceanbase.jdbc.internal.util.scheduler.DynamicSizedSchedulerInterface;
import com.alipay.oceanbase.jdbc.internal.util.scheduler.SchedulerServiceProviderHolder;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SchedulerServiceProviderHolderTest {

    @After
    @Before
    public void providerReset() {
        SchedulerServiceProviderHolder.setSchedulerProvider(null);
    }

    @Test
    public void getDefaultProviderTest() {
        assertTrue(SchedulerServiceProviderHolder.DEFAULT_PROVIDER == SchedulerServiceProviderHolder
            .getSchedulerProvider());
    }

    @Test
    public void defaultProviderGetSchedulerTest() {
        testRunnable(SchedulerServiceProviderHolder.getScheduler(1, "testScheduler", 8));
        testRunnable(SchedulerServiceProviderHolder.getFixedSizeScheduler(1, "testFixedScheduler"));
    }

    private void testRunnable(ScheduledExecutorService scheduler) {
    try {
      assertNotNull(scheduler);
      // verify scheduler works
      scheduler.execute(
          () -> {
            try {
              Thread.sleep(10);
            } catch (InterruptedException ie) {
              // eat
            }
          });
      scheduler.shutdown();
      Assert.assertTrue(scheduler.awaitTermination(1000, TimeUnit.MILLISECONDS));
    } catch (InterruptedException ie) {
      fail();
    }
  }

    @Test
    public void defaultProviderSchedulerShutdownTest() {
        testExecuteAfterShutdown(SchedulerServiceProviderHolder.getScheduler(1, "testScheduler", 8));
        testExecuteAfterShutdown(SchedulerServiceProviderHolder.getFixedSizeScheduler(1,
            "testFixedScheduler"));
    }

    private void testExecuteAfterShutdown(ScheduledExecutorService scheduler) {
    scheduler.shutdown();
    try {
      scheduler.execute(() -> {});
      fail("Exception should have thrown");
    } catch (RejectedExecutionException expected) {
      // ignore
    }
  }

    @Test
    public void setAndGetProviderTest() {
        SchedulerServiceProviderHolder.SchedulerProvider emptyProvider = new SchedulerServiceProviderHolder.SchedulerProvider() {
            @Override
            public DynamicSizedSchedulerInterface getScheduler(int minimumThreads, String poolName,
                                                               int maximumPoolSize) {
                throw new UnsupportedOperationException();
            }

            @Override
            public ScheduledThreadPoolExecutor getFixedSizeScheduler(int minimumThreads,
                                                                     String poolName) {
                throw new UnsupportedOperationException();
            }

            @Override
            public ScheduledThreadPoolExecutor getTimeoutScheduler() {
                throw new UnsupportedOperationException();
            }

            @Override
            public ScheduledThreadPoolExecutor getBulkScheduler() {
                throw new UnsupportedOperationException();
            }

            @Override
            public void close() {
                // do nothing
            }
        };

        SchedulerServiceProviderHolder.setSchedulerProvider(emptyProvider);
        assertTrue(emptyProvider == SchedulerServiceProviderHolder.getSchedulerProvider());
    }
}
