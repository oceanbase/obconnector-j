package com.alipay.oceanbase.jdbc.internal.util.scheduler;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Provider for when ever an internal thread pool is needed. This can allow library users to
 * override our default pooling behavior with possibly better and faster options.
 */
public class SchedulerServiceProviderHolder {

  /** The default provider will construct a new pool on every request. */
  public static final SchedulerProvider DEFAULT_PROVIDER =
      new SchedulerProvider() {

        private DynamicSizedSchedulerInterface dynamicSizedScheduler;
        private FixedSizedSchedulerImpl fixedSizedScheduler;
        private ScheduledThreadPoolExecutor timeoutScheduler;
        private ThreadPoolExecutor threadPoolExecutor;

        @Override
        public DynamicSizedSchedulerInterface getScheduler(
            int minimumThreads, String poolName, int maximumPoolSize) {
          if (dynamicSizedScheduler == null) {
            synchronized (this) {
              if (dynamicSizedScheduler == null) {
                dynamicSizedScheduler =
                    new DynamicSizedSchedulerImpl(minimumThreads, poolName, maximumPoolSize);
              }
            }
          }
          return dynamicSizedScheduler;
        }

        @Override
        public ScheduledThreadPoolExecutor getFixedSizeScheduler(
            int minimumThreads, String poolName) {
          if (fixedSizedScheduler == null) {
            synchronized (this) {
              if (fixedSizedScheduler == null) {
                fixedSizedScheduler = new FixedSizedSchedulerImpl(minimumThreads, poolName);
              }
            }
          }
          return fixedSizedScheduler;
        }

        @Override
        public ScheduledThreadPoolExecutor getTimeoutScheduler() {
          if (timeoutScheduler == null) {
            synchronized (this) {
              if (timeoutScheduler == null) {
                timeoutScheduler =
                    new ScheduledThreadPoolExecutor(1, new OceanBaseThreadFactory("MariaDb-timeout"));
                timeoutScheduler.setRemoveOnCancelPolicy(true);
              }
            }
          }
          return timeoutScheduler;
        }

        @Override
        @SuppressWarnings("unchecked")
        public ThreadPoolExecutor getBulkScheduler() {
          if (threadPoolExecutor == null) {
            synchronized (this) {
              if (threadPoolExecutor == null) {
                threadPoolExecutor =
                    new ThreadPoolExecutor(
                        5,
                        100,
                        1,
                        TimeUnit.MINUTES,
                        new SynchronousQueue(),
                        new OceanBaseThreadFactory("MariaDb-bulk"));
              }
            }
          }
          return threadPoolExecutor;
        }

        public void close() {
          synchronized (this) {
            if (dynamicSizedScheduler != null) {
              dynamicSizedScheduler.shutdownNow();
            }
            if (fixedSizedScheduler != null) {
              fixedSizedScheduler.shutdownNow();
            }
            if (timeoutScheduler != null) {
              timeoutScheduler.shutdownNow();
            }
            if (threadPoolExecutor != null) {
              threadPoolExecutor.shutdownNow();
            }

            dynamicSizedScheduler = null;
            fixedSizedScheduler = null;
            timeoutScheduler = null;
            threadPoolExecutor = null;
          }
        }
      };

  private static AtomicReference<SchedulerProvider> currentProvider =
      new AtomicReference<>(DEFAULT_PROVIDER);

  /**
   * Get the currently set {@link SchedulerProvider} from set invocations via {@link
   * #setSchedulerProvider(SchedulerProvider)}. If none has been set a default provider will be
   * provided (never a {@code null} result).
   *
   * @return Provider to get scheduler pools from
   */
  public static SchedulerProvider getSchedulerProvider() {
    return currentProvider.get();
  }

  /**
   * Change the current set scheduler provider. This provider will be provided in future requests to
   * {@link #getSchedulerProvider()}.
   *
   * @param newProvider New provider to use, or {@code null} to use the default provider
   */
  public static void setSchedulerProvider(SchedulerProvider newProvider) {
    if (newProvider == null) {
      newProvider = DEFAULT_PROVIDER;
    }
    currentProvider.getAndSet(newProvider).close();
  }

  /** Close currentProvider. */
  public static void close() {
    currentProvider.get().close();
  }

  /**
   * Get a Dynamic sized scheduler directly with the current set provider.
   *
   * @param initialThreadCount Number of threads scheduler is allowed to grow to
   * @param poolName name of pool to identify threads
   * @param maximumPoolSize maximum pool size
   * @return Scheduler capable of providing the needed thread count
   */
  public static DynamicSizedSchedulerInterface getScheduler(
      int initialThreadCount, String poolName, int maximumPoolSize) {
    return getSchedulerProvider().getScheduler(initialThreadCount, poolName, maximumPoolSize);
  }

  /**
   * Get a fixed sized scheduler directly with the current set provider.
   *
   * @param initialThreadCount Number of threads scheduler is allowed to grow to
   * @param poolName name of pool to identify threads
   * @return Scheduler capable of providing the needed thread count
   */
  public static ScheduledExecutorService getFixedSizeScheduler(
      int initialThreadCount, String poolName) {
    return getSchedulerProvider().getFixedSizeScheduler(initialThreadCount, poolName);
  }

  /**
   * Get a scheduler to handle timeout.
   *
   * @return Scheduler capable of providing the needed thread count
   */
  public static ScheduledExecutorService getTimeoutScheduler() {
    return getSchedulerProvider().getTimeoutScheduler();
  }

  public static ThreadPoolExecutor getBulkScheduler() {
    return getSchedulerProvider().getBulkScheduler();
  }

  /**
   * Provider for thread pools which allow scheduling capabilities. It is expected that the thread
   * pools entire lifecycle (start to stop) is done through the same provider instance.
   */
  public interface SchedulerProvider {

    /**
     * Request to get a scheduler with a minimum number of AVAILABLE threads.
     *
     * @param minimumThreads Minimum number of available threads for the returned scheduler
     * @param poolName name of pool to identify threads
     * @param maximumPoolSize maximum pool size
     * @return A new scheduler that is ready to accept tasks
     */
    DynamicSizedSchedulerInterface getScheduler(
        int minimumThreads, String poolName, int maximumPoolSize);

    ScheduledExecutorService getFixedSizeScheduler(int minimumThreads, String poolName);

    /**
     * Default Timeout scheduler.
     *
     * <p>This is a one Thread fixed sized scheduler. This specific scheduler is using java 1.7
     * RemoveOnCancelPolicy, so the task are removed from queue permitting to avoid memory
     * consumption [CONJ-297]
     *
     * @return A new scheduler that is ready to accept tasks
     */
    ScheduledThreadPoolExecutor getTimeoutScheduler();

    ThreadPoolExecutor getBulkScheduler();

    void close();
  }
}
