package com.alipay.oceanbase.jdbc.internal.failover.thread;

import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.alipay.oceanbase.jdbc.internal.failover.Listener;
import com.alipay.oceanbase.jdbc.internal.util.scheduler.SchedulerServiceProviderHolder;

public class ConnectionValidator {

  private static final int MINIMUM_CHECK_DELAY_MILLIS = 100;
  private final ScheduledExecutorService fixedSizedScheduler =
      SchedulerServiceProviderHolder.getFixedSizeScheduler(1, "validator");
  private final ConcurrentLinkedQueue<Listener> queue = new ConcurrentLinkedQueue<>();
  private final AtomicLong currentScheduledFrequency = new AtomicLong(-1);
  private final ListenerChecker checker = new ListenerChecker();

  /**
   * Add listener to validation list.
   *
   * @param listener listener
   * @param listenerCheckMillis schedule time
   */
  public void addListener(Listener listener, long listenerCheckMillis) {
    queue.add(listener);

    long newFrequency = Math.min(MINIMUM_CHECK_DELAY_MILLIS, listenerCheckMillis);

    // first listener
    if (currentScheduledFrequency.get() == -1) {
      if (currentScheduledFrequency.compareAndSet(-1, newFrequency)) {
        fixedSizedScheduler.schedule(checker, listenerCheckMillis, TimeUnit.MILLISECONDS);
      }
    } else {
      long frequency = currentScheduledFrequency.get();
      if (frequency > newFrequency) {
        currentScheduledFrequency.compareAndSet(frequency, newFrequency);
      }
    }
  }

  /**
   * Remove listener to validation list.
   *
   * @param listener listener
   */
  public void removeListener(Listener listener) {
    queue.remove(listener);

    if (queue.isEmpty()) {
      synchronized (queue) {
        if (currentScheduledFrequency.get() > 0 && queue.isEmpty()) {
          currentScheduledFrequency.set(-1);
        }
      }
    }
  }

  private class ListenerChecker implements Runnable {

    @Override
    public void run() {
      try {
        doRun();
      } finally {
        long delay = currentScheduledFrequency.get();
        if (delay > 0) {
          fixedSizedScheduler.schedule(this, delay, TimeUnit.MILLISECONDS);
        }
      }
    }

    private void doRun() {
      Listener listener;
      Iterator<Listener> tmpQueue = queue.iterator();
      long now = -1;
      while (tmpQueue.hasNext()) {
        listener = tmpQueue.next();
        if (!listener.isExplicitClosed()) {
          long durationNanos =
              (now == -1 ? now = System.nanoTime() : now) - listener.getLastQueryNanos();
          long durationSeconds = TimeUnit.NANOSECONDS.toSeconds(durationNanos);
          if (durationSeconds >= listener.getUrlParser().getOptions().validConnectionTimeout
              && !listener.isMasterHostFail()) {
            boolean masterFail = false;
            if (listener.isMasterConnected()) {
              listener.checkMasterStatus(null);
            } else {
              masterFail = true;
            }

            if (masterFail && listener.setMasterHostFail()) {
              try {
                listener.primaryFail(null, null, false, false);
              } catch (Throwable t) {
                // do nothing
              }
            }
          }
        }
      }
    }
  }
}
