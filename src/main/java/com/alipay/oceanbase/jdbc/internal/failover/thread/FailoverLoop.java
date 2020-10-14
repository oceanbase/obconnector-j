package com.alipay.oceanbase.jdbc.internal.failover.thread;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.alipay.oceanbase.jdbc.internal.failover.Listener;
import com.alipay.oceanbase.jdbc.internal.failover.tools.SearchFilter;

public class FailoverLoop extends TerminableRunnable {

  private static final ConcurrentLinkedQueue<Listener> queue = new ConcurrentLinkedQueue<>();

  public FailoverLoop(ScheduledExecutorService scheduler) {
    super(scheduler, 1, 1, TimeUnit.SECONDS);
  }

  public static void addListener(Listener listener) {
    queue.add(listener);
  }

  public static void removeListener(Listener listener) {
    queue.remove(listener);
  }

  @Override
  protected void doRun() {
    Listener listener;
    while (!isUnschedule() && (listener = queue.poll()) != null) {
      if (!listener.isExplicitClosed() && listener.hasHostFail() && listener.canRetryFailLoop()) {
        try {
          SearchFilter filter = listener.getFilterForFailedHost();
          filter.setFailoverLoop(true);
          listener.reconnectFailedConnection(filter);
          if (listener.hasHostFail() && !listener.isExplicitClosed()) {
            queue.add(listener);
          }

          // reconnection done !
        } catch (Exception e) {
          // FailoverLoop search connection failed
          queue.add(listener);
        }
      }
    }
  }
}
