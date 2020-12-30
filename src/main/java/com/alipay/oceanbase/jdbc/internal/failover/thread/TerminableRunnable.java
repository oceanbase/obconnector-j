package com.alipay.oceanbase.jdbc.internal.failover.thread;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

public abstract class TerminableRunnable implements Runnable {

  private final AtomicReference<State> runState = new AtomicReference<>(State.IDLE);
  private final AtomicBoolean unschedule = new AtomicBoolean();
  private volatile ScheduledFuture<?> scheduledFuture = null;

  public TerminableRunnable(
      ScheduledExecutorService scheduler, long initialDelay, long delay, TimeUnit unit) {
    this.scheduledFuture = scheduler.scheduleWithFixedDelay(this, initialDelay, delay, unit);
  }

  protected abstract void doRun();

  @Override
  public final void run() {
    if (!runState.compareAndSet(State.IDLE, State.ACTIVE)) {
      // task has somehow either started to run in parallel (should not be possible)
      // or more likely the task has now been set to terminate
      return;
    }
    try {
      doRun();
    } finally {
      runState.compareAndSet(State.ACTIVE, State.IDLE);
    }
  }

  /** Unschedule next launched, and wait for the current task to complete before closing it. */
  public void blockTillTerminated() {
    while (!runState.compareAndSet(State.IDLE, State.REMOVED)) {
      // wait and retry
      LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
      if (Thread.currentThread().isInterrupted()) {
        runState.set(State.REMOVED);
        return;
      }
    }
  }

  public boolean isUnschedule() {
    return unschedule.get();
  }

  /**
   * Unschedule task if active, and cancel thread to inform it must be interrupted in a proper way.
   */
  public void unscheduleTask() {
    if (unschedule.compareAndSet(false, true)) {
      scheduledFuture.cancel(false);
      scheduledFuture = null;
    }
  }

  private enum State {
    REMOVED,
    IDLE,
    ACTIVE
  }
}
