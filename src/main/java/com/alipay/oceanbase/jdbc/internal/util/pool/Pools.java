package com.alipay.oceanbase.jdbc.internal.util.pool;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.alipay.oceanbase.jdbc.UrlParser;
import com.alipay.oceanbase.jdbc.internal.util.scheduler.OceanBaseThreadFactory;

public class Pools {

  private static final AtomicInteger poolIndex = new AtomicInteger();
  private static final Map<UrlParser, Pool> poolMap = new ConcurrentHashMap<>();
  private static ScheduledThreadPoolExecutor poolExecutor = null;

  /**
   * Get existing pool for a configuration. Create it if doesn't exists.
   *
   * @param urlParser configuration parser
   * @return pool
   */
  public static Pool retrievePool(UrlParser urlParser) {
    if (!poolMap.containsKey(urlParser)) {
      synchronized (poolMap) {
        if (!poolMap.containsKey(urlParser)) {
          if (poolExecutor == null) {
            poolExecutor =
                new ScheduledThreadPoolExecutor(
                    1, new OceanBaseThreadFactory("MariaDbPool-maxTimeoutIdle-checker"));
          }
          Pool pool = new Pool(urlParser, poolIndex.incrementAndGet(), poolExecutor);
          poolMap.put(urlParser, pool);
          return pool;
        }
      }
    }
    return poolMap.get(urlParser);
  }

  /**
   * Remove pool.
   *
   * @param pool pool to remove
   */
  public static void remove(Pool pool) {
    if (poolMap.containsKey(pool.getUrlParser())) {
      synchronized (poolMap) {
        if (poolMap.containsKey(pool.getUrlParser())) {
          poolMap.remove(pool.getUrlParser());

          if (poolMap.isEmpty()) {
            shutdownExecutor();
          }
        }
      }
    }
  }

  /** Close all pools. */
  public static void close() {
    synchronized (poolMap) {
      for (Pool pool : poolMap.values()) {
        try {
          pool.close();
        } catch (InterruptedException exception) {
          // eat
        }
      }
      shutdownExecutor();
      poolMap.clear();
    }
  }

  /**
   * Closing a pool with name defined in url.
   *
   * @param poolName the option "poolName" value
   */
  public static void close(String poolName) {
    if (poolName == null) {
      return;
    }
    synchronized (poolMap) {
      for (Pool pool : poolMap.values()) {
        if (poolName.equals(pool.getUrlParser().getOptions().poolName)) {
          try {
            pool.close(); // Pool.close() calls Pools.remove(), which does the rest of the cleanup
          } catch (InterruptedException exception) {
            // eat
          }
          return;
        }
      }
    }
  }

  private static void shutdownExecutor() {
    poolExecutor.shutdown();
    try {
      poolExecutor.awaitTermination(10, TimeUnit.SECONDS);
    } catch (InterruptedException interrupted) {
      // eat
    }
    poolExecutor = null;
  }
}
