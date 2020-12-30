package com.alipay.oceanbase.jdbc.internal.failover;

import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import com.alipay.oceanbase.jdbc.UrlParser;
import com.alipay.oceanbase.jdbc.internal.failover.tools.SearchFilter;
import com.alipay.oceanbase.jdbc.internal.logging.Logger;
import com.alipay.oceanbase.jdbc.internal.logging.LoggerFactory;
import com.alipay.oceanbase.jdbc.internal.util.pool.GlobalStateInfo;
import com.alipay.oceanbase.jdbc.internal.protocol.Protocol;

public abstract class AbstractMastersSlavesListener extends AbstractMastersListener {

  private static final Logger logger = LoggerFactory.getLogger(AbstractMastersSlavesListener.class);
  // These reference are when failloop reconnect failing connection, but lock is already held by
  // another thread (query in progress), so switching the connection wait for the query to be
  // finish.
  // next query will reconnect those during preExecute method, or if actual used connection failed
  // during reconnection phase.
  protected final AtomicReference<Protocol> waitNewSecondaryProtocol = new AtomicReference<>();
  protected final AtomicReference<Protocol> waitNewMasterProtocol = new AtomicReference<>();
  private final AtomicBoolean secondaryHostFail = new AtomicBoolean();
  /* =========================== Failover variables ========================================= */
  private volatile long secondaryHostFailNanos = 0;

  protected AbstractMastersSlavesListener(UrlParser urlParser, final GlobalStateInfo globalInfo) {
    super(urlParser, globalInfo);
    this.secondaryHostFail.set(true);
  }

  /**
   * Handle failover on master or slave connection.
   *
   * @param method called method
   * @param args methods parameters
   * @param protocol current protocol
   * @return HandleErrorResult object to indicate if query has finally been relaunched or exception
   *     if not.
   * @throws SQLException if primary fail reconnection fails
   */
  public HandleErrorResult handleFailover(
      SQLException qe, Method method, Object[] args, Protocol protocol, boolean isClosed)
      throws SQLException {
    if (isExplicitClosed()) {
      throw new SQLException("Connection has been closed !");
    }

    // check that failover is due to kill command
    boolean killCmd =
        qe != null
            && qe.getSQLState() != null
            && qe.getSQLState().equals("70100")
            && 1927 == qe.getErrorCode();

    if (protocol != null) {
      if (protocol.mustBeMasterConnection()) {
        if (!protocol.isMasterConnection()) {
          logger.warn(
              "SQL Primary node [{}, conn={}] is now in read-only mode. Exception : {}",
              this.currentProtocol.getHostAddress().toString(),
              this.currentProtocol.getServerThreadId(),
              qe.getMessage());
        } else if (setMasterHostFail()) {
          logger.warn(
              "SQL Primary node [{}, conn={}] connection fail. Reason : {}",
              this.currentProtocol.getHostAddress().toString(),
              this.currentProtocol.getServerThreadId(),
              qe.getMessage());

          addToBlacklist(protocol.getHostAddress());
        }
        return primaryFail(method, args, killCmd, isClosed);
      } else {
        if (setSecondaryHostFail()) {
          logger.warn(
              "SQL secondary node [{}, conn={}] connection fail. Reason : {}",
              this.currentProtocol.getHostAddress().toString(),
              this.currentProtocol.getServerThreadId(),
              qe.getMessage());
          addToBlacklist(protocol.getHostAddress());
        }
        return secondaryFail(method, args, killCmd);
      }
    } else {
      return primaryFail(method, args, killCmd, isClosed);
    }
  }

  @Override
  protected void resetMasterFailoverData() {
    super.resetMasterFailoverData();

    // if all connection are up, reset failovers timers
    if (!secondaryHostFail.get()) {
      currentConnectionAttempts.set(0);
      lastRetry = 0;
    }
  }

  protected void resetSecondaryFailoverData() {
    if (secondaryHostFail.compareAndSet(true, false)) {
      secondaryHostFailNanos = 0;
    }

    // if all connection are up, reset failovers timers
    if (!isMasterHostFail()) {
      currentConnectionAttempts.set(0);
      lastRetry = 0;
    }
  }

  public long getSecondaryHostFailNanos() {
    return secondaryHostFailNanos;
  }

  /**
   * Set slave connection lost variables.
   *
   * @return true if fail wasn't seen before
   */
  public boolean setSecondaryHostFail() {
    if (secondaryHostFail.compareAndSet(false, true)) {
      secondaryHostFailNanos = System.nanoTime();
      currentConnectionAttempts.set(0);
      return true;
    }
    return false;
  }

  public boolean isSecondaryHostFail() {
    return secondaryHostFail.get();
  }

  public boolean isSecondaryHostFailReconnect() {
    return secondaryHostFail.get() && waitNewSecondaryProtocol.get() == null;
  }

  public boolean isMasterHostFailReconnect() {
    return isMasterHostFail() && waitNewMasterProtocol.get() == null;
  }

  public boolean hasHostFail() {
    return isSecondaryHostFailReconnect() || isMasterHostFailReconnect();
  }

  public SearchFilter getFilterForFailedHost() {
    return new SearchFilter(isMasterHostFail(), isSecondaryHostFail());
  }

  public abstract HandleErrorResult secondaryFail(Method method, Object[] args, boolean killCmd)
      throws SQLException;

  public abstract void foundActiveSecondary(Protocol newSecondaryProtocol) throws SQLException;
}
