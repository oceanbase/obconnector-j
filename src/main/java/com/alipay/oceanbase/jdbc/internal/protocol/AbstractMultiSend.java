package com.alipay.oceanbase.jdbc.internal.protocol;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;

import com.alipay.oceanbase.jdbc.internal.com.read.dao.Results;
import com.alipay.oceanbase.jdbc.internal.com.send.ComStmtPrepare;
import com.alipay.oceanbase.jdbc.internal.com.send.parameters.ParameterHolder;
import com.alipay.oceanbase.jdbc.internal.util.BulkStatus;
import com.alipay.oceanbase.jdbc.internal.util.SqlStates;
import com.alipay.oceanbase.jdbc.internal.util.dao.ClientPrepareResult;
import com.alipay.oceanbase.jdbc.internal.util.dao.PrepareResult;
import com.alipay.oceanbase.jdbc.internal.util.dao.ServerPrepareResult;
import com.alipay.oceanbase.jdbc.internal.ColumnType;
import com.alipay.oceanbase.jdbc.internal.io.output.PacketOutputStream;

public abstract class AbstractMultiSend {

    private final Protocol           protocol;
    private final PacketOutputStream writer;
    private final Results            results;
    private final boolean            binaryProtocol;
    private final boolean            readPrepareStmtResult;
    protected int                    statementId = -1;
    protected ColumnType[]           parameterTypeHeader;
    private List<ParameterHolder[]>  parametersList;
    private PrepareResult            prepareResult;
    private List<String>             queries;
    private String                   sql;
    private ThreadPoolExecutor       readScheduler;

    /**
     * Bulk execute for Server PreparedStatement.executeBatch (when no COM_MULTI)
     *
     * @param protocol protocol
     * @param writer outputStream
     * @param results query results
     * @param serverPrepareResult Prepare result
     * @param parametersList parameters
     * @param readPrepareStmtResult must execute prepare result
     * @param sql sql query.
     * @param readScheduler reading thread-pool
     */
    public AbstractMultiSend(Protocol protocol, PacketOutputStream writer, Results results,
                             ServerPrepareResult serverPrepareResult,
                             List<ParameterHolder[]> parametersList, boolean readPrepareStmtResult,
                             String sql, ThreadPoolExecutor readScheduler) {
        this.protocol = protocol;
        this.writer = writer;
        this.results = results;
        this.prepareResult = serverPrepareResult;
        this.parametersList = parametersList;
        this.binaryProtocol = true;
        this.readPrepareStmtResult = readPrepareStmtResult;
        this.sql = sql;
        this.readScheduler = readScheduler;
    }

    /**
     * Bulk execute for client-side PreparedStatement.executeBatch (no prepare).
     *
     * @param protocol current protocol
     * @param writer outputStream
     * @param results results
     * @param clientPrepareResult clientPrepareResult
     * @param parametersList parameters
     * @param readScheduler reading thread-pool
     */
    public AbstractMultiSend(Protocol protocol, PacketOutputStream writer, Results results,
                             final ClientPrepareResult clientPrepareResult,
                             List<ParameterHolder[]> parametersList,
                             ThreadPoolExecutor readScheduler) {
        this.protocol = protocol;
        this.writer = writer;
        this.results = results;
        this.prepareResult = clientPrepareResult;
        this.parametersList = parametersList;
        this.binaryProtocol = false;
        this.readPrepareStmtResult = false;
        this.readScheduler = readScheduler;
    }

    /**
     * Bulk execute for statement.executeBatch().
     *
     * @param protocol protocol
     * @param writer outputStream
     * @param results results
     * @param queries query list
     * @param readScheduler reading thread-pool
     */
    public AbstractMultiSend(Protocol protocol, PacketOutputStream writer, Results results,
                             List<String> queries, ThreadPoolExecutor readScheduler) {
        this.protocol = protocol;
        this.writer = writer;
        this.results = results;
        this.queries = queries;
        this.binaryProtocol = false;
        this.readPrepareStmtResult = false;
        this.readScheduler = readScheduler;
    }

    public abstract void sendCmd(PacketOutputStream writer, Results results,
                                 List<ParameterHolder[]> parametersList, List<String> queries,
                                 int paramCount, BulkStatus status, PrepareResult prepareResult)
                                                                                                throws SQLException,
                                                                                                IOException;

    public abstract SQLException handleResultException(SQLException qex, Results results,
                                                       List<ParameterHolder[]> parametersList,
                                                       List<String> queries, int currentCounter,
                                                       int sendCmdCounter, int paramCount,
                                                       PrepareResult prepareResult);

    public abstract int getParamCount();

    public abstract int getTotalExecutionNumber();

    public PrepareResult getPrepareResult() {
        return prepareResult;
    }

    /**
     * Execute Bulk execution (send packets by batch of useBatchMultiSendNumber or when max packet is
     * reached) before reading results.
     *
     * @return prepare result
     * @throws SQLException if any error occur
     */
    public PrepareResult executeBatch() throws SQLException {
        int paramCount = getParamCount();
        if (binaryProtocol) {
            if (readPrepareStmtResult) {
                parameterTypeHeader = new ColumnType[paramCount];
                if (prepareResult == null && protocol.getOptions().cachePrepStmts
                    && protocol.getOptions().useServerPrepStmts) {
                    String key = protocol.getDatabase() + "-" + sql;
                    prepareResult = protocol.prepareStatementCache().get(key);
                    if (prepareResult != null
                        && !((ServerPrepareResult) prepareResult).incrementShareCounter()) {
                        // in cache but been de-allocated
                        prepareResult = null;
                    }
                }
                statementId = (prepareResult == null) ? -1 : ((ServerPrepareResult) prepareResult)
                    .getStatementId();
            } else if (prepareResult != null) {
                statementId = ((ServerPrepareResult) prepareResult).getStatementId();
            }
        }
        return executeBatchStandard(paramCount);
    }

    /**
     * Execute Bulk execution (send packets by batch of useBatchMultiSendNumber or when max packet is
     * reached) before reading results.
     *
     * @param estimatedParameterCount parameter counter
     * @return prepare result
     * @throws SQLException if any error occur
     */
    private PrepareResult executeBatchStandard(int estimatedParameterCount) throws SQLException {
    int totalExecutionNumber = getTotalExecutionNumber();
    SQLException exception = null;
    BulkStatus status = new BulkStatus();

    ComStmtPrepare comStmtPrepare = null;
    FutureTask<AsyncMultiReadResult> futureReadTask = null;

    int requestNumberByBulk;
    int paramCount = estimatedParameterCount;
    try {
      do {
        status.sendEnded = false;
        status.sendSubCmdCounter = 0;
        requestNumberByBulk =
            Math.min(
                totalExecutionNumber - status.sendCmdCounter,
                protocol.getOptions().useBatchMultiSendNumber);
        protocol.changeSocketTcpNoDelay(false); // enable NAGLE algorithm temporary.

        // add prepare sub-command
        if (readPrepareStmtResult && prepareResult == null) {

          comStmtPrepare = new ComStmtPrepare(protocol, sql);
          comStmtPrepare.send(writer);

          // read prepare result
          prepareResult = comStmtPrepare.read(protocol.getReader(), protocol.isEofDeprecated());
          statementId = ((ServerPrepareResult) prepareResult).getStatementId();
          paramCount = getParamCount();
        }

        boolean useCurrentThread = false;

        for (; status.sendSubCmdCounter < requestNumberByBulk; ) {
          sendCmd(writer, results, parametersList, queries, paramCount, status, prepareResult);
          status.sendSubCmdCounter++;
          status.sendCmdCounter++;
          if (useCurrentThread) {
            try {
              protocol.getResult(results);
            } catch (SQLException qex) {
              if (((readPrepareStmtResult && prepareResult == null)
                  || !protocol.getOptions().continueBatchOnError)) {
                throw qex;
              } else {
                exception = qex;
              }
            }
          } else if (futureReadTask == null) {
            try {
              futureReadTask =
                  new FutureTask<>(
                      new AsyncMultiRead(
                          comStmtPrepare,
                          status,
                          protocol,
                          false,
                          this,
                          paramCount,
                          results,
                          parametersList,
                          queries,
                          prepareResult));
              readScheduler.execute(futureReadTask);
            } catch (RejectedExecutionException r) {
              useCurrentThread = true;
              try {
                protocol.getResult(results);
              } catch (SQLException qex) {
                if (((readPrepareStmtResult && prepareResult == null)
                    || !protocol.getOptions().continueBatchOnError)) {
                  throw qex;
                } else {
                  exception = qex;
                }
              }
            }
          }
        }

        status.sendEnded = true;
        if (!useCurrentThread) {
          protocol.changeSocketTcpNoDelay(protocol.getOptions().tcpNoDelay);
          try {
            AsyncMultiReadResult asyncMultiReadResult = futureReadTask.get();

            if (binaryProtocol
                && prepareResult == null
                && asyncMultiReadResult.getPrepareResult() != null) {
              prepareResult = asyncMultiReadResult.getPrepareResult();
              statementId = ((ServerPrepareResult) prepareResult).getStatementId();
              paramCount = prepareResult.getParamCount();
            }

            if (asyncMultiReadResult.getException() != null) {
              if (((readPrepareStmtResult && prepareResult == null)
                  || !protocol.getOptions().continueBatchOnError)) {
                throw asyncMultiReadResult.getException();
              } else {
                exception = asyncMultiReadResult.getException();
              }
            }
          } catch (ExecutionException executionException) {
            if (executionException.getCause() == null) {
              throw new SQLException("Error reading results " + executionException.getMessage());
            }
            throw new SQLException(
                "Error reading results " + executionException.getCause().getMessage());
          } catch (InterruptedException interruptedException) {
            protocol.setActiveFutureTask(futureReadTask);
            Thread.currentThread().interrupt();
            throw new SQLException(
                "Interrupted awaiting response ",
                SqlStates.INTERRUPTED_EXCEPTION.getSqlState(),
                interruptedException);
          } finally {
            // bulk can prepare, and so if prepare cache is enable, can replace an already cached
            // prepareStatement
            // this permit to release those old prepared statement without conflict.
            protocol.forceReleaseWaitingPrepareStatement();
          }
        }

        if (protocol.isInterrupted()) {
          // interrupted during read, must throw an exception manually
          futureReadTask.cancel(true);
          throw new SQLTimeoutException("Timeout during batch execution");
        }
        futureReadTask = null;

      } while (status.sendCmdCounter < totalExecutionNumber);

      if (exception != null) {
        throw exception;
      }

      return prepareResult;

    } catch (IOException e) {
      status.sendEnded = true;
      status.sendCmdCounter = 0; // to ensure read doesn't hang
      throw protocol.handleIoException(e);
    }
  }

    public Protocol getProtocol() {
        return protocol;
    }
}
