/**
 *  OceanBase Client for Java
 *
 *  Copyright (c) 2012-2014 Monty Program Ab.
 *  Copyright (c) 2015-2020 MariaDB Corporation Ab.
 *  Copyright (c) 2021 OceanBase.
 *
 *  This library is free software; you can redistribute it and/or modify it under
 *  the terms of the GNU Lesser General Public License as published by the Free
 *  Software Foundation; either version 2.1 of the License, or (at your option)
 *  any later version.
 *
 *  This library is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 *  FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 *  for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public License along
 *  with this library; if not, write to Monty Program Ab info@montyprogram.com.
 *
 *  This particular MariaDB Client for Java file is work
 *  derived from a Drizzle-JDBC. Drizzle-JDBC file which is covered by subject to
 *  the following copyright and notice provisions:
 *
 *  Copyright (c) 2009-2011, Marcus Eriksson
 *
 *  Redistribution and use in source and binary forms, with or without modification,
 *  are permitted provided that the following conditions are met:
 *  Redistributions of source code must retain the above copyright notice, this list
 *  of conditions and the following disclaimer.
 *
 *  Redistributions in binary form must reproduce the above copyright notice, this
 *  list of conditions and the following disclaimer in the documentation and/or
 *  other materials provided with the distribution.
 *
 *  Neither the name of the driver nor the names of its contributors may not be
 *  used to endorse or promote products derived from this software without specific
 *  prior written permission.
 *
 *  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS  AND CONTRIBUTORS "AS IS"
 *  AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 *  WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 *  IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 *  INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 *  NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 *  PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 *  WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 *  ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
 *  OF SUCH DAMAGE.
 */
package com.oceanbase.jdbc.internal.util.exceptions;

import java.sql.*;

import com.oceanbase.jdbc.OceanBaseConnection;
import com.oceanbase.jdbc.util.Options;

public final class ExceptionFactory {

    public static final ExceptionFactory INSTANCE = new ExceptionFactory(-1L, null);

    private final long                   threadId;
    private final Options                options;

    private OceanBaseConnection          connection;
    private Statement                    statement;

    public ExceptionFactory(long threadId, Options options, OceanBaseConnection connection,
                            Statement statement) {
        this.threadId = threadId;
        this.options = options;
        this.connection = connection;
        this.statement = statement;
    }

    private ExceptionFactory(long threadId, Options options) {
        this.threadId = threadId;
        this.options = options;
    }

    public static ExceptionFactory of(long threadId, Options options) {
        return new ExceptionFactory(threadId, options);
    }

    private static SQLException createException(String initialMessage, String sqlState,
                                                int errorCode, long threadId, Options options,
                                                OceanBaseConnection connection,
                                                Statement statement, Exception cause) {

        String msg = buildMsgText(initialMessage, threadId, options, cause);

        if ("70100".equals(sqlState)) { // ER_QUERY_INTERRUPTED
            return new SQLTimeoutException(msg, sqlState, errorCode);
        }

        SQLException returnEx;
        String sqlClass = sqlState == null ? "42" : sqlState.substring(0, 2);
        switch (sqlClass) {
            case "0A":
                returnEx = new SQLFeatureNotSupportedException(msg, sqlState, errorCode, cause);
                break;
            case "22":
                returnEx = new OceanBaseDataTruncation(0, true, false, 0, 0, cause, msg, errorCode);
                break;
            case "26":
            case "2F":
            case "20":
            case "42":
            case "XA":
                returnEx = new SQLSyntaxErrorException(msg, sqlState, errorCode, cause);
                break;
            case "25":
            case "28":
                returnEx = new SQLInvalidAuthorizationSpecException(msg, sqlState, errorCode, cause);
                break;
            case "21":
            case "23":
                returnEx = new SQLIntegrityConstraintViolationException(msg, sqlState, errorCode,
                    cause);
                break;
            case "08":
                returnEx = new SQLNonTransientConnectionException(msg, sqlState, errorCode, cause);
                break;
            case "40":
                returnEx = new SQLTransactionRollbackException(msg, sqlState, errorCode, cause);
                break;
            default:
                returnEx = new SQLTransientConnectionException(msg, sqlState, errorCode, cause);
                break;
        }

        if (connection != null && connection.pooledConnection != null) {
            connection.pooledConnection.fireStatementErrorOccured(statement, returnEx);
        }
        return returnEx;
    }

    private static String buildMsgText(
      String initialMessage, long threadId, Options options, Exception cause) {

    StringBuilder msg = new StringBuilder();
    String deadLockException = null;
    String threadName = null;

    if (threadId != -1L) {
      msg.append("(conn=").append(threadId).append(") ").append(initialMessage);
    } else {
      msg.append(initialMessage);
    }

    if (cause instanceof OceanBaseSqlException) {
      OceanBaseSqlException exception = ((OceanBaseSqlException) cause);
      String sql = exception.getSql();
      if (options.dumpQueriesOnException && sql != null) {
        if (options != null
            && options.maxQuerySizeToLog != 0
            && sql.length() > options.maxQuerySizeToLog - 3) {
          msg.append("\nQuery is: ").append(sql, 0, options.maxQuerySizeToLog - 3).append("...");
        } else {
          msg.append("\nQuery is: ").append(sql);
        }
      }
      deadLockException = exception.getDeadLockInfo();
      threadName = exception.getThreadName();
    }

    if (options != null
        && options.includeInnodbStatusInDeadlockExceptions
        && deadLockException != null) {
      msg.append("\ndeadlock information: ").append(deadLockException);
    }

    if (options != null && options.includeThreadDumpInDeadlockExceptions) {
      if (threadName != null) {
        msg.append("\nthread name: ").append(threadName);
      }
      msg.append("\ncurrent threads: ");
      Thread.getAllStackTraces()
          .forEach(
              (thread, traces) -> {
                msg.append("\n  name:\"")
                    .append(thread.getName())
                    .append("\" pid:")
                    .append(thread.getId())
                    .append(" status:")
                    .append(thread.getState());
                for (int i = 0; i < traces.length; i++) {
                  msg.append("\n    ").append(traces[i]);
                }
              });
    }

    return msg.toString();
  }

    public ExceptionFactory raiseStatementError(OceanBaseConnection connection, Statement stmt) {
        return new ExceptionFactory(threadId, options, connection, stmt);
    }

    public SQLException create(SQLException cause) {

        return createException(cause.getMessage(), cause.getSQLState(), cause.getErrorCode(),
            threadId, options, connection, statement, cause);
    }

    public SQLException notSupported(String message) {
        return createException(message, "0A000", -1, threadId, options, connection, statement, null);
    }

    public SQLException create(String message) {
        return createException(message, "42000", -1, threadId, options, connection, statement, null);
    }

    public SQLException create(String message, Exception cause) {
        return createException(message, "42000", -1, threadId, options, connection, statement,
            cause);
    }

    public SQLException create(String message, String sqlState) {
        return createException(message, sqlState, -1, threadId, options, connection, statement,
            null);
    }

    public SQLException create(String message, String sqlState, Exception cause) {
        return createException(message, sqlState, -1, threadId, options, connection, statement,
            cause);
    }

    public SQLException create(String message, String sqlState, int errorCode) {
        return createException(message, sqlState, errorCode, threadId, options, connection,
            statement, null);
    }

    public SQLException create(String message, String sqlState, int errorCode, Exception cause) {
        return createException(message, sqlState, errorCode, threadId, options, connection,
            statement, cause);
    }

    public long getThreadId() {
        return threadId;
    }

    public Options getOptions() {
        return options;
    }

    @Override
    public String toString() {
        return "ExceptionFactory{threadId=" + threadId + '}';
    }
}
