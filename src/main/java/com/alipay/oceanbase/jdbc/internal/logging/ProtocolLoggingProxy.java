package com.alipay.oceanbase.jdbc.internal.logging;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.List;

import com.alipay.oceanbase.jdbc.internal.com.send.parameters.ParameterHolder;
import com.alipay.oceanbase.jdbc.internal.util.dao.ClientPrepareResult;
import com.alipay.oceanbase.jdbc.internal.util.dao.PrepareResult;
import com.alipay.oceanbase.jdbc.internal.util.dao.ServerPrepareResult;
import com.alipay.oceanbase.jdbc.internal.protocol.Protocol;
import com.alipay.oceanbase.jdbc.util.Options;

public class ProtocolLoggingProxy implements InvocationHandler {

    private static final Logger logger = LoggerFactory.getLogger(ProtocolLoggingProxy.class);
    private final NumberFormat  numberFormat;
    private final boolean       profileSql;
    private final Long          slowQueryThresholdNanos;
    private final int           maxQuerySizeToLog;
    private final Protocol      protocol;

    /**
     * Constructor. Will create a proxy around protocol to log queries.
     *
     * @param protocol protocol to proxy
     * @param options options
     */
    public ProtocolLoggingProxy(Protocol protocol, Options options) {
        this.protocol = protocol;
        this.profileSql = options.profileSql;
        this.slowQueryThresholdNanos = options.slowQueryThresholdNanos;
        this.maxQuerySizeToLog = options.maxQuerySizeToLog;
        this.numberFormat = DecimalFormat.getInstance();
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        try {

            switch (method.getName()) {
                case "executeQuery":
                case "executePreparedQuery":
                case "executeBatchStmt":
                case "executeBatchClient":
                case "executeBatchServer":
                    final long startTime = System.nanoTime();
                    Object returnObj = method.invoke(protocol, args);
                    if (logger.isInfoEnabled()
                        && (profileSql || (slowQueryThresholdNanos != null && System.nanoTime()
                                                                              - startTime > slowQueryThresholdNanos))) {

                        String sql = logQuery(method.getName(), args);
                        logger
                            .info("conn={}({}) - {} ms - Query: {}", protocol.getServerThreadId(),
                                protocol.isMasterConnection() ? "M" : "S", numberFormat
                                    .format(((double) System.nanoTime() - startTime) / 1000000),
                                subQuery(sql));
                    }
                    return returnObj;

                default:
                    return method.invoke(protocol, args);
            }

        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }

    @SuppressWarnings("unchecked")
    private String logQuery(String methodName, Object[] args) {
        switch (methodName) {
            case "executeQuery":
                switch (args.length) {
                    case 1:
                        return (String) args[0];
                    case 3:
                        return (String) args[2];
                    case 4:
                    case 5:
                        if (args[3] instanceof Charset) {
                            return (String) args[2];
                        }
                        ClientPrepareResult clientPrepareResult = (ClientPrepareResult) args[2];
                        return getQueryFromPrepareParameters(clientPrepareResult,
                            (ParameterHolder[]) args[3], clientPrepareResult.getParamCount());
                    default:
                        // no default
                }
                break;

            case "executeBatchClient":
                ClientPrepareResult clientPrepareResult = (ClientPrepareResult) args[2];
                return getQueryFromPrepareParameters(clientPrepareResult.getSql(),
                    (List<ParameterHolder[]>) args[3], clientPrepareResult.getParamCount());

            case "executeBatchStmt":
                List<String> multipleQueries = (List<String>) args[2];
                if (multipleQueries.size() == 1) {
                    return multipleQueries.get(0);
                } else {
                    StringBuilder sb = new StringBuilder();
                    for (String multipleQuery : multipleQueries) {
                        if (maxQuerySizeToLog > 0
                            && (sb.length() + multipleQuery.length() + 1) > maxQuerySizeToLog) {
                            sb.append(multipleQuery, 1,
                                Math.max(1, maxQuerySizeToLog - sb.length()));
                            break;
                        }
                        sb.append(multipleQuery).append(";");
                        if (maxQuerySizeToLog > 0 && sb.length() >= maxQuerySizeToLog) {
                            break;
                        }
                    }
                    return sb.toString();
                }

            case "executeBatchServer":
                List<ParameterHolder[]> parameterList = (List<ParameterHolder[]>) args[4];
                ServerPrepareResult serverPrepareResult = (ServerPrepareResult) args[1];
                return getQueryFromPrepareParameters(serverPrepareResult.getSql(), parameterList,
                    serverPrepareResult.getParamCount());

            case "executePreparedQuery":
                ServerPrepareResult prepareResult = (ServerPrepareResult) args[1];
                if (args[3] instanceof ParameterHolder[]) {
                    return getQueryFromPrepareParameters(prepareResult,
                        (ParameterHolder[]) args[3], prepareResult.getParamCount());
                } else {
                    return getQueryFromPrepareParameters(prepareResult.getSql(),
                        (List<ParameterHolder[]>) args[3], prepareResult.getParameters().length);
                }

            default:
                // no default
        }
        return "-unknown-";
    }

    /**
     * Get query, truncated if to big.
     *
     * @param sql current query
     * @return possibly truncated query if too big
     */
    public String subQuery(String sql) {
        if (maxQuerySizeToLog > 0 && sql.length() > maxQuerySizeToLog - 3) {
            return sql.substring(0, maxQuerySizeToLog - 3) + "...";
        }
        return sql;
    }

    private String getQueryFromPrepareParameters(String sql, List<ParameterHolder[]> parameterList,
                                                 int parameterLength) {

        if (parameterLength == 0) {
            return sql;
        } else {
            StringBuilder sb = new StringBuilder(sql).append(", parameters ");
            for (int paramNo = 0; paramNo < parameterList.size(); paramNo++) {
                ParameterHolder[] parameters = parameterList.get(paramNo);

                if (paramNo != 0) {
                    sb.append(",");
                }
                sb.append("[");
                for (int i = 0; i < parameterLength; i++) {
                    if (i != 0) {
                        sb.append(",");
                    }
                    sb.append(parameters[i].toString());
                }
                if (maxQuerySizeToLog > 0 && sb.length() > maxQuerySizeToLog) {
                    break;
                } else {
                    sb.append("]");
                }
            }
            return sb.toString();
        }
    }

    private String getQueryFromPrepareParameters(PrepareResult serverPrepareResult,
                                                 ParameterHolder[] paramHolders, int parameterLength) {
        StringBuilder sb = new StringBuilder(serverPrepareResult.getSql());
        if (paramHolders.length > 0) {
            sb.append(", parameters [");
            for (int i = 0; i < parameterLength; i++) {
                if (i != 0) {
                    sb.append(",");
                }
                sb.append(paramHolders[i].toString());
                if (maxQuerySizeToLog > 0 && sb.length() > maxQuerySizeToLog) {
                    break;
                }
            }
            return sb.append("]").toString();
        }
        return serverPrepareResult.getSql();
    }
}
