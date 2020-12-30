package com.alipay.oceanbase.jdbc.internal.util;

import com.alipay.oceanbase.jdbc.internal.com.send.parameters.ParameterHolder;
import com.alipay.oceanbase.jdbc.internal.util.dao.PrepareResult;
import com.alipay.oceanbase.jdbc.util.Options;

public class LogQueryTool {

    /**
     * Return exception message with query.
     *
     * @param serverPrepareResult prepare result
     * @param parameters query parameters
     * @param options options
     * @return exception message with query
     */
    public static String queryWithParams(PrepareResult serverPrepareResult,
                                         ParameterHolder[] parameters, Options options) {
        if (options.dumpQueriesOnException && serverPrepareResult != null) {
            StringBuilder sql = new StringBuilder(serverPrepareResult.getSql());
            if (serverPrepareResult.getParamCount() > 0 && parameters != null) {
                sql.append(", parameters [");
                if (parameters.length > 0) {
                    for (int i = 0; i < Math.min(parameters.length,
                        serverPrepareResult.getParamCount()); i++) {
                        sql.append(parameters[i].toString()).append(",");
                    }
                    sql = new StringBuilder(sql.substring(0, sql.length() - 1));
                }
                sql.append("]");
            }

            if (options.maxQuerySizeToLog != 0 && sql.length() > options.maxQuerySizeToLog - 3) {
                return sql.substring(0, options.maxQuerySizeToLog - 3) + "...";
            } else {
                return sql.toString();
            }
        }
        return null;
    }
}
