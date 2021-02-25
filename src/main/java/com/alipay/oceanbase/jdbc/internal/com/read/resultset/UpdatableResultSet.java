package com.alipay.oceanbase.jdbc.internal.com.read.resultset;

import com.alipay.oceanbase.jdbc.JDBC4UpdatableResultSet;
import com.alipay.oceanbase.jdbc.internal.com.read.dao.Results;
import com.alipay.oceanbase.jdbc.internal.io.input.PacketInputStream;
import com.alipay.oceanbase.jdbc.internal.protocol.Protocol;

import java.io.IOException;
import java.sql.SQLException;

public class UpdatableResultSet extends JDBC4UpdatableResultSet {
    /**
     * Constructor.
     *
     * @param columnsInformation column information
     * @param results            results
     * @param protocol           current protocol
     * @param reader             stream fetcher
     * @param callableResult     is it from a callableStatement ?
     * @param eofDeprecated      is EOF deprecated
     * @param isPsOutParamter
     * @throws IOException  if any connection error occur
     * @throws SQLException if any connection error occur
     */
    public UpdatableResultSet(ColumnDefinition[] columnsInformation, Results results,
                              Protocol protocol, PacketInputStream reader, boolean callableResult,
                              boolean eofDeprecated, boolean isPsOutParamter) throws IOException,
                                                                             SQLException {
        super(columnsInformation, results, protocol, reader, callableResult, eofDeprecated,
            isPsOutParamter);
    }
}
