package com.alipay.oceanbase.jdbc.internal.com.read.resultset;

import com.alipay.oceanbase.jdbc.JDBC4ResultSet;
import com.alipay.oceanbase.jdbc.internal.com.read.dao.Results;
import com.alipay.oceanbase.jdbc.internal.com.read.resultset.ColumnDefinition;
import com.alipay.oceanbase.jdbc.internal.io.input.PacketInputStream;
import com.alipay.oceanbase.jdbc.internal.protocol.Protocol;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

public class SelectResultSet extends JDBC4ResultSet {
    public SelectResultSet(ColumnDefinition[] columnDefinition, Results results, Protocol protocol,
                           PacketInputStream reader, boolean callableResult, boolean eofDeprecated,
                           boolean isPsOutParameter) throws IOException, SQLException {
        super(columnDefinition, results, protocol, reader, callableResult, eofDeprecated,
            isPsOutParameter);
    }

    public SelectResultSet(ColumnDefinition[] columnDefinition, Results results, Protocol protocol,
                           boolean callableResult, boolean eofDeprecated, boolean isPsOutParameter)
                                                                                                   throws IOException,
                                                                                                   SQLException {
        super(columnDefinition, results, protocol, callableResult, eofDeprecated, isPsOutParameter);
    }

    public SelectResultSet(ColumnDefinition[] columnDefinition, List<byte[]> resultSet,
                           Protocol protocol, int resultSetScrollType) {
        super(columnDefinition, resultSet, protocol, resultSetScrollType);
    }
}
