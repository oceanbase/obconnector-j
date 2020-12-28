package com.alipay.oceanbase.jdbc.internal.com.send;

import java.io.IOException;
import java.sql.SQLException;

import com.alipay.oceanbase.jdbc.internal.com.Packet;
import com.alipay.oceanbase.jdbc.internal.com.read.ErrorPacket;
import com.alipay.oceanbase.jdbc.internal.com.read.resultset.ColumnDefinition;
import com.alipay.oceanbase.jdbc.internal.com.read.Buffer;
import com.alipay.oceanbase.jdbc.internal.io.input.PacketInputStream;
import com.alipay.oceanbase.jdbc.internal.io.output.PacketOutputStream;
import com.alipay.oceanbase.jdbc.internal.protocol.Protocol;
import com.alipay.oceanbase.jdbc.internal.util.dao.ServerPrepareResult;

public class ComStmtPrepare {

    private final Protocol protocol;
    private final String   sql;

    public ComStmtPrepare(Protocol protocol, String sql) {
        this.protocol = protocol;
        this.sql = sql;
    }

    /**
     * Send directly to socket the sql data.
     *
     * @param pos the writer
     * @throws IOException if connection error occur
     */
    public void send(PacketOutputStream pos) throws IOException {
        pos.startPacket(0);
        pos.write(Packet.COM_STMT_PREPARE);
        pos.write(this.sql);
        pos.flush();
    }

    /**
     * Read COM_PREPARE_RESULT.
     *
     * @param reader inputStream
     * @param eofDeprecated are EOF_packet deprecated
     * @return ServerPrepareResult prepare result
     * @throws IOException if connection has error
     * @throws SQLException if server answer with error.
     */
    public ServerPrepareResult read(PacketInputStream reader, boolean eofDeprecated)
                                                                                    throws IOException,
                                                                                    SQLException {
        Buffer buffer = reader.getPacket(true);
        byte firstByte = buffer.getByteAt(buffer.position);

        if (firstByte == Packet.ERROR) {
            throw buildErrorException(buffer);
        }

        if (firstByte == Packet.OK) {

            /* Prepared Statement OK */
            buffer.readByte(); /* skip field count */
            final int statementId = buffer.readInt();
            final int numColumns = buffer.readShort() & 0xffff;
            final int numParams = buffer.readShort() & 0xffff;

            ColumnDefinition[] params = new ColumnDefinition[numParams];
            ColumnDefinition[] columns = new ColumnDefinition[numColumns];

            if (numParams > 0) {
                for (int i = 0; i < numParams; i++) {
                    params[i] = new ColumnDefinition(reader.getPacket(false),
                        this.protocol.isOracleMode());
                }

                if (numColumns > 0) {
                    if (!eofDeprecated) {
                        protocol.skipEofPacket();
                    }
                    for (int i = 0; i < numColumns; i++) {
                        columns[i] = new ColumnDefinition(reader.getPacket(false),
                            this.protocol.isOracleMode());
                    }
                }
                if (!eofDeprecated) {
                    protocol.readEofPacket();
                }
            } else {
                if (numColumns > 0) {
                    for (int i = 0; i < numColumns; i++) {
                        columns[i] = new ColumnDefinition(reader.getPacket(false),
                            this.protocol.isOracleMode());
                    }
                    if (!eofDeprecated) {
                        protocol.readEofPacket();
                    }
                } else {
                    // read warning only if no param / columns, because will be overwritten by EOF warning
                    // data
                    buffer.readByte(); // reserved
                    protocol.setHasWarnings(buffer.readShort() > 0);
                }
            }

            ServerPrepareResult serverPrepareResult = new ServerPrepareResult(sql, statementId,
                columns, params, protocol);
            if (protocol.getOptions().cachePrepStmts && protocol.getOptions().useServerPrepStmts
                && sql != null && sql.length() < protocol.getOptions().prepStmtCacheSqlLimit) {
                String key = protocol.getDatabase() + "-" + sql;
                ServerPrepareResult cachedServerPrepareResult = protocol.addPrepareInCache(key,
                    serverPrepareResult);
                return cachedServerPrepareResult != null ? cachedServerPrepareResult
                    : serverPrepareResult;
            }
            return serverPrepareResult;

        } else {
            throw new SQLException("Unexpected packet returned by server, first byte " + firstByte);
        }
    }

    private SQLException buildErrorException(Buffer buffer) {
        ErrorPacket ep = new ErrorPacket(buffer);
        String message = ep.getMessage();
        if (1054 == ep.getErrorCode()) {
            return new SQLException(
                message
                        + "\nIf column exists but type cannot be identified (example 'select ? `field1` from dual'). "
                        + "Use CAST function to solve this problem (example 'select CAST(? as integer) `field1` from dual')",
                ep.getSqlState(), ep.getErrorCode());
        } else {
            return new SQLException(message, ep.getSqlState(), ep.getErrorCode());
        }
    }
}
