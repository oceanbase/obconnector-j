package com.oceanbase.jdbc.internal.com.send;

import java.io.IOException;
import java.sql.SQLException;

import com.oceanbase.jdbc.internal.ColumnType;
import com.oceanbase.jdbc.internal.com.Packet;
import com.oceanbase.jdbc.internal.com.read.Buffer;
import com.oceanbase.jdbc.internal.com.read.dao.Results;
import com.oceanbase.jdbc.internal.com.read.resultset.ColumnDefinition;
import com.oceanbase.jdbc.internal.com.send.parameters.OBArrayParameter;
import com.oceanbase.jdbc.internal.com.send.parameters.OBStringParameter;
import com.oceanbase.jdbc.internal.com.send.parameters.OBStructParameter;
import com.oceanbase.jdbc.internal.com.send.parameters.ParameterHolder;
import com.oceanbase.jdbc.internal.io.input.PacketInputStream;
import com.oceanbase.jdbc.internal.io.output.PacketOutputStream;
import com.oceanbase.jdbc.internal.protocol.Protocol;
import com.oceanbase.jdbc.internal.util.dao.ServerPrepareResult;

/**
 * @author yexinyan.yxy
 * @version 1.0: ComStmtPrepareExecute, 2021-09-06 16:09 yexinyan.yxy Exp $
 */
public class ComStmtPrepareExecute {
    /**
     * Send a prepare statement binary stream.
     *
     * @param pos database socket
     * @param results prepareResult object received after preparation.
     * @param parameterCount parameters number
     * @param parameterTypeHeader parameters header received from last response
     * @param cursorFlag cursor flag. Possible values :
     *     <ol>
     *       <li>CURSOR_TYPE_NO_CURSOR = fetch all
     *       <li>CURSOR_TYPE_READ_ONLY = fetch by bunch
     *       <li>CURSOR_TYPE_FOR_UPDATE = fetch by bunch with lock ?
     *       <li>CURSOR_TYPE_SCROLLABLE = //reserved, but not working
     *     </ol>
     *
     * @throws IOException if a connection error occur
     */
    public static void send(final PacketOutputStream pos, Results results,
                            final int parameterCount, ParameterHolder[] parameters,
                            ColumnType[] parameterTypeHeader, byte cursorFlag, Protocol protocol,
                            ServerPrepareResult serverPrepareResult) throws IOException,
                                                                    SQLException {
        pos.startPacket(0);
        pos.write(Packet.COM_STMT_PREPARE_EXECUTE);
        if (serverPrepareResult == null) {
            pos.writeInt(results.getStatementId());
        } else {
            pos.writeInt(serverPrepareResult.getStatementId());
        }
        pos.write(cursorFlag);
        pos.writeInt(protocol.getIterationCount());
        ParameterHolder lengthEncodedString = new OBStringParameter(results.getStatement()
            .getOriginalSql(), protocol.noBackslashEscapes(),
            protocol.getOptions().characterEncoding);
        lengthEncodedString.writeBinary(pos);
        pos.writeInt(parameterCount);

        if (parameterCount > 0) {
            // create null-bitmap
            int nullCount = (parameterCount + 7) / 8;
            byte[] nullBitsBuffer = new byte[nullCount];

            for (int i = 0; i < parameterCount; i++) {
                if (parameters[i].isNullData()) {
                    nullBitsBuffer[i / 8] |= (1 << (i % 8));
                }
            }
            pos.write(nullBitsBuffer, 0, nullCount);

            // create new-params-bound-flag
            // check if parameters type (using setXXX) have change since previous request,
            // and resend new header type if so
            int mustSendHeaderType = 0;
            if (parameterTypeHeader == null || parameterTypeHeader[0] == null) {
                mustSendHeaderType = 1;
            } else {
                for (int i = 0; i < parameterCount; i++) {
                    if (!parameterTypeHeader[i].equals(parameters[i].getColumnType())) {
                        mustSendHeaderType = 1;
                        break;
                    }
                }
            }
            pos.write((byte) mustSendHeaderType);

            // send parameter type
            if (mustSendHeaderType == 1) {
                // Store types of parameters in first package that is sent to the server.
                for (int i = 0; i < parameterCount; i++) {
                    if (parameterTypeHeader == null) {
                        pos.writeShort(parameters[i].getColumnType().getType());
                    } else {
                        parameterTypeHeader[i] = parameters[i].getColumnType();
                        pos.writeShort(parameterTypeHeader[i].getType());
                    }

                    if (parameters[i].getColumnType().getType() == ColumnType.COMPLEX.getType()) {
                        Object obj = parameters[i];
                        if (obj instanceof OBArrayParameter) {
                            ((OBArrayParameter) obj).storeArrayTypeInfo(pos);
                        } else if (obj instanceof OBStructParameter) {
                            ((OBStructParameter) obj).storeStructTypeInfo(pos);
                        } else {
                            throw new SQLException(
                                "complex param type is not supported， only array is supported");
                        }
                    }
                }
            }

            // send parameter value
            for (int i = 0; i < parameterCount; i++) {
                ParameterHolder holder = parameters[i];
                if (!holder.isNullData() && !holder.isLongData()) {
                    holder.writeBinary(pos);
                }
            }
        }

        pos.writeInt(protocol.getExecuteMode());
        // TODO: if num-close-stmt-count > 0
        pos.writeInt(0);
        pos.writeInt((int) protocol.getChecksum());
        pos.writeInt(0);

        pos.flush();
    }

    public static ServerPrepareResult read(Protocol protocol, PacketInputStream reader,
                                           ServerPrepareResult serverPrepareResult, Results results)
                                                                                                    throws IOException,
                                                                                                    SQLException {
        boolean eofDeprecated = protocol.isEofDeprecated();
        Buffer buffer = reader.getPacket(true);
        byte firstByte = buffer.getByteAt(0);

        switch (firstByte) {
            case Packet.OK:
                buffer.skipByte();
                final int statementId = buffer.readInt();
                results.setStatementId(statementId);
                final int numColumns = buffer.readShort() & 0xffff;
                final int numParams = buffer.readIntV1();
                final byte reserved1 = buffer.readByte();
                final short warningCount = buffer.readShort();
                final int extendFlag = buffer.readInt();
                final byte hasResultSet = buffer.readByte();
                ColumnDefinition[] params = new ColumnDefinition[numParams];
                if (numParams > 0) {
                    for (int i = 0; i < numParams; i++) {
                        params[i] = new ColumnDefinition(reader.getPacket(false), true,
                            protocol.getOptions().characterEncoding);
                    }
                    if (!eofDeprecated) {
                        protocol.skipEofPacket();
                    }
                }

                ColumnDefinition[] columns = new ColumnDefinition[numColumns];
                if (hasResultSet == 1) {
                    protocol.readResultSet(columns, results);
                }

                buffer = reader.getPacket(true);
                switch (buffer.getByteAt(0)) {
                    case Packet.OK:
                        protocol.readOkPacket(buffer, results);
                        break;
                    case Packet.ERROR:
                        throw protocol.readErrorPacket(buffer, results);
                }

                return serverPrepareResult != null ? serverPrepareResult : new ServerPrepareResult(
                    results.getSql(), statementId, columns, params, protocol);

            case Packet.ERROR:
                throw protocol.readErrorPacket(buffer, results);

            default:
                throw new SQLException("Unexpected packet returned by server, first byte "
                                       + firstByte);
        }
    }
}
