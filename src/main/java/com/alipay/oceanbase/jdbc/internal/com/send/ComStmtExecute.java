package com.alipay.oceanbase.jdbc.internal.com.send;

import java.io.IOException;

import com.alipay.oceanbase.jdbc.internal.com.Packet;
import com.alipay.oceanbase.jdbc.internal.com.send.parameters.ParameterHolder;
import com.alipay.oceanbase.jdbc.internal.ColumnType;
import com.alipay.oceanbase.jdbc.internal.io.output.PacketOutputStream;
import com.alipay.oceanbase.jdbc.internal.protocol.Protocol;

public class ComStmtExecute {

    /**
     * Write COM_STMT_EXECUTE sub-command to output buffer.
     *
     * @param statementId prepareResult object received after preparation.
     * @param parameters parameters
     * @param parameterCount parameters number
     * @param parameterTypeHeader parameters header1
     * @param pos outputStream
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
    public static void writeCmd(final int statementId, final ParameterHolder[] parameters,
                                final int parameterCount, ColumnType[] parameterTypeHeader,
                                final PacketOutputStream pos, final byte cursorFlag,
                                Protocol protocol) throws IOException {
        pos.write(Packet.COM_STMT_EXECUTE);
        pos.writeInt(statementId);
        pos.write(cursorFlag);
        if (protocol.versionGreaterOrEqual(4, 1, 2)) {
            if (protocol.isOracleMode() && protocol.getOptions().useServerPsStmtChecksum) {
                pos.writeInt((int) protocol.getChecksum());
            } else {
                pos.writeInt(1);
            }
        } else {
            pos.writeInt(1); //Iteration pos
        }
        // create null bitmap
        if (parameterCount > 0) {
            int nullCount = (parameterCount + 7) / 8;

            byte[] nullBitsBuffer = new byte[nullCount];
            for (int i = 0; i < parameterCount; i++) {
                if (parameters[i].isNullData()) {
                    nullBitsBuffer[i / 8] |= (1 << (i % 8));
                }
            }
            pos.write(nullBitsBuffer, 0, nullCount);

            // check if parameters type (using setXXX) have change since previous request, and resend new
            // header type if so
            boolean mustSendHeaderType = false;
            if (parameterTypeHeader[0] == null) {
                mustSendHeaderType = true;
            } else {
                for (int i = 0; i < parameterCount; i++) {
                    if (!parameterTypeHeader[i].equals(parameters[i].getColumnType())) {
                        mustSendHeaderType = true;
                        break;
                    }
                }
            }

            if (mustSendHeaderType) {
                pos.write((byte) 0x01);
                // Store types of parameters in first in first package that is sent to the server.
                for (int i = 0; i < parameterCount; i++) {
                    parameterTypeHeader[i] = parameters[i].getColumnType();
                    pos.writeShort(parameterTypeHeader[i].getType());
                }
            } else {
                pos.write((byte) 0x00);
            }
        }

        for (int i = 0; i < parameterCount; i++) {
            ParameterHolder holder = parameters[i];
            if (!holder.isNullData() && !holder.isLongData()) {
                holder.writeBinary(pos);
            }
        }
    }

    /**
     * Send a prepare statement binary stream.
     *
     * @param pos database socket
     * @param statementId prepareResult object received after preparation.
     * @param parameters parameters
     * @param parameterCount parameters number
     * @param parameterTypeHeader parameters header
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
    public static void send(final PacketOutputStream pos, final int statementId,
                            final ParameterHolder[] parameters, final int parameterCount,
                            ColumnType[] parameterTypeHeader, byte cursorFlag, Protocol protocol)
                                                                                                 throws IOException {
        pos.startPacket(0);
        writeCmd(statementId, parameters, parameterCount, parameterTypeHeader, pos, cursorFlag,
            protocol);
        pos.flush();
    }
}
