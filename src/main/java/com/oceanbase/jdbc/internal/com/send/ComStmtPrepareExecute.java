/*
 *
 * OceanBase Client for Java
 *
 * Copyright (c) 2021 OceanBase.
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 *
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * Copyright (c) 2009-2011, Marcus Eriksson
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 * Redistributions of source code must retain the above copyright notice, this list
 * of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice, this
 * list of conditions and the following disclaimer in the documentation and/or
 * other materials provided with the distribution.
 *
 * Neither the name of the driver nor the names of its contributors may not be
 * used to endorse or promote products derived from this software without specific
 * prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS  AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 * NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
 * OF SUCH DAMAGE.
 *
 */
package com.oceanbase.jdbc.internal.com.send;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import com.oceanbase.jdbc.internal.ColumnType;
import com.oceanbase.jdbc.internal.com.Packet;
import com.oceanbase.jdbc.internal.com.read.Buffer;
import com.oceanbase.jdbc.internal.com.read.dao.Results;
import com.oceanbase.jdbc.internal.com.read.resultset.ColumnDefinition;
import com.oceanbase.jdbc.internal.com.send.parameters.OBArrayParameter;
import com.oceanbase.jdbc.internal.com.send.parameters.OBVarcharParameter;
import com.oceanbase.jdbc.internal.com.send.parameters.OBStructParameter;
import com.oceanbase.jdbc.internal.com.send.parameters.ParameterHolder;
import com.oceanbase.jdbc.internal.io.input.PacketInputStream;
import com.oceanbase.jdbc.internal.io.output.PacketOutputStream;
import com.oceanbase.jdbc.internal.protocol.Protocol;
import com.oceanbase.jdbc.internal.util.dao.ServerPrepareResult;

public class ComStmtPrepareExecute {

    /***** Extend Flag *****/
    private static int NOT_RETURNING_RESULT_SET           = 0x0000;
    private static int RETURNING_RESULT_SET_WITHOUT_FIELD = 0x0001;
    private static int ARRAY_BINDING_FIELD                = 0x0002;
    private static int RETURNING_RESULT_SET_WITH_FIELD    = 0x0003;
    private static int PL_OUT_PARAMETER                   = 0x0004;

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
        pos.write(cursorFlag); // No acket.ARRAY_BINDIN required with prepareExecute
        pos.writeInt(protocol.getIterationCount());
        ParameterHolder lengthEncodedString = new OBVarcharParameter(results.getStatement()
            .getActualSql(), protocol.noBackslashEscapes(), protocol.getOptions().getCharacterEncoding());
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
                    ColumnType columnType = parameters[i].getColumnType();
                    if (parameterTypeHeader != null) {
                        parameterTypeHeader[i] = columnType;
                    }
                    pos.writeShort(columnType.getType());

                    if (columnType.getType() == ColumnType.COMPLEX.getType()) {
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
        reader.startReceiveResponse();

        try {
            boolean eofDeprecated = protocol.isEofDeprecated();
            Buffer buffer = reader.getPacket(true);
            byte firstByte = buffer.getByteAt(0);

            switch (firstByte) {
                case Packet.OK:
                    buffer.skipByte();
                    final int statementId = buffer.readInt();
                    results.setStatementId(statementId);
                    final int numColumns = buffer.readShort() & 0xffff;
                    final int numParams = buffer.readInt2BytesV1();
                    final byte reserved1 = buffer.readByte();
                    final short warningCount = buffer.readShort();
                    final int extendFlag = buffer.readInt();
                    final byte hasResultSet = buffer.readByte();
                    reader.getLogger().trace("Got header OK packet.");

                    ColumnDefinition[] params = new ColumnDefinition[numParams];
                    if (numParams > 0) {
                        for (int i = 0; i < numParams; i++) {
                            params[i] = new ColumnDefinition(reader.getPacket(false), true,
                                    protocol.getOptions().getCharacterEncoding(), protocol.getOptions());
                        }
                        if (!eofDeprecated) {
                            protocol.skipEofPacket();
                        }
                        reader.getLogger().trace("Got param definition.");
                    }

                    ColumnDefinition[] columns = new ColumnDefinition[numColumns];
                    if (hasResultSet == 1) {
                        if ((extendFlag & RETURNING_RESULT_SET_WITHOUT_FIELD) != 0
                                || (extendFlag & ARRAY_BINDING_FIELD) != 0
                                || (extendFlag & PL_OUT_PARAMETER) != 0) {
                            results.setInternalResult(true);
                        }
                        results.setToPrepareExecute(true);
                        results.setReturning((extendFlag & RETURNING_RESULT_SET_WITHOUT_FIELD) != 0);

                        protocol.readResultSet(columns, results);

                        results.setToPrepareExecute(false);
                        results.setInternalResult(false);
                    }

                    buffer = reader.getPacket(false);
                    switch (buffer.getByteAt(0)) {
                        case Packet.OK:
                            protocol.readOkPacket(buffer, results);
                            reader.getLogger().trace("Got tail OK packet.");
                            break;
                        case Packet.ERROR:
                            reader.getLogger().trace("Got tail ERROR packet.");
                            throw protocol.readErrorPacket(buffer, results);
                        default:
                            reader.getLogger().error(
                                "Got unexpected tail packet returned by server, first byte "
                                        + firstByte);
                            throw new SQLException(
                                "Unexpected tail packet returned by server, first byte "
                                        + firstByte);
                    }

                    if (serverPrepareResult == null) {
                        String sql = results.getSql();
                        serverPrepareResult = new ServerPrepareResult(sql, statementId,
                                columns, params, protocol);
                        serverPrepareResult.setReturnByPrepareExecute(true);
                        if (protocol.getOptions().cachePrepStmts && protocol.getOptions().useServerPrepStmts
                                && sql != null && sql.length() < protocol.getOptions().prepStmtCacheSqlLimit) {
                            String key = protocol.getDatabase() + "-" + sql;
                            ServerPrepareResult cachedServerPrepareResult = protocol.addPrepareInCache(key,
                                    serverPrepareResult);
                            return cachedServerPrepareResult != null ? cachedServerPrepareResult
                                    : serverPrepareResult;
                        }
                    } else {
                        serverPrepareResult.setStatementId(statementId);
                    }
                    return serverPrepareResult;

                case Packet.ERROR:
                    reader.getLogger().trace("Got header ERROR packet.");
                    throw protocol.readErrorPacket(buffer, results);

                default:
                    reader.getLogger().error(
                        "Got unexpected header packet returned by server, first byte " + firstByte);
                    throw new SQLException(
                        "Unexpected header packet returned by server, first byte " + firstByte);
            }
        } finally {
            reader.endReceiveResponse(results.getSql());
        }
    }

    public static void readTailPacket(Protocol protocol, PacketInputStream reader, Results results,
                                      boolean hasResultSet) throws IOException, SQLException {
        Buffer buffer = reader.getPacket(true);
        switch (buffer.getByteAt(0)) {
            case Packet.OK:
                if (!hasResultSet) {
                    // if batch errors or "returning ... into" is supported later,
                    // then hasResultSet flag and ok packet are meaningful both
                    protocol.readOkPacket(buffer, results);
                }
                break;
            case Packet.ERROR:
                throw protocol.readErrorPacket(buffer, results);
        }
    }

    private static void writeCmdArrayBinding(final int statementId,
                                             final List<ParameterHolder[]> queryParameters,
                                             final int queryParamtersSize,
                                             final int parameterCount,
                                             ColumnType[] parameterTypeHeader,
                                             final PacketOutputStream pos, final byte cursorFlag,
                                             Protocol protocol, Results results,
                                             ServerPrepareResult serverPrepareResult)
                                                                                     throws IOException,
                                                                                     SQLException {
        pos.write(Packet.COM_STMT_PREPARE_EXECUTE);
        if (serverPrepareResult == null) {
            pos.writeInt(results.getStatementId());
        } else {
            pos.writeInt(serverPrepareResult.getStatementId());
        }
        pos.write(cursorFlag);
        pos.writeInt(protocol.getIterationCount());
        ParameterHolder lengthEncodedString = new OBVarcharParameter(results.getStatement()
            .getActualSql(), protocol.noBackslashEscapes(), protocol.getOptions().getCharacterEncoding());
        lengthEncodedString.writeBinary(pos);
        pos.writeInt(parameterCount);
        if (parameterCount > 0) {
            int nullCount = (parameterCount + 7) / 8;
            byte[] nullBitsBuffer = new byte[nullCount];
            pos.write(nullBitsBuffer, 0, nullCount);
            boolean mustSendHeaderType = true;
            if (mustSendHeaderType) {
                pos.write((byte) 0x01);
                for (int i = 0; i < parameterCount; i++) {
                    pos.writeShort(ColumnType.COMPLEX.getType());
                    pos.writeFieldLength(0);
                    pos.writeFieldLength(0);
                    pos.writeFieldLength(0); // version
                    int elementType = queryParameters.get(0)[i].getColumnType().getType();
                    pos.writeBytes((byte) elementType, 1);
                }
            } else {
                pos.write((byte) 0x00);
            }
        }
        for (int i = 0; i < parameterCount; i++) {
            ParameterHolder curHolder = queryParameters.get(0)[i];
            pos.writeFieldLength(queryParamtersSize);
            int nullCount = (queryParamtersSize + 7) / 8;
            int nullBitsPosition = pos.getPosition();
            if (!curHolder.isLongData()) {
                for (int j = 0; j < nullCount; j++) {
                    pos.writeBytes((byte) 0, 1);
                }
            }
            byte[] nullBitsBuffer = new byte[nullCount];
            for (int j = 0; j < queryParamtersSize; ++j) {
                ParameterHolder holder = queryParameters.get(j)[i];
                if (null != holder && !holder.isLongData() && !holder.isNullData()) {
                    holder.writeBinary(pos);
                } else {
                    nullBitsBuffer[j / 8] |= (1 << (j % 8));
                }
            }
            if (!curHolder.isLongData()) {
                int endPosition = pos.getPosition();
                pos.setPosition(nullBitsPosition);
                pos.write(nullBitsBuffer); // null bits
                pos.setPosition(endPosition);
            }
        }
        pos.writeInt(protocol.getExecuteMode());
        // TODO: if num-close-stmt-count > 0
        pos.writeInt(0);
        pos.writeInt((int) protocol.getChecksum());
        pos.writeInt(0);
    }

    public static void sendArrayBinding(final PacketOutputStream pos, final int statementId,
                                        final List<ParameterHolder[]> queryParameters,
                                        final int queryParamtersSize, final int parameterCount,
                                        ColumnType[] parameterTypeHeader, byte cursorFlag,
                                        Protocol protocol, Results results,
                                        ServerPrepareResult serverPrepareResult)
                                                                                throws IOException,
                                                                                SQLException {
        pos.startPacket(0);
        writeCmdArrayBinding(statementId, queryParameters, queryParamtersSize, parameterCount,
            parameterTypeHeader, pos, cursorFlag, protocol, results, serverPrepareResult);
        pos.flush();
    }
}
