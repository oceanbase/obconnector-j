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
package com.oceanbase.jdbc.internal.com.send;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import com.oceanbase.jdbc.internal.com.Packet;
import com.oceanbase.jdbc.internal.com.send.parameters.ParameterHolder;
import com.oceanbase.jdbc.internal.io.output.PacketOutputStream;
import com.oceanbase.jdbc.internal.util.dao.ClientPrepareResult;

public class ComQuery {

    /**
     * Client-side PrepareStatement.execute() packet send.
     *
     * @param out outputStream
     * @param clientPrepareResult clientPrepareResult
     * @param parameters parameter
     * @param queryTimeout query timeout
     * @throws IOException if connection fail
     */
    public static void sendSubCmd(final PacketOutputStream out,
                                  final ClientPrepareResult clientPrepareResult,
                                  ParameterHolder[] parameters, int queryTimeout)
            throws IOException, SQLException {
        out.startPacket(0);
        out.write(Packet.COM_QUERY);
        if (queryTimeout > 0) {
            out.write(("SET STATEMENT max_statement_time=" + queryTimeout + " FOR ").getBytes(out
                .getCharset()));
        }
        if (clientPrepareResult.isRewriteType()) {

            out.write(clientPrepareResult.getQueryParts().get(0));
            out.write(clientPrepareResult.getQueryParts().get(1));
            for (int i = 0; i < clientPrepareResult.getParamCount(); i++) {
                addParenthesesArroundValue(out, parameters[i]);
                out.write(clientPrepareResult.getQueryParts().get(i + 2));
            }
            out.write(clientPrepareResult.getQueryParts().get(
                clientPrepareResult.getParamCount() + 2));

        } else {

            out.write(clientPrepareResult.getQueryParts().get(0));
            for (int i = 0; i < clientPrepareResult.getParamCount(); i++) {
                addParenthesesArroundValue(out, parameters[i]);
                out.write(clientPrepareResult.getQueryParts().get(i + 1));
            }
        }
        out.flush();
    }

    /**
     * Client side PreparedStatement.executeBatch values rewritten (concatenate value params according
     * to max_allowed_packet)
     *
     * @param pos outputStream
     * @param queryParts query parts
     * @param currentIndex currentIndex
     * @param paramCount parameter pos
     * @param parameterList parameter list
     * @param rewriteValues is query rewritable by adding values
     * @return current index
     * @throws IOException if connection fail
     */
    public static int sendRewriteCmd(final PacketOutputStream pos, final List<byte[]> queryParts,
                                     int currentIndex, int paramCount,
                                     List<ParameterHolder[]> parameterList, boolean rewriteValues)
            throws IOException, SQLException {
        pos.startPacket(0);
        pos.write(Packet.COM_QUERY);
        int index = currentIndex;
        ParameterHolder[] parameters = parameterList.get(index++);

        byte[] firstPart = queryParts.get(0);
        byte[] secondPart = queryParts.get(1);

        if (!rewriteValues) {
            // write first
            pos.write(firstPart, 0, firstPart.length);
            pos.write(secondPart, 0, secondPart.length);

            int staticLength = 1;
            for (byte[] queryPart : queryParts) {
                staticLength += queryPart.length;
            }

            for (int i = 0; i < paramCount; i++) {
                addParenthesesArroundValue(pos, parameters[i]);
                pos.write(queryParts.get(i + 2));
            }
            pos.write(queryParts.get(paramCount + 2));

            // write other, separate by ";"
            while (index < parameterList.size()) {
                parameters = parameterList.get(index);

                // check packet length so to separate in multiple packet
                int parameterLength = 0;
                boolean knownParameterSize = true;
                for (ParameterHolder parameter : parameters) {
                    int paramSize = parameter.getApproximateTextProtocolLength();
                    if (paramSize == -1) {
                        knownParameterSize = false;
                        break;
                    }
                    parameterLength += paramSize;
                }

                if (knownParameterSize) {
                    // We know the additional query part size. This permit :
                    // - to resize buffer size if needed (to avoid resize test every write)
                    // - if this query will be separated in a new packet.
                    if (pos.checkRemainingSize(staticLength + parameterLength)) {
                        pos.write((byte) ';');
                        pos.write(firstPart, 0, firstPart.length);
                        pos.write(secondPart, 0, secondPart.length);
                        for (int i = 0; i < paramCount; i++) {
                            addParenthesesArroundValue(pos, parameters[i]);
                            pos.write(queryParts.get(i + 2));
                        }
                        pos.write(queryParts.get(paramCount + 2));
                        index++;
                    } else {
                        break;
                    }
                } else {
                    // we cannot know the additional query part size.
                    pos.write(';');
                    pos.write(firstPart, 0, firstPart.length);
                    pos.write(secondPart, 0, secondPart.length);
                    for (int i = 0; i < paramCount; i++) {
                        addParenthesesArroundValue(pos, parameters[i]);
                        pos.write(queryParts.get(i + 2));
                    }
                    pos.write(queryParts.get(paramCount + 2));
                    index++;
                    break;
                }
            }

        } else {
            pos.write(firstPart, 0, firstPart.length);
            pos.write(secondPart, 0, secondPart.length);
            int lastPartLength = queryParts.get(paramCount + 2).length;
            int intermediatePartLength = queryParts.get(1).length;

            for (int i = 0; i < paramCount; i++) {
                addParenthesesArroundValue(pos, parameters[i]);
                pos.write(queryParts.get(i + 2));
                intermediatePartLength += queryParts.get(i + 2).length;
            }

            while (index < parameterList.size()) {
                parameters = parameterList.get(index);

                // check packet length so to separate in multiple packet
                int parameterLength = 0;
                boolean knownParameterSize = true;
                for (ParameterHolder parameter : parameters) {
                    int paramSize = parameter.getApproximateTextProtocolLength();
                    if (paramSize == -1) {
                        knownParameterSize = false;
                        break;
                    }
                    parameterLength += paramSize;
                }

                if (knownParameterSize) {
                    // We know the additional query part size. This permit :
                    // - to resize buffer size if needed (to avoid resize test every write)
                    // - if this query will be separated in a new packet.
                    if (pos.checkRemainingSize(1 + parameterLength + intermediatePartLength
                                               + lastPartLength)) {
                        pos.write((byte) ',');
                        pos.write(secondPart, 0, secondPart.length);

                        for (int i = 0; i < paramCount; i++) {
                            addParenthesesArroundValue(pos, parameters[i]);
                            byte[] addPart = queryParts.get(i + 2);
                            pos.write(addPart, 0, addPart.length);
                        }
                        index++;
                    } else {
                        break;
                    }
                } else {
                    pos.write((byte) ',');
                    pos.write(secondPart, 0, secondPart.length);

                    for (int i = 0; i < paramCount; i++) {
                        addParenthesesArroundValue(pos, parameters[i]);
                        pos.write(queryParts.get(i + 2));
                    }
                    index++;
                    break;
                }
            }
            pos.write(queryParts.get(paramCount + 2));
        }

        pos.flush();
        return index;
    }

    /**
     * Statement.executeBatch() rewritten multiple (concatenate with ";") according to
     * max_allowed_packet)
     *
     * @param writer outputstream
     * @param firstQuery first query
     * @param queries queries
     * @param currentIndex currentIndex
     * @return current index
     * @throws IOException if connection error occur
     */
    public static int sendBatchAggregateSemiColon(final PacketOutputStream writer,
                                                  String firstQuery, List<String> queries,
                                                  int currentIndex) throws IOException {
        writer.startPacket(0);
        writer.write(Packet.COM_QUERY);
        // index is already set to 1 for first one
        writer.write(firstQuery.getBytes(writer.getCharset()));

        int index = currentIndex;

        // add query with ";"
        while (index < queries.size()) {
            byte[] sqlByte = queries.get(index).getBytes(writer.getCharset());
            if (!writer.checkRemainingSize(sqlByte.length + 1)) {
                break;
            }
            writer.write(';');
            writer.write(sqlByte);
            index++;
        }

        writer.flush();
        return index;
    }

    /**
     * Send directly to socket the sql data.
     *
     * @param pos output stream
     * @param sqlBytes the query in UTF-8 bytes
     * @throws IOException if connection error occur
     */
    public static void sendDirect(final PacketOutputStream pos, byte[] sqlBytes) throws IOException {
        pos.startPacket(0);
        pos.write(Packet.COM_QUERY);
        pos.write(sqlBytes);
        pos.flush();
    }

    /**
     * Send directly to socket the sql data.
     *
     * @param pos output stream
     * @param sqlBytes the query in UTF-8 bytes
     * @param queryTimeout timeout using max_statement_time
     * @throws IOException if connection error occur
     */
    public static void sendDirect(final PacketOutputStream pos, byte[] sqlBytes, int queryTimeout)
                                                                                                  throws IOException {
        pos.startPacket(0);
        pos.write(Packet.COM_QUERY);
        if (queryTimeout > 0) {
            pos.write(("SET STATEMENT max_statement_time=" + queryTimeout + " FOR ").getBytes(pos
                .getCharset()));
        }
        pos.write(sqlBytes);
        pos.flush();
    }

    /**
     * Send directly to socket the sql data.
     *
     * @param pos output stream
     * @param sqlBytes the query in UTF-8 bytes
     * @throws IOException if connection error occur
     */
    public static void sendMultiDirect(final PacketOutputStream pos, List<byte[]> sqlBytes)
                                                                                           throws IOException {
        pos.startPacket(0);
        pos.write(Packet.COM_QUERY);
        for (byte[] bytes : sqlBytes) {
            pos.write(bytes);
        }
        pos.flush();
    }

    /**
     * Send directly to socket the sql data.
     *
     * @param pos output stream
     * @param sqlBytes the query in UTF-8 bytes
     * @param queryTimeout timeout using max_statement_time
     * @throws IOException if connection error occur
     */
    public static void sendMultiDirect(final PacketOutputStream pos, List<byte[]> sqlBytes,
                                       int queryTimeout) throws IOException {
        pos.startPacket(0);
        pos.write(Packet.COM_QUERY);
        pos.write(("SET STATEMENT max_statement_time=" + queryTimeout + " FOR ").getBytes(pos
            .getCharset()));
        for (byte[] bytes : sqlBytes) {
            pos.write(bytes);
        }
        pos.flush();
    }

    /**
     * Use encloseParamInParentheses to determine whether to add parentheses
     * starting from 2.4.10, add spaces before parameters
     * @param pos output stream
     * @param parameter the value
     * @throws IOException
     * @throws SQLException
     */
    public static void addParenthesesArroundValue(PacketOutputStream pos, ParameterHolder parameter) throws IOException, SQLException {
        if (pos.isOracleMode() && pos.isEncloseParamInParentheses()) {
            pos.write(" ".getBytes(pos.getCharset()));
        }
        parameter.writeTo(pos);
    }
}
