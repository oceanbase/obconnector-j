package com.alipay.oceanbase.jdbc.internal.com.send;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import com.alipay.oceanbase.jdbc.internal.com.Packet;
import com.alipay.oceanbase.jdbc.internal.com.send.parameters.ParameterHolder;
import com.alipay.oceanbase.jdbc.internal.io.output.PacketOutputStream;
import com.alipay.oceanbase.jdbc.internal.util.dao.ClientPrepareResult;

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
                                                                                 throws IOException {
        out.write(Packet.COM_QUERY);
        if (queryTimeout > 0) {
            out.write(("SET STATEMENT max_statement_time=" + queryTimeout + " FOR ").getBytes());
        }
        if (clientPrepareResult.isRewriteType()) {

            out.write(clientPrepareResult.getQueryParts().get(0));
            out.write(clientPrepareResult.getQueryParts().get(1));
            for (int i = 0; i < clientPrepareResult.getParamCount(); i++) {
                parameters[i].writeTo(out);
                out.write(clientPrepareResult.getQueryParts().get(i + 2));
            }
            out.write(clientPrepareResult.getQueryParts().get(
                clientPrepareResult.getParamCount() + 2));

        } else {

            out.write(clientPrepareResult.getQueryParts().get(0));
            for (int i = 0; i < clientPrepareResult.getParamCount(); i++) {
                parameters[i].writeTo(out);
                out.write(clientPrepareResult.getQueryParts().get(i + 1));
            }
        }
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
                                                                                                  throws IOException {
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
                parameters[i].writeTo(pos);
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
                            parameters[i].writeTo(pos);
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
                        parameters[i].writeTo(pos);
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
                parameters[i].writeTo(pos);
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
                            parameters[i].writeTo(pos);
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
                        parameters[i].writeTo(pos);
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
        writer.write(firstQuery.getBytes(StandardCharsets.UTF_8));

        int index = currentIndex;

        // add query with ";"
        while (index < queries.size()) {
            byte[] sqlByte = queries.get(index).getBytes(StandardCharsets.UTF_8);
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
            pos.write(("SET STATEMENT max_statement_time=" + queryTimeout + " FOR ").getBytes());
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
        pos.write(("SET STATEMENT max_statement_time=" + queryTimeout + " FOR ").getBytes());
        for (byte[] bytes : sqlBytes) {
            pos.write(bytes);
        }
        pos.flush();
    }
}
