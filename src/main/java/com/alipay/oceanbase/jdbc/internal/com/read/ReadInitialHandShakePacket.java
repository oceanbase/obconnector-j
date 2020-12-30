package com.alipay.oceanbase.jdbc.internal.com.read;

import static com.alipay.oceanbase.jdbc.internal.com.Packet.ERROR;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import com.alipay.oceanbase.jdbc.internal.OceanBaseServerCapabilities;
import com.alipay.oceanbase.jdbc.internal.io.input.PacketInputStream;
import com.alipay.oceanbase.jdbc.internal.util.Utils;

public class ReadInitialHandShakePacket {

    /* MDEV-4088/CONJ-32 :  in 10.0, the real version string maybe prefixed with "5.5.5-",
     * to workaround bugs in Oracle MySQL replication
     */
    private static final String MARIADB_RPL_HACK_PREFIX  = "5.5.5-";
    private final byte          protocolVersion;
    private final long          serverThreadId;
    private final long          serverCapabilities;
    private final byte          serverLanguage;
    private final short         serverStatus;
    private final byte[]        seed;
    private String              serverVersion;
    private String              authenticationPluginType = "";
    private boolean             serverMariaDb;

    /**
     * Read database initial stream.
     *
     * @param reader packetFetcher
     * @throws IOException if a connection error occur
     * @throws SQLException if received an error packet
     */
    public ReadInitialHandShakePacket(final PacketInputStream reader) throws IOException,
                                                                     SQLException {
        Buffer buffer = reader.getPacket(true);
        if (buffer.getByteAt(0) == ERROR) {
            ErrorPacket errorPacket = new ErrorPacket(buffer);
            throw new SQLException(errorPacket.getMessage());
        }
        protocolVersion = buffer.readByte();
        serverVersion = buffer.readStringNullEnd(StandardCharsets.US_ASCII);
        // server send truncated threadId (4 byte out of 8)
        serverThreadId = buffer.readInt();
        final byte[] seed1 = buffer.readRawBytes(8);
        buffer.skipByte();
        int serverCapabilities2FirstBytes = buffer.readShort() & 0x0000ffff;
        serverLanguage = buffer.readByte();
        serverStatus = buffer.readShort();
        int serverCapabilities4FirstBytes = serverCapabilities2FirstBytes
                                            + (buffer.readShort() << 16);
        int saltLength = 0;

        if ((serverCapabilities4FirstBytes & OceanBaseServerCapabilities.PLUGIN_AUTH) != 0) {
            saltLength = Math.max(12, buffer.readByte() - 9);
        } else {
            buffer.skipByte();
        }
        buffer.skipBytes(6);

        // MariaDB additional capabilities.
        // Filled only if MariaDB server 10.2+
        long mariaDbAdditionalCapacities = buffer.readInt();

        if ((serverCapabilities4FirstBytes & OceanBaseServerCapabilities.SECURE_CONNECTION) != 0) {
            final byte[] seed2;
            if (saltLength > 0) {
                seed2 = buffer.readRawBytes(saltLength);
            } else {
                // for servers before 5.5 version
                seed2 = buffer.readBytesNullEnd();
            }
            seed = Utils.copyWithLength(seed1, seed1.length + seed2.length);
            System.arraycopy(seed2, 0, seed, seed1.length, seed2.length);
            //  reader.skipByte();
        } else {
            seed = Utils.copyWithLength(seed1, seed1.length);
        }
        buffer.skipByte();

        /*
         * check for MariaDB 10.x replication hack , remove fake prefix if needed
         *  (see comments about MARIADB_RPL_HACK_PREFIX)
         */

        if (serverVersion.startsWith(MARIADB_RPL_HACK_PREFIX)) {
            serverMariaDb = true;
            serverVersion = serverVersion.substring(MARIADB_RPL_HACK_PREFIX.length());
        } else {
            serverMariaDb = this.serverVersion.contains("MariaDB");
        }

        // since MariaDB 10.2
        if ((serverCapabilities4FirstBytes & OceanBaseServerCapabilities.CLIENT_MYSQL) == 0) {
            serverCapabilities = (serverCapabilities4FirstBytes & 0xffffffffL)
                                 + (mariaDbAdditionalCapacities << 32);
            serverMariaDb = true;
        } else {
            serverCapabilities = serverCapabilities4FirstBytes & 0xffffffffL;
        }

        if ((serverCapabilities4FirstBytes & OceanBaseServerCapabilities.PLUGIN_AUTH) != 0) {
            authenticationPluginType = buffer.readStringNullEnd(StandardCharsets.US_ASCII);
        }
    }

    @Override
    public String toString() {
        return protocolVersion + ":" + serverVersion + ":" + serverThreadId + ":"
               + new String(seed) + ":" + serverCapabilities + ":" + serverLanguage + ":"
               + serverStatus;
    }

    public String getServerVersion() {
        return serverVersion;
    }

    public byte getProtocolVersion() {
        return protocolVersion;
    }

    public long getServerThreadId() {
        return serverThreadId;
    }

    public byte[] getSeed() {
        return seed;
    }

    public long getServerCapabilities() {
        return serverCapabilities;
    }

    public byte getServerLanguage() {
        return serverLanguage;
    }

    public short getServerStatus() {
        return serverStatus;
    }

    public String getAuthenticationPluginType() {
        return authenticationPluginType;
    }

    public boolean isServerMariaDb() {
        return serverMariaDb;
    }
}
