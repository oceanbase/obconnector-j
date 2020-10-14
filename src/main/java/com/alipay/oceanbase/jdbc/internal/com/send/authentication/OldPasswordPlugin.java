package com.alipay.oceanbase.jdbc.internal.com.send.authentication;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import com.alipay.oceanbase.jdbc.authentication.AuthenticationPlugin;
import com.alipay.oceanbase.jdbc.internal.com.read.Buffer;
import com.alipay.oceanbase.jdbc.internal.io.input.PacketInputStream;
import com.alipay.oceanbase.jdbc.internal.io.output.PacketOutputStream;
import com.alipay.oceanbase.jdbc.internal.util.Utils;
import com.alipay.oceanbase.jdbc.util.Options;

public class OldPasswordPlugin implements AuthenticationPlugin {
    public static final String TYPE = "mysql_old_password";
    private String             authenticationData;
    private byte[]             seed;

    @Override
    public String name() {
        return "mysql pre 4.1 password encoding";
    }

    @Override
    public String type() {
        return TYPE;
    }

    public void initialize(String authenticationData, byte[] seed, Options options) {
        this.seed = seed;
        this.authenticationData = authenticationData;
    }

    /**
     * Process old password plugin authentication. see
     * https://mariadb.com/kb/en/library/authentication-plugin-mysql_old_password/
     *
     * @param out out stream
     * @param in in stream
     * @param sequence packet sequence
     * @return response packet
     * @throws IOException if socket error
     */
    public Buffer process(PacketOutputStream out, PacketInputStream in, AtomicInteger sequence)
                                                                                               throws IOException {

        if (authenticationData == null || authenticationData.isEmpty()) {
            out.writeEmptyPacket(sequence.incrementAndGet());
        } else {
            out.startPacket(sequence.incrementAndGet());
            byte[] seed = Utils.copyWithLength(this.seed, 8);
            out.write(cryptOldFormatPassword(authenticationData, new String(seed)));
            out.write((byte) 0x00);
            out.flush();
        }

        Buffer buffer = in.getPacket(true);
        sequence.set(in.getLastPacketSeq());
        return buffer;
    }

    private byte[] cryptOldFormatPassword(String password, String seed) {
        byte[] result = new byte[seed.length()];

        if ((password == null) || (password.length() == 0)) {
            return new byte[0];
        }

        long[] seedHash = hashPassword(seed);
        long[] passHash = hashPassword(password);

        RandStruct randSeed = new RandStruct(seedHash[0] ^ passHash[0], seedHash[1] ^ passHash[1]);

        for (int i = 0; i < seed.length(); i++) {
            result[i] = (byte) Math.floor((random(randSeed) * 31) + 64);
        }
        byte extra = (byte) Math.floor(random(randSeed) * 31);
        for (int i = 0; i < seed.length(); i++) {
            result[i] ^= extra;
        }
        return result;
    }

    private double random(RandStruct rand) {
        rand.seed1 = (rand.seed1 * 3 + rand.seed2) % rand.maxValue;
        rand.seed2 = (rand.seed1 + rand.seed2 + 33) % rand.maxValue;
        return (double) rand.seed1 / rand.maxValue;
    }

    private long[] hashPassword(String password) {
        long nr = 1345345333L;
        long nr2 = 0x12345671L;
        long add = 7;

        for (int i = 0; i < password.length(); i++) {
            char currChar = password.charAt(i);
            if (currChar == ' ' || currChar == '\t') {
                continue;
            }

            nr ^= (((nr & 63) + add) * (long) currChar) + (nr << 8);
            nr2 += (nr2 << 8) ^ nr;
            add += currChar;
        }
        return new long[] { nr & 0x7FFFFFFF, nr2 & 0x7FFFFFFF };
    }

    private class RandStruct {

        private final long maxValue = 0x3FFFFFFFL;
        private long       seed1;
        private long       seed2;

        public RandStruct(long seed1, long seed2) {
            this.seed1 = seed1 % maxValue;
            this.seed2 = seed2 % maxValue;
        }
    }
}
