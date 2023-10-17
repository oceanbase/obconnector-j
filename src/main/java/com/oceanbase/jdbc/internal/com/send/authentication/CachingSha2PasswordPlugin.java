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
package com.oceanbase.jdbc.internal.com.send.authentication;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import com.oceanbase.jdbc.authentication.AuthenticationPlugin;
import com.oceanbase.jdbc.internal.com.read.Buffer;
import com.oceanbase.jdbc.internal.io.input.PacketInputStream;
import com.oceanbase.jdbc.internal.io.output.PacketOutputStream;
import com.oceanbase.jdbc.util.Options;

public class CachingSha2PasswordPlugin implements AuthenticationPlugin {

    public static final String TYPE = "caching_sha2_password";

    private String             authenticationData;
    private byte[]             seed;
    private Options            options;

    /**
     * Send a SHA-2 encrypted password. encryption XOR(SHA256(password), SHA256(seed,
     * SHA256(SHA256(password))))
     *
     * @param password password
     * @param seed seed
     * @param passwordCharacterEncoding option if not using default byte encoding
     * @return encrypted pwd
     * @throws NoSuchAlgorithmException if SHA-256 algorithm is unknown
     * @throws UnsupportedEncodingException if SHA-256 algorithm is unknown
     */
    public static byte[] sha256encryptPassword(final String password, final byte[] seed,
                                               String passwordCharacterEncoding)
                                                                                throws NoSuchAlgorithmException,
                                                                                UnsupportedEncodingException {

        if (password == null || password.isEmpty()) {
            return new byte[0];
        }

        final MessageDigest messageDigest = MessageDigest.getInstance("SHA-256");
        byte[] bytePwd;
        if (passwordCharacterEncoding != null && !passwordCharacterEncoding.isEmpty()) {
            bytePwd = password.getBytes(passwordCharacterEncoding);
        } else {
            bytePwd = password.getBytes();
        }

        final byte[] stage1 = messageDigest.digest(bytePwd);
        messageDigest.reset();

        final byte[] stage2 = messageDigest.digest(stage1);
        messageDigest.reset();

        messageDigest.update(stage2);
        messageDigest.update(seed);

        final byte[] digest = messageDigest.digest();
        final byte[] returnBytes = new byte[digest.length];
        for (int i = 0; i < digest.length; i++) {
            returnBytes[i] = (byte) (stage1[i] ^ digest[i]);
        }
        return returnBytes;
    }

    @Override
    public String name() {
        return "caching sha2 password";
    }

    @Override
    public String type() {
        return TYPE;
    }

    /**
     * Initialized data.
     *
     * @param authenticationData authentication data (password/token)
     * @param seed server provided seed
     * @param options Connection string options
     */
    public void initialize(String authenticationData, byte[] seed, Options options) {
        this.seed = seed;
        this.authenticationData = authenticationData;
        this.options = options;
    }

    /**
     * Process native password plugin authentication. see
     * https://mariadb.com/kb/en/library/authentication-plugin-mysql_native_password/
     *
     * @param out out stream
     * @param in in stream
     * @param sequence packet sequence
     * @return response packet
     * @throws IOException if socket error
     */
    public Buffer process(PacketOutputStream out, PacketInputStream in, AtomicInteger sequence)
                                                                                               throws IOException,
                                                                                               SQLException {
        if (authenticationData == null || authenticationData.isEmpty()) {
            out.writeEmptyPacket(sequence.incrementAndGet());
        } else {
            try {
                out.startPacket(sequence.incrementAndGet());
                byte[] truncatedSeed;
                if (seed.length > 0) {
                    // Seed is ended with a null byte value.
                    truncatedSeed = Arrays.copyOfRange(seed, 0, seed.length - 1);
                } else {
                    truncatedSeed = new byte[0];
                }

                out.write(sha256encryptPassword(authenticationData, truncatedSeed,
                    options.passwordCharacterEncoding));
                out.flush();
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException("Could not use SHA-256, failing", e);
            }
        }

        Buffer buffer = in.getPacket(true);
        sequence.set(in.getLastPacketSeq());
        switch (buffer.getByteAt(0)) {

        // success or error
            case (byte) 0x00:
            case (byte) 0xFF:
                return buffer;

                // fast authentication result
            default:
                byte[] authResult = buffer.getLengthEncodedBytes();
                switch (authResult[0]) {
                    case 3:
                        buffer = in.getPacket(true);
                        sequence.set(in.getLastPacketSeq());
                        return buffer;
                    case 4:
                        if (Boolean.TRUE.equals(options.useSsl)) {
                            // send clear password
                            out.startPacket(sequence.incrementAndGet());
                            byte[] bytePwd;
                            if (options.passwordCharacterEncoding != null
                                && !options.passwordCharacterEncoding.isEmpty()) {
                                bytePwd = authenticationData
                                    .getBytes(options.passwordCharacterEncoding);
                            } else {
                                bytePwd = authenticationData.getBytes();
                            }

                            out.write(bytePwd);
                            out.write(0);
                            out.flush();
                        } else {
                            // retrieve public key from configuration or from server
                            PublicKey publicKey;
                            if (options.serverRsaPublicKeyFile != null
                                && !options.serverRsaPublicKeyFile.isEmpty()) {
                                publicKey = Sha256PasswordPlugin
                                    .readPublicKeyFromFile(options.serverRsaPublicKeyFile);
                            } else {
                                if (!options.allowPublicKeyRetrieval) {
                                    throw new SQLException(
                                        "RSA public key is not available client side (option serverRsaPublicKeyFile not"
                                                + " set)", "S1009");
                                }

                                // ask public Key Retrieval
                                out.startPacket(sequence.incrementAndGet());
                                out.write((byte) 2);
                                out.flush();
                                publicKey = Sha256PasswordPlugin.readPublicKeyFromSocket(in,
                                    sequence);
                            }

                            try {
                                byte[] cipherBytes = Sha256PasswordPlugin.encrypt(publicKey,
                                    authenticationData, seed, options.passwordCharacterEncoding);
                                out.startPacket(sequence.incrementAndGet());
                                out.write(cipherBytes);
                                out.flush();
                            } catch (Exception ex) {
                                throw new SQLException("Could not connect using SHA256 plugin : "
                                                       + ex.getMessage(), "S1009", ex);
                            }
                        }

                        buffer = in.getPacket(true);
                        sequence.set(in.getLastPacketSeq());
                        return buffer;

                    default:
                        throw new SQLException(
                            "Protocol exchange error. Expect login success or RSA login request message",
                            "S1009");
                }
        }
    }
}
