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
package com.oceanbase.jdbc.internal.com.send.authentication.ed25519.math.ed25519;

import com.oceanbase.jdbc.internal.com.send.authentication.ed25519.math.Encoding;
import com.oceanbase.jdbc.internal.com.send.authentication.ed25519.math.FieldElement;

/**
 * Helper class for encoding/decoding from/to the 32 byte representation.
 *
 * <p>Reviewed/commented by Bloody Rookie (nemproject@gmx.de)
 */
public class Ed25519LittleEndianEncoding extends Encoding {

    static int load_3(byte[] in, int offset) {
        int result = in[offset++] & 0xff;
        result |= (in[offset++] & 0xff) << 8;
        result |= (in[offset] & 0xff) << 16;
        return result;
    }

    static long load_4(byte[] in, int offset) {
        int result = in[offset++] & 0xff;
        result |= (in[offset++] & 0xff) << 8;
        result |= (in[offset++] & 0xff) << 16;
        result |= in[offset] << 24;
        return ((long) result) & 0xffffffffL;
    }

    /**
     * Encodes a given field element in its 32 byte representation. This is done in two steps:
     *
     * <ol>
     *   <li>Reduce the value of the field element modulo $p$.
     *   <li>Convert the field element to the 32 byte representation.
     * </ol>
     *
     * <p>The idea for the modulo $p$ reduction algorithm is as follows:
     *
     * <h2>Assumption:</h2>
     *
     * <ul>
     *   <li>$p = 2^{255} - 19$
     *   <li>$h = h_0 + 2^{25} * h_1 + 2^{(26+25)} * h_2 + \dots + 2^{230} * h_9$ where $0 \le |h_i|
     *       \lt 2^{27}$ for all $i=0,\dots,9$.
     *   <li>$h \cong r \mod p$, i.e. $h = r + q * p$ for some suitable $0 \le r \lt p$ and an integer
     *       $q$.
     * </ul>
     *
     * <p>Then $q = [2^{-255} * (h + 19 * 2^{-25} * h_9 + 1/2)]$ where $[x] = floor(x)$.
     *
     * <h2>Proof:</h2>
     *
     * <p>We begin with some very raw estimation for the bounds of some expressions:
     *
     * <p>$$ \begin{equation} |h| \lt 2^{230} * 2^{30} = 2^{260} \Rightarrow |r + q * p| \lt 2^{260}
     * \Rightarrow |q| \lt 2^{10}. \\ \Rightarrow -1/4 \le a := 19^2 * 2^{-255} * q \lt 1/4. \\ |h -
     * 2^{230} * h_9| = |h_0 + \dots + 2^{204} * h_8| \lt 2^{204} * 2^{30} = 2^{234}. \\ \Rightarrow
     * -1/4 \le b := 19 * 2^{-255} * (h - 2^{230} * h_9) \lt 1/4 \end{equation} $$
     *
     * <p>Therefore $0 \lt 1/2 - a - b \lt 1$.
     *
     * <p>Set $x := r + 19 * 2^{-255} * r + 1/2 - a - b$. Then:
     *
     * <p>$$ 0 \le x \lt 255 - 20 + 19 + 1 = 2^{255} \\ \Rightarrow 0 \le 2^{-255} * x \lt 1. $$
     *
     * <p>Since $q$ is an integer we have
     *
     * <p>$$ [q + 2^{-255} * x] = q \quad (1) $$
     *
     * <p>Have a closer look at $x$:
     *
     * <p>$$ \begin{align} x &amp;= h - q * (2^{255} - 19) + 19 * 2^{-255} * (h - q * (2^{255} - 19))
     * + 1/2 - 19^2 * 2^{-255} * q - 19 * 2^{-255} * (h - 2^{230} * h_9) \\ &amp;= h - q * 2^{255} +
     * 19 * q + 19 * 2^{-255} * h - 19 * q + 19^2 * 2^{-255} * q + 1/2 - 19^2 * 2^{-255} * q - 19 *
     * 2^{-255} * h + 19 * 2^{-25} * h_9 \\ &amp;= h + 19 * 2^{-25} * h_9 + 1/2 - q^{255}. \end{align}
     * $$
     *
     * <p>Inserting the expression for $x$ into $(1)$ we get the desired expression for $q$.
     */
    public byte[] encode(FieldElement x) {
        int[] h = ((Ed25519FieldElement) x).t;
        int h0 = h[0];
        int h1 = h[1];
        int h2 = h[2];
        int h3 = h[3];
        int h4 = h[4];
        int h5 = h[5];
        int h6 = h[6];
        int h7 = h[7];
        int h8 = h[8];
        int h9 = h[9];
        int q;
        int carry0;
        int carry1;
        int carry2;
        int carry3;
        int carry4;
        int carry5;
        int carry6;
        int carry7;
        int carry8;
        int carry9;

        // Step 1:
        // Calculate q
        q = (19 * h9 + (1 << 24)) >> 25;
        q = (h0 + q) >> 26;
        q = (h1 + q) >> 25;
        q = (h2 + q) >> 26;
        q = (h3 + q) >> 25;
        q = (h4 + q) >> 26;
        q = (h5 + q) >> 25;
        q = (h6 + q) >> 26;
        q = (h7 + q) >> 25;
        q = (h8 + q) >> 26;
        q = (h9 + q) >> 25;

        // r = h - q * p = h - 2^255 * q + 19 * q
        // First add 19 * q then discard the bit 255
        h0 += 19 * q;

        carry0 = h0 >> 26;
        h1 += carry0;
        h0 -= carry0 << 26;
        carry1 = h1 >> 25;
        h2 += carry1;
        h1 -= carry1 << 25;
        carry2 = h2 >> 26;
        h3 += carry2;
        h2 -= carry2 << 26;
        carry3 = h3 >> 25;
        h4 += carry3;
        h3 -= carry3 << 25;
        carry4 = h4 >> 26;
        h5 += carry4;
        h4 -= carry4 << 26;
        carry5 = h5 >> 25;
        h6 += carry5;
        h5 -= carry5 << 25;
        carry6 = h6 >> 26;
        h7 += carry6;
        h6 -= carry6 << 26;
        carry7 = h7 >> 25;
        h8 += carry7;
        h7 -= carry7 << 25;
        carry8 = h8 >> 26;
        h9 += carry8;
        h8 -= carry8 << 26;
        carry9 = h9 >> 25;
        h9 -= carry9 << 25;

        // Step 2 (straight forward conversion):
        byte[] s = new byte[32];
        s[0] = (byte) h0;
        s[1] = (byte) (h0 >> 8);
        s[2] = (byte) (h0 >> 16);
        s[3] = (byte) ((h0 >> 24) | (h1 << 2));
        s[4] = (byte) (h1 >> 6);
        s[5] = (byte) (h1 >> 14);
        s[6] = (byte) ((h1 >> 22) | (h2 << 3));
        s[7] = (byte) (h2 >> 5);
        s[8] = (byte) (h2 >> 13);
        s[9] = (byte) ((h2 >> 21) | (h3 << 5));
        s[10] = (byte) (h3 >> 3);
        s[11] = (byte) (h3 >> 11);
        s[12] = (byte) ((h3 >> 19) | (h4 << 6));
        s[13] = (byte) (h4 >> 2);
        s[14] = (byte) (h4 >> 10);
        s[15] = (byte) (h4 >> 18);
        s[16] = (byte) h5;
        s[17] = (byte) (h5 >> 8);
        s[18] = (byte) (h5 >> 16);
        s[19] = (byte) ((h5 >> 24) | (h6 << 1));
        s[20] = (byte) (h6 >> 7);
        s[21] = (byte) (h6 >> 15);
        s[22] = (byte) ((h6 >> 23) | (h7 << 3));
        s[23] = (byte) (h7 >> 5);
        s[24] = (byte) (h7 >> 13);
        s[25] = (byte) ((h7 >> 21) | (h8 << 4));
        s[26] = (byte) (h8 >> 4);
        s[27] = (byte) (h8 >> 12);
        s[28] = (byte) ((h8 >> 20) | (h9 << 6));
        s[29] = (byte) (h9 >> 2);
        s[30] = (byte) (h9 >> 10);
        s[31] = (byte) (h9 >> 18);
        return s;
    }

    /**
     * Decodes a given field element in its 10 byte $2^{25.5}$ representation.
     *
     * @param in The 32 byte representation.
     * @return The field element in its $2^{25.5}$ bit representation.
     */
    public FieldElement decode(byte[] in) {
        long h0 = load_4(in, 0);
        long h1 = load_3(in, 4) << 6;
        long h2 = load_3(in, 7) << 5;
        long h3 = load_3(in, 10) << 3;
        long h4 = load_3(in, 13) << 2;
        long h5 = load_4(in, 16);
        long h6 = load_3(in, 20) << 7;
        long h7 = load_3(in, 23) << 5;
        long h8 = load_3(in, 26) << 4;
        long h9 = (load_3(in, 29) & 0x7FFFFF) << 2;
        long carry0;
        long carry1;
        long carry2;
        long carry3;
        long carry4;
        long carry5;
        long carry6;
        long carry7;
        long carry8;
        long carry9;

        // Remember: 2^255 congruent 19 modulo p
        carry9 = (h9 + (long) (1 << 24)) >> 25;
        h0 += carry9 * 19;
        h9 -= carry9 << 25;
        carry1 = (h1 + (long) (1 << 24)) >> 25;
        h2 += carry1;
        h1 -= carry1 << 25;
        carry3 = (h3 + (long) (1 << 24)) >> 25;
        h4 += carry3;
        h3 -= carry3 << 25;
        carry5 = (h5 + (long) (1 << 24)) >> 25;
        h6 += carry5;
        h5 -= carry5 << 25;
        carry7 = (h7 + (long) (1 << 24)) >> 25;
        h8 += carry7;
        h7 -= carry7 << 25;

        carry0 = (h0 + (long) (1 << 25)) >> 26;
        h1 += carry0;
        h0 -= carry0 << 26;
        carry2 = (h2 + (long) (1 << 25)) >> 26;
        h3 += carry2;
        h2 -= carry2 << 26;
        carry4 = (h4 + (long) (1 << 25)) >> 26;
        h5 += carry4;
        h4 -= carry4 << 26;
        carry6 = (h6 + (long) (1 << 25)) >> 26;
        h7 += carry6;
        h6 -= carry6 << 26;
        carry8 = (h8 + (long) (1 << 25)) >> 26;
        h9 += carry8;
        h8 -= carry8 << 26;

        int[] h = new int[10];
        h[0] = (int) h0;
        h[1] = (int) h1;
        h[2] = (int) h2;
        h[3] = (int) h3;
        h[4] = (int) h4;
        h[5] = (int) h5;
        h[6] = (int) h6;
        h[7] = (int) h7;
        h[8] = (int) h8;
        h[9] = (int) h9;
        return new Ed25519FieldElement(f, h);
    }

    /**
     * Is the FieldElement negative in this encoding?
     *
     * <p>Return true if $x$ is in $\{1,3,5,\dots,q-2\}$<br>
     * Return false if $x$ is in $\{0,2,4,\dots,q-1\}$
     *
     * <p>Preconditions:
     *
     * <ul>
     *   <li>$|x|$ bounded by $1.1*2^{26},1.1*2^{25},1.1*2^{26},1.1*2^{25}$, etc.
     * </ul>
     *
     * @return true if $x$ is in $\{1,3,5,\dots,q-2\}$, false otherwise.
     */
    public boolean isNegative(FieldElement x) {
        byte[] s = encode(x);
        return (s[0] & 1) != 0;
    }
}
