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
package com.oceanbase.jdbc.internal.com.send.authentication.ed25519.math;

import java.io.Serializable;

/**
 * An EdDSA finite field. Includes several pre-computed values.
 *
 * @author str4d
 */
public class Field implements Serializable {

    private static final long  serialVersionUID = 8746587465875676L;

    public final FieldElement  ZERO;
    public final FieldElement  ONE;
    public final FieldElement  TWO;
    public final FieldElement  FOUR;
    public final FieldElement  FIVE;
    public final FieldElement  EIGHT;

    private final int          b;
    private final FieldElement q;
    /** q-2 */
    private final FieldElement qm2;
    /** (q-5) / 8 */
    private final FieldElement qm5d8;

    private final Encoding     enc;

    public Field(int b, byte[] q, Encoding enc) {
        this.b = b;
        this.enc = enc;
        this.enc.setField(this);

        this.q = fromByteArray(q);

        // Set up constants
        ZERO = fromByteArray(Constants.ZERO);
        ONE = fromByteArray(Constants.ONE);
        TWO = fromByteArray(Constants.TWO);
        FOUR = fromByteArray(Constants.FOUR);
        FIVE = fromByteArray(Constants.FIVE);
        EIGHT = fromByteArray(Constants.EIGHT);

        // Precompute values
        qm2 = this.q.subtract(TWO);
        qm5d8 = this.q.subtract(FIVE).divide(EIGHT);
    }

    public FieldElement fromByteArray(byte[] x) {
        return enc.decode(x);
    }

    public int getb() {
        return b;
    }

    public FieldElement getQ() {
        return q;
    }

    public FieldElement getQm2() {
        return qm2;
    }

    public FieldElement getQm5d8() {
        return qm5d8;
    }

    public Encoding getEncoding() {
        return enc;
    }

    @Override
    public int hashCode() {
        return q.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Field)) {
            return false;
        }
        Field f = (Field) obj;
        return b == f.b && q.equals(f.q);
    }
}
