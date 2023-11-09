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
package com.oceanbase.jdbc;

import static org.junit.Assert.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.SQLException;

import org.junit.Test;

public class OceanBaseBlobTest {

    private final byte[] bytes = new byte[] { 0, 1, 2, 3, 4, 5 };

    @Test
    public void length() {
        Blob blob = new Blob(bytes);
        assertEquals(6, blob.length);

        Blob blob2 = new Blob(bytes, 2, 3);
        assertEquals(3, blob2.length);
    }

    @Test
    public void getBytes() throws SQLException {
        Blob blob = new Blob(bytes);
        assertArrayEquals(bytes, blob.getBytes(1, 6));
        assertArrayEquals(new byte[] { 0, 1, 2, 3, 4, 5 }, blob.getBytes(1, 7));
        assertArrayEquals(new byte[] { 0, 1 }, blob.getBytes(1, 2));

        Blob blob2 = new Blob(bytes, 2, 3);
        assertArrayEquals(new byte[] { 2, 3, 4 }, blob2.getBytes(1, 3));
        assertArrayEquals(new byte[] { 2, 3, 4 }, blob2.getBytes(1, 6));
        assertArrayEquals(new byte[] { 2, 3 }, blob2.getBytes(1, 2));
        assertArrayEquals(new byte[] { 3, 4, 0 }, blob2.getBytes(2, 3));
        assertArrayEquals(new byte[] { 3, 4, 0 }, blob2.getBytes(2, 6));
        assertArrayEquals(new byte[] { 3, 4 }, blob2.getBytes(2, 2));

        try {
            blob2.getBytes(0, 3);
            fail("must have thrown exception, min pos is 1");
        } catch (SQLException sqle) {
            // normal exception
        }
    }

    @Test
    public void getBinaryStream() throws SQLException {
        Blob blob = new Blob(bytes);
        assureInputStreamEqual(bytes, blob.getBinaryStream(1, 6));
        try {
            assureInputStreamEqual(new byte[] { 0, 1, 2, 3, 4, 5, 0 }, blob.getBinaryStream(1, 7));
            fail("must have thrown exception, max length is 6");
        } catch (SQLException sqle) {
            // normal exception
        }

        assureInputStreamEqual(new byte[] { 0, 1 }, blob.getBinaryStream(1, 2));

        Blob blob2 = new Blob(bytes, 2, 3);
        assureInputStreamEqual(new byte[] { 2, 3, 4 }, blob2.getBinaryStream(1, 3));
        try {
            assureInputStreamEqual(new byte[] { 2, 3, 4, 0, 0, 0 }, blob2.getBinaryStream(1, 6));
            fail("must have thrown exception, max length is 3");
        } catch (SQLException sqle) {
            // normal exception
        }
        assureInputStreamEqual(new byte[] { 2, 3 }, blob2.getBinaryStream(1, 2));
        try {
            assureInputStreamEqual(new byte[] { 3, 4, 0 }, blob2.getBinaryStream(2, 3));
        } catch (SQLException sqle) {
            // normal exception
        }
        assureInputStreamEqual(new byte[] { 3, 4 }, blob2.getBinaryStream(2, 2));

        try {
            blob2.getBytes(0, 3);
            fail("must have thrown exception, min pos is 1");
        } catch (SQLException sqle) {
            // normal exception
        }
    }

    private void assureInputStreamEqual(byte[] expected, InputStream stream) {
        try {
            for (byte expectedVal : expected) {
                int val = stream.read();
                assertEquals(expectedVal, val);
            }
            assertEquals(-1, stream.read());
        } catch (IOException ioe) {
            ioe.printStackTrace();
            fail();
        }
    }

    @Test
    public void position() throws SQLException {
        Blob blob = new Blob(bytes);
        assertEquals(5, blob.position(new byte[] { 4, 5 }, 2));

        Blob blob2 = new Blob(bytes, 2, 4);
        assertEquals(3, blob2.position(new byte[] { 4, 5 }, 2));
    }

    @Test
    public void setBytes() throws SQLException {
        final byte[] bytes = new byte[] { 0, 1, 2, 3, 4, 5 };
        final byte[] otherBytes = new byte[] { 10, 11, 12, 13 };

        Blob blob = new Blob(new byte[] { 0, 1, 2, 3, 4, 5 });
        blob.setBytes(2, otherBytes);
        assertArrayEquals(new byte[] { 0, 10, 11, 12, 13, 5 }, blob.getBytes(1, 6));

        Blob blob2 = new Blob(new byte[] { 0, 1, 2, 3, 4, 5 });
        blob2.setBytes(4, otherBytes);
        assertArrayEquals(new byte[] { 0, 1, 2, 10, 11, 12, 13 }, blob2.getBytes(1, 7));

        Blob blob3 = new Blob(new byte[] { 0, 1, 2, 3, 4, 5 }, 2, 3);
        blob3.setBytes(2, otherBytes);
        assertArrayEquals(new byte[] { 2, 10, 11, 12, 13 }, blob3.getBytes(1, 7));

        Blob blob4 = new Blob(new byte[] { 0, 1, 2, 3, 4, 5 }, 2, 3);
        blob4.setBytes(4, otherBytes);
        assertArrayEquals(new byte[] { 2, 3, 4, 10, 11, 12 }, blob4.getBytes(1, 6));

        try {
            Blob blob5 = new Blob(new byte[] { 0, 1, 2, 3, 4, 5 }, 2, 3);
            blob5.setBytes(0, otherBytes);
        } catch (SQLException sqle) {
            // normal exception
        }
    }

    @Test
    public void setBytesOffset() throws SQLException {
        final byte[] bytes = new byte[] { 0, 1, 2, 3, 4, 5 };
        final byte[] otherBytes = new byte[] { 10, 11, 12, 13 };

        Blob blob = new Blob(new byte[] { 0, 1, 2, 3, 4, 5 });
        blob.setBytes(2, otherBytes, 2, 3);
        assertArrayEquals(new byte[] { 0, 12, 13, 3, 4, 5 }, blob.getBytes(1, 6));

        Blob blob2 = new Blob(new byte[] { 0, 1, 2, 3, 4, 5 });
        blob2.setBytes(4, otherBytes, 3, 2);
        assertArrayEquals(new byte[] { 0, 1, 2, 13, 4, 5, }, blob2.getBytes(1, 7));

        Blob blob3 = new Blob(new byte[] { 0, 1, 2, 3, 4, 5 }, 2, 4);
        blob3.setBytes(2, otherBytes, 2, 3);
        assertArrayEquals(new byte[] { 2, 12, 13, 5 }, blob3.getBytes(1, 7));

        Blob blob4 = new Blob(new byte[] { 0, 1, 2, 3, 4, 5 }, 2, 3);
        blob4.setBytes(4, otherBytes, 2, 2);
        assertArrayEquals(new byte[] { 2, 3, 4, 12, 13 }, blob4.getBytes(1, 6));

        Blob blob5 = new Blob(new byte[] { 0, 1, 2, 3, 4, 5 }, 2, 3);
        blob5.setBytes(4, otherBytes, 2, 20);
        assertArrayEquals(new byte[] { 2, 3, 4, 12, 13 }, blob5.getBytes(1, 6));

        try {
            Blob blob6 = new Blob(new byte[] { 0, 1, 2, 3, 4, 5 }, 2, 3);
            blob6.setBytes(0, otherBytes, 2, 3);
        } catch (SQLException sqle) {
            // normal exception
        }
    }

    @Test
    public void setBinaryStream() throws SQLException, IOException {
        final byte[] bytes = new byte[] { 0, 1, 2, 3, 4, 5 };
        final byte[] otherBytes = new byte[] { 10, 11, 12, 13 };

        Blob blob = new Blob(new byte[] { 0, 1, 2, 3, 4, 5 });
        OutputStream out = blob.setBinaryStream(2);
        out.write(otherBytes);
        assertArrayEquals(new byte[] { 0, 10, 11, 12, 13, 5 }, blob.getBytes(1, 6));

        Blob blob2 = new Blob(new byte[] { 0, 1, 2, 3, 4, 5 });
        OutputStream out2 = blob2.setBinaryStream(4);
        out2.write(otherBytes);
        assertArrayEquals(new byte[] { 0, 1, 2, 10, 11, 12, 13 }, blob2.getBytes(1, 7));

        Blob blob3 = new Blob(new byte[] { 0, 1, 2, 3, 4, 5 });
        OutputStream out3 = blob3.setBinaryStream(2);
        out3.write(otherBytes);
        assertArrayEquals(new byte[] { 0, 10, 11, 12, 13, 5 }, blob3.getBytes(1, 7));

        Blob blob4 = new Blob(new byte[] { 0, 1, 2, 3, 4, 5 }, 2, 3);
        OutputStream out4 = blob4.setBinaryStream(4);
        out4.write(otherBytes);
        assertArrayEquals(new byte[] { 2, 3, 4, 10, 11, 12 }, blob4.getBytes(1, 6));

        try {
            Blob blob5 = new Blob(new byte[] { 0, 1, 2, 3, 4, 5 }, 2, 3);
            blob5.setBinaryStream(0);
        } catch (SQLException sqle) {
            // normal exception
        }
    }

    @Test
    public void setBinaryStreamOffset() throws SQLException, IOException {
        final byte[] bytes = new byte[] { 0, 1, 2, 3, 4, 5 };
        final byte[] otherBytes = new byte[] { 10, 11, 12, 13 };

        Blob blob = new Blob(new byte[] { 0, 1, 2, 3, 4, 5 });
        OutputStream out = blob.setBinaryStream(2);
        out.write(otherBytes, 2, 3);
        assertArrayEquals(new byte[] { 0, 12, 13, 3, 4, 5 }, blob.getBytes(1, 6));

        Blob blob2 = new Blob(new byte[] { 0, 1, 2, 3, 4, 5 });
        OutputStream out2 = blob2.setBinaryStream(4);
        out2.write(otherBytes, 3, 2);
        blob2.getBytes(1, 7);
        assertArrayEquals(new byte[] { 0, 1, 2, 13, 4, 5 }, blob2.getBytes(1, 7));
        Blob blob3 = new Blob(new byte[] { 0, 1, 2, 3, 4, 5 });
        OutputStream out3 = blob3.setBinaryStream(2);
        out3.write(otherBytes, 2, 3);
        assertArrayEquals(new byte[] { 0, 12, 13, 3, 4, 5 }, blob3.getBytes(1, 7));

        Blob blob4 = new Blob(new byte[] { 0, 1, 2, 3, 4, 5 }, 2, 3);
        OutputStream out4 = blob4.setBinaryStream(4);
        out4.write(otherBytes, 2, 2);
        assertArrayEquals(new byte[] { 2, 3, 4, 12, 13 }, blob4.getBytes(1, 6));

        Blob blob5 = new Blob(new byte[] { 0, 1, 2, 3, 4, 5 }, 2, 3);
        OutputStream out5 = blob5.setBinaryStream(4);
        out5.write(otherBytes, 2, 20);
        assertArrayEquals(new byte[] { 2, 3, 4, 12, 13 }, blob5.getBytes(1, 6));
    }

    @Test
    public void truncate() throws SQLException {
        Blob blob = new Blob(bytes);
        blob.truncate(20);
        assertArrayEquals(bytes, blob.getBytes(1, 6));
        blob.truncate(5);
        assertArrayEquals(new byte[] { 0, 1, 2, 3, 4 }, blob.getBytes(1, 7));
        blob.truncate(0);
        assertArrayEquals(new byte[] {}, blob.getBytes(1, 2));

        Blob blob2 = new Blob(bytes, 2, 3);
        blob2.truncate(20);
        assertArrayEquals(new byte[] { 2, 3, 4 }, blob2.getBytes(1, 3));
        blob2.truncate(2);
        assertArrayEquals(new byte[] { 2, 3 }, blob2.getBytes(1, 6));

        blob2.truncate(1);
        assertArrayEquals(new byte[] { 2 }, blob2.getBytes(1, 2));
    }

    @Test
    public void free() {
        Blob blob = new Blob(bytes);
        blob.free();
        assertEquals(0, blob.length);
    }
}
