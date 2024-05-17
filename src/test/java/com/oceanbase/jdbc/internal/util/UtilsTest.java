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
package com.oceanbase.jdbc.internal.util;

import static org.junit.Assert.*;

import org.junit.Test;

import com.oceanbase.jdbc.internal.com.send.parameters.ParameterHolder;
import com.oceanbase.jdbc.internal.com.send.parameters.StringParameter;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;

public class UtilsTest {

    @Test
    public void testByteDump() {
        byte[] bb = new byte[] { 0x4A, 0x00, 0x00, 0x00, 0x03, 0x53, 0x45, 0x4C, 0x45, 0x43, 0x54,
                0x20, 0x40, 0x40, 0x6D, 0x61, 0x78, 0x5F, 0x61, 0x6C, 0x6C, 0x6F, 0x77, 0x65, 0x64,
                0x5F, 0x70, 0x61, 0x63, 0x6B, 0x65, 0x74, 0x20, 0x2C, 0x20, 0x40, 0x40, 0x73, 0x79,
                0x73, 0x74, 0x65, 0x6D, 0x5F, 0x74, 0x69, 0x6D, 0x65, 0x5F, 0x7A, 0x6F, 0x6E, 0x65,
                0x2C, 0x20, 0x40, 0x40, 0x74, 0x69, 0x6D, 0x65, 0x5F, 0x7A, 0x6F, 0x6E, 0x65, 0x2C,
                0x20, 0x40, 0x40, 0x73, 0x71, 0x6C, 0x5F, 0x6D, 0x6F, 0x64, 0x65 };
        String result = "\n+--------------------------------------------------+\n"
                        + "|  0  1  2  3  4  5  6  7   8  9  a  b  c  d  e  f |\n"
                        + "+--------------------------------------------------+------------------+\n"
                        + "| 4A 00 00 00 03 53 45 4C  45 43 54 20 40 40 6D 61 | J....SELECT @@ma |\n"
                        + "| 78 5F 61 6C 6C 6F 77 65  64 5F 70 61 63 6B 65 74 | x_allowed_packet |\n"
                        + "| 20 2C 20 40 40 73 79 73  74 65 6D 5F 74 69 6D 65 |  , @@system_time |\n"
                        + "| 5F 7A 6F 6E 65 2C 20 40  40 74 69 6D 65 5F 7A 6F | _zone, @@time_zo |\n"
                        + "| 6E 65 2C 20 40 40 73 71  6C 5F 6D 6F 64 65       | ne, @@sql_mode   |\n"
                        + "+--------------------------------------------------+------------------+\n";
        assertEquals(result, Utils.hexdump(bb));
    }

    @Test
    public void sessionVariableParsing() {
        assertEquals("net_write_timeout=3600",
            Utils.parseSessionVariables("net_write_timeout=3600"));
        assertEquals("net_write,_timeout=3600",
            Utils.parseSessionVariables("net_write,_timeout=3600"));

        assertEquals("net_write_timeout=3600",
            Utils.parseSessionVariables(",;net_write_timeout=3600,"));
        assertEquals("net_write_timeout=3600,INSERT INTO USER",
            Utils.parseSessionVariables("net_write_timeout=3600;INSERT INTO USER"));
        assertEquals("net_write_timeout=3600,init_connect='SELECT 1;SELECT 2'",
            Utils.parseSessionVariables("net_write_timeout=3600;init_connect='SELECT 1;SELECT 2'"));
    }

    @Test
    public void localLocalParsing() throws SQLException, IOException {
        File file = new File("./test_localLocalParsing.txt");
        file.createNewFile();
        file.deleteOnExit();
        try {
            assertTrue(Utils.validateFileName("LOAD DATA LOCAL INFILE '" + file.toPath() + "' INTO TABLE test_tab", null, file.getPath()));
            assertTrue(Utils.validateFileName("LOAD DATA LOW_PRIORITY LOCAL INFILE '" + file.toPath() + "' INTO TABLE test_tab", null,
                    file.getPath()));
            assertTrue(Utils.validateFileName("LOAD DATA CONCURRENT LOCAL INFILE '" + file.toPath() + "' INTO TABLE test_tab", null,
                    file.getPath()));
            assertTrue(Utils.validateFileName("/*test*/ LOAD DATA LOCAL INFILE '" + file.toPath() + "' /*gni*/INTO TABLE test_tab",
                    null, file.getPath()));
            assertTrue(Utils.validateFileName("/*test*/ LOAD DATA LOCAL INFILE\n'" + file.toPath() + "' INTO TABLE test_tab",
                    null, file.getPath()));
            assertFalse(Utils.validateFileName("/*test*/ LOAD DATA LOCAL INFILE\n'/etc/tto/file_name'",
                    null, "file_name"));
            assertFalse(Utils.validateFileName("LOAD DATA INFILE '" + file.toPath() + "' /**/ INTO TABLE test_tab", null, "file_name"));
            ParameterHolder[] goodParameterHolders = new ParameterHolder[]{new StringParameter(
                    file.getPath(), false, "utf8")};
            assertTrue(Utils.validateFileName("LOAD DATA LOCAL INFILE ? INTO TABLE test_tab", goodParameterHolders,
                    file.getPath()));
            assertTrue(Utils.validateFileName("LOAD DATA LOW_PRIORITY LOCAL INFILE ? INTO TABLE test_tab",
                    goodParameterHolders, file.getPath()));
            assertTrue(Utils.validateFileName("LOAD DATA CONCURRENT LOCAL INFILE ? INTO TABLE test_tab",
                    goodParameterHolders, file.getPath()));
            assertTrue(Utils.validateFileName("/*test*/ LOAD DATA LOCAL INFILE ? /*gni*/ INTO TABLE test_tab",
                    goodParameterHolders, file.getPath()));
            ParameterHolder[] pathParameterHolders = new ParameterHolder[]{new StringParameter(
                    "/etc/tto/file_name", false, "utf8")};
            assertTrue(Utils.validateFileName("/*test*/ LOAD DATA LOCAL INFILE\n? INTO TABLE test_tab",
                    pathParameterHolders, "/etc/tto/file_name"));
            assertFalse(Utils.validateFileName("/*test*/ LOAD DATA LOCAL INFILE\n? INTO TABLE test_tab",
                    pathParameterHolders, file.getPath()));
            assertFalse(Utils.validateFileName("LOAD DATA INFILE ? /**/ INTO TABLE test_tab", goodParameterHolders,
                    file.getPath()));
        } finally {
            if (file != null && file.exists()) {
                file.delete();
            }
        }
    }

    @Test
    public void intToHexString() {
        assertEquals("05", Utils.intToHexString(5));
        assertEquals("0400", Utils.intToHexString(1024));
        assertEquals("C3C20186", Utils.intToHexString(-1010695802));
        assertEquals("FFFFFFFF", Utils.intToHexString(-1));
    }
}
