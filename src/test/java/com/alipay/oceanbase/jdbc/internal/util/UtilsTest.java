package com.alipay.oceanbase.jdbc.internal.util;

import static org.junit.Assert.*;

import com.alipay.oceanbase.jdbc.internal.com.send.parameters.ParameterHolder;
import com.alipay.oceanbase.jdbc.internal.com.send.parameters.StringParameter;
import org.junit.Test;

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
    public void localLocalParsing() {
        assertTrue(Utils.validateFileName("LOAD DATA LOCAL INFILE 'file_name'", null, "file_name"));
        assertTrue(Utils.validateFileName("LOAD DATA LOW_PRIORITY LOCAL INFILE 'file_name'", null,
            "file_name"));
        assertTrue(Utils.validateFileName("LOAD DATA CONCURRENT LOCAL INFILE 'file_name'", null,
            "file_name"));
        assertTrue(Utils.validateFileName("/*test*/ LOAD DATA LOCAL INFILE 'file_name' /*gni*/",
            null, "file_name"));
        assertTrue(Utils.validateFileName("/*test*/ LOAD DATA LOCAL INFILE\n'/etc/tto/file_name'",
            null, "/etc/tto/file_name"));

        assertFalse(Utils.validateFileName("/*test*/ LOAD DATA LOCAL INFILE\n'/etc/tto/file_name'",
            null, "file_name"));
        assertFalse(Utils.validateFileName("LOAD DATA INFILE 'file_name' /**/", null, "file_name"));

        ParameterHolder[] goodParameterHolders = new ParameterHolder[] { new StringParameter(
            "file_name", false) };

        assertTrue(Utils.validateFileName("LOAD DATA LOCAL INFILE ?", goodParameterHolders,
            "file_name"));
        assertTrue(Utils.validateFileName("LOAD DATA LOW_PRIORITY LOCAL INFILE ?",
            goodParameterHolders, "file_name"));
        assertTrue(Utils.validateFileName("LOAD DATA CONCURRENT LOCAL INFILE ?",
            goodParameterHolders, "file_name"));
        assertTrue(Utils.validateFileName("/*test*/ LOAD DATA LOCAL INFILE ? /*gni*/",
            goodParameterHolders, "file_name"));
        ParameterHolder[] pathParameterHolders = new ParameterHolder[] { new StringParameter(
            "/etc/tto/file_name", false) };
        assertTrue(Utils.validateFileName("/*test*/ LOAD DATA LOCAL INFILE\n?",
            pathParameterHolders, "/etc/tto/file_name"));

        assertFalse(Utils.validateFileName("/*test*/ LOAD DATA LOCAL INFILE\n?",
            pathParameterHolders, "file_name"));
        assertFalse(Utils.validateFileName("LOAD DATA INFILE ? /**/", goodParameterHolders,
            "file_name"));
    }

    @Test
    public void intToHexString() {
        assertEquals("05", Utils.intToHexString(5));
        assertEquals("0400", Utils.intToHexString(1024));
        assertEquals("C3C20186", Utils.intToHexString(-1010695802));
        assertEquals("FFFFFFFF", Utils.intToHexString(-1));
    }
}
