/*
 *
 * OceanBase Client for Java
 *
 * Copyright (c) 2021 OceanBase.
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 *
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * Copyright (c) 2009-2011, Marcus Eriksson
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 * Redistributions of source code must retain the above copyright notice, this list
 * of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice, this
 * list of conditions and the following disclaimer in the documentation and/or
 * other materials provided with the distribution.
 *
 * Neither the name of the driver nor the names of its contributors may not be
 * used to endorse or promote products derived from this software without specific
 * prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS  AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 * NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
 * OF SUCH DAMAGE.
 *
 */
package com.oceanbase.jdbc;

import static org.junit.Assert.*;

import java.sql.*;

import org.junit.*;

import com.oceanbase.jdbc.extend.datatype.TIMESTAMPLTZ;
import com.oceanbase.jdbc.extend.datatype.TIMESTAMPTZ;

/**
 * Checksum is a mechanism rather than an interface. Our test case can only test whether it can run normally after being turned on or off
 */
public class ChecksumOracleTest extends BaseOracleTest {
    /**
     * Tables initialisation.
     *
     * @throws SQLException exception
     */
    @BeforeClass()
    public static void initClass() throws SQLException {
        createTable(
            "test_batch1",
            "c1 int, c2 varchar(100), c3 date, c4 timestamp(9), c5 timestamp(9) with time zone, c6 timestamp(9) with local time zone");
        createTable(
            "test_batch2",
            "c1 int, c2 varchar(100), c3 date, c4 timestamp(9), c5 timestamp(9) with time zone, c6 timestamp(9) with local time zone");
        createTable(
            "test_batch3",
            "c1 int, c2 varchar(100), c3 date, c4 timestamp(9), c5 timestamp(9) with time zone, c6 timestamp(9) with local time zone");
    }

    public static void testChecksum(Connection conn, String tableName) {
        try {
            ResultSet rs = null;
            PreparedStatement ps = null;

            //            ps = conn
            //                .prepareStatement("  /* trace_id=1234567421568388972063772055923,rpc_id=0.33 *//* table=pmt_comm_fd_dtl_00,part_key=63 */ insert into "
            //                                  + tableName + " values (?,   ?,     ?, ?, ?, ?)    ");

            ps = conn.prepareStatement("insert into " + tableName
                                       + " values (?,   ?,     ?, ?, ?, ?)    ");

            for (int i = 0; i < 10; ++i) {
                ps.setInt(1, i);
                ps.setString(2, "value_" + String.format("%02d", i));
                ps.setTimestamp(3, Timestamp.valueOf("2018-12-26 16:54:19.878879"));
                ps.setTimestamp(4, Timestamp.valueOf("2018-12-26 16:54:19.878879"));
                ps.setTimestamp(5, Timestamp.valueOf("2018-12-26 16:54:19.878879"));
                ps.setTimestamp(6, Timestamp.valueOf("2018-12-26 16:54:19.878879"));
                ps.addBatch();
            }
            ps.executeBatch();
            ps.close();

            ps = conn.prepareStatement("select * from " + tableName + " where c1 = ?");
            for (int i = 0; i < 10; ++i) {
                ps.setInt(1, i);
                rs = ps.executeQuery();
                rs.next();
                assertEquals("2018-12-26 16:54:19.0", rs.getTimestamp("c3").toString());
                assertFalse(rs.wasNull());
                assertEquals("2018-12-26 16:54:19.878879", rs.getTimestamp("c4").toString());
                assertFalse(rs.wasNull());
                assertEquals("2018-12-26 16:54:19.878879", rs.getTimestamp("c5").toString());
                assertFalse(rs.wasNull());
                assertEquals("2018-12-26 16:54:19.878879", rs.getTimestamp("c6").toString());
                assertFalse(rs.wasNull());

                assertTrue(rs.getObject("c3") instanceof Timestamp);
                assertFalse(rs.wasNull());
                assertTrue(rs.getObject("c4") instanceof Timestamp);
                assertFalse(rs.wasNull());
                assertTrue(rs.getObject("c5") instanceof TIMESTAMPTZ);
                assertFalse(rs.wasNull());
                assertTrue(rs.getObject("c6") instanceof TIMESTAMPLTZ);
                assertFalse(rs.wasNull());
                rs.close();
                ps.clearParameters();
            }
            ps.close();

            // multi lines comment
            ps = conn.prepareStatement("-- t1\n-- t2\n-- t3\n insert into " + tableName
                                       + " values (?, ?, ?, ?, ?, ?)");
            for (int i = 11; i < 20; ++i) {
                ps.setInt(1, i);
                ps.setString(2, "value_" + String.format("%02d", i));
                ps.setTimestamp(3, Timestamp.valueOf("2018-12-26 16:54:19.878879"));
                ((BasePrepareStatement) ps).setTIMESTAMP(4,
                    Timestamp.valueOf("2018-12-26 16:54:19.878879"));
                ((BasePrepareStatement) ps).setTIMESTAMPTZ(5,
                    new TIMESTAMPTZ(conn, Timestamp.valueOf("2018-12-26 16:54:19.878879")));
                ((BasePrepareStatement) ps).setTIMESTAMPLTZ(6,
                    new TIMESTAMPLTZ(conn, Timestamp.valueOf("2018-12-26 16:54:19.878879")));
                ps.addBatch();
            }
            ps.executeBatch();
            ps.close();

            ps = conn.prepareStatement("select * from " + tableName + " where c1 = ?");
            for (int i = 11; i < 20; ++i) {
                ps.setInt(1, i);
                rs = ps.executeQuery();
                rs.next();
                assertEquals("2018-12-26 16:54:19.0", rs.getTimestamp("c3").toString());
                assertFalse(rs.wasNull());
                assertEquals("2018-12-26 16:54:19.878879", rs.getTimestamp("c4").toString());
                assertFalse(rs.wasNull());
                assertEquals("2018-12-26 16:54:19.878879", rs.getTimestamp("c5").toString());
                assertFalse(rs.wasNull());
                assertEquals("2018-12-26 16:54:19.878879", rs.getTimestamp("c6").toString());
                assertFalse(rs.wasNull());

                assertTrue(rs.getObject("c3") instanceof Timestamp);
                assertFalse(rs.wasNull());
                assertTrue(rs.getObject("c4") instanceof Timestamp);
                assertFalse(rs.wasNull());
                assertTrue(rs.getObject("c5") instanceof TIMESTAMPTZ);
                assertFalse(rs.wasNull());
                assertTrue(rs.getObject("c6") instanceof TIMESTAMPLTZ);
                assertFalse(rs.wasNull());
                rs.close();
                ps.clearParameters();
            }
            ps.close();

            // multi kinds of comment
            ps = conn.prepareStatement("-- t1\n-- t2\n-- t3\n/*t1, t2, t3*/ insert into "
                                       + tableName + " values (?, ?, ?, ?, ?, ?)");
            for (int i = 21; i < 30; ++i) {
                ps.setInt(1, i);
                ps.setString(2, "value_" + String.format("%02d", i));
                ps.setTimestamp(3, Timestamp.valueOf("2018-12-26 16:54:19.878879"));
                ((BasePrepareStatement) ps).setTIMESTAMP(4,
                    Timestamp.valueOf("2018-12-26 16:54:19.878879"));
                ((BasePrepareStatement) ps).setTIMESTAMPTZ(5,
                    new TIMESTAMPTZ(conn, Timestamp.valueOf("2018-12-26 16:54:19.878879")));
                ((BasePrepareStatement) ps).setTIMESTAMPLTZ(6,
                    new TIMESTAMPLTZ(conn, Timestamp.valueOf("2018-12-26 16:54:19.878879")));
                ps.addBatch();
            }
            ps.executeBatch();
            ps.close();

            ps = conn.prepareStatement("select * from " + tableName + " where c1 = ?");
            for (int i = 21; i < 30; ++i) {
                ps.setInt(1, i);
                rs = ps.executeQuery();
                rs.next();
                assertEquals("2018-12-26 16:54:19.0", rs.getTimestamp("c3").toString());
                assertFalse(rs.wasNull());
                assertEquals("2018-12-26 16:54:19.878879", rs.getTimestamp("c4").toString());
                assertFalse(rs.wasNull());
                assertEquals("2018-12-26 16:54:19.878879", rs.getTimestamp("c5").toString());
                assertFalse(rs.wasNull());
                assertEquals("2018-12-26 16:54:19.878879", rs.getTimestamp("c6").toString());
                assertFalse(rs.wasNull());

                assertTrue(rs.getObject("c3") instanceof Timestamp);
                assertFalse(rs.wasNull());
                assertTrue(rs.getObject("c4") instanceof Timestamp);
                assertFalse(rs.wasNull());
                assertTrue(rs.getObject("c5") instanceof TIMESTAMPTZ);
                assertFalse(rs.wasNull());
                assertTrue(rs.getObject("c6") instanceof TIMESTAMPLTZ);
                assertFalse(rs.wasNull());
                rs.close();
                ps.clearParameters();
            }
            ps.close();

            // multi kinds of comment
            ps = conn
                .prepareStatement("/*t1,t2,t3*/ -- t1\n-- t2\n-- t3\n/*t1, t2, t3*/ insert into "
                                  + tableName + " values (?, ?, ?, ?, ?, ?)");
            for (int i = 31; i < 40; ++i) {
                ps.setInt(1, i);
                ps.setString(2, "value_" + String.format("%02d", i));
                ps.setTimestamp(3, Timestamp.valueOf("2018-12-26 16:54:19.878879"));
                ((BasePrepareStatement) ps).setTIMESTAMP(4,
                    Timestamp.valueOf("2018-12-26 16:54:19.878879"));
                ((BasePrepareStatement) ps).setTIMESTAMPTZ(5,
                    new TIMESTAMPTZ(conn, Timestamp.valueOf("2018-12-26 16:54:19.878879")));
                ((BasePrepareStatement) ps).setTIMESTAMPLTZ(6,
                    new TIMESTAMPLTZ(conn, Timestamp.valueOf("2018-12-26 16:54:19.878879")));
                ps.addBatch();
            }
            ps.executeBatch();
            ps.close();

            ps = conn.prepareStatement("select * from " + tableName + " where c1 = ?");
            for (int i = 31; i < 40; ++i) {
                ps.setInt(1, i);
                rs = ps.executeQuery();
                rs.next();
                assertEquals("2018-12-26 16:54:19.0", rs.getTimestamp("c3").toString());
                assertFalse(rs.wasNull());
                assertEquals("2018-12-26 16:54:19.878879", rs.getTimestamp("c4").toString());
                assertFalse(rs.wasNull());
                assertEquals("2018-12-26 16:54:19.878879", rs.getTimestamp("c5").toString());
                assertFalse(rs.wasNull());
                assertEquals("2018-12-26 16:54:19.878879", rs.getTimestamp("c6").toString());
                assertFalse(rs.wasNull());

                assertTrue(rs.getObject("c3") instanceof Timestamp);
                assertFalse(rs.wasNull());
                assertTrue(rs.getObject("c4") instanceof Timestamp);
                assertFalse(rs.wasNull());
                assertTrue(rs.getObject("c5") instanceof TIMESTAMPTZ);
                assertFalse(rs.wasNull());
                assertTrue(rs.getObject("c6") instanceof TIMESTAMPLTZ);
                assertFalse(rs.wasNull());
                rs.close();
                ps.clearParameters();
            }
            ps.close();

            // Chinese
            ps = conn
                .prepareStatement("/*t1,t2,t3*/ -- t1\n-- t2\n-- t3\n/*t1, t2, t3*/ insert into "
                                  + tableName + " values (?, '麓云', ?, ?, ?, ?)");
            for (int i = 31; i < 40; ++i) {
                ps.setInt(1, i);
                ps.setTimestamp(2, Timestamp.valueOf("2018-12-26 16:54:19.878879"));
                ((BasePrepareStatement) ps).setTIMESTAMP(3,
                    Timestamp.valueOf("2018-12-26 16:54:19.878879"));
                ((BasePrepareStatement) ps).setTIMESTAMPTZ(4,
                    new TIMESTAMPTZ(conn, Timestamp.valueOf("2018-12-26 16:54:19.878879")));
                ((BasePrepareStatement) ps).setTIMESTAMPLTZ(5,
                    new TIMESTAMPLTZ(conn, Timestamp.valueOf("2018-12-26 16:54:19.878879")));
                ps.addBatch();
            }
            ps.executeBatch();
            ps.close();

            ps = conn
                .prepareStatement("select * from " + tableName + " where c1 = ? and c2 = '麓云'");
            for (int i = 31; i < 40; ++i) {
                ps.setInt(1, i);
                rs = ps.executeQuery();
                rs.next();
                assertEquals("2018-12-26 16:54:19.0", rs.getTimestamp("c3").toString());
                assertFalse(rs.wasNull());
                assertEquals("2018-12-26 16:54:19.878879", rs.getTimestamp("c4").toString());
                assertFalse(rs.wasNull());
                assertEquals("2018-12-26 16:54:19.878879", rs.getTimestamp("c5").toString());
                assertFalse(rs.wasNull());
                assertEquals("2018-12-26 16:54:19.878879", rs.getTimestamp("c6").toString());
                assertFalse(rs.wasNull());

                assertTrue(rs.getObject("c3") instanceof Timestamp);
                assertFalse(rs.wasNull());
                assertTrue(rs.getObject("c4") instanceof Timestamp);
                assertFalse(rs.wasNull());
                assertTrue(rs.getObject("c5") instanceof TIMESTAMPTZ);
                assertFalse(rs.wasNull());
                assertTrue(rs.getObject("c6") instanceof TIMESTAMPLTZ);
                assertFalse(rs.wasNull());
                rs.close();
                ps.clearParameters();
            }
            ps.close();
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    public static void testChecksumReflection(Connection conn, String tableName) {
        try {
            ResultSet rs = null;
            PreparedStatement ps = null;

            ps = conn
                .prepareStatement("  /* trace_id=1234567421568388972063772055923,rpc_id=0.33 *//* table=pmt_comm_fd_dtl_00,part_key=63 */ insert into "
                                  + tableName + " values (?,   ?,     ?, ?, ?, ?)    ");
            for (int i = 0; i < 10; ++i) {
                ps.setInt(1, i);
                ps.setString(2, "value_" + String.format("%02d", i));
                ps.setTimestamp(3, Timestamp.valueOf("2018-12-26 16:54:19.878879"));
                ps.setTimestamp(4, Timestamp.valueOf("2018-12-26 16:54:19.878879"));
                ps.setTimestamp(5, Timestamp.valueOf("2018-12-26 16:54:19.878879"));
                ps.setTimestamp(6, Timestamp.valueOf("2018-12-26 16:54:19.878879"));
                ps.addBatch();
            }
            ps.executeBatch();
            ps = conn
                .prepareStatement("  /* trace_id=1234567421568388972063772055923,rpc_id=0.33 *//* table=pmt_comm_fd_dtl_00,part_key=63 */ insert into "
                                  + tableName + " values (?,   ?,     ?, ?, ?, ?)    ");
            Class psClass = (Class) ps.getClass();
            java.lang.reflect.Field f = psClass.getDeclaredField("checksum");
            f.setAccessible(true);
            f.set(ps, 1222);
            for (int i = 0; i < 10; ++i) {
                ps.setInt(1, i);
                ps.setString(2, "value_" + String.format("%02d", i));
                ps.setTimestamp(3, Timestamp.valueOf("2018-12-26 16:54:19.878879"));
                ps.setTimestamp(4, Timestamp.valueOf("2018-12-26 16:54:19.878879"));
                ps.setTimestamp(5, Timestamp.valueOf("2018-12-26 16:54:19.878879"));
                ps.setTimestamp(6, Timestamp.valueOf("2018-12-26 16:54:19.878879"));
                ps.addBatch();
            }
            ps.executeBatch();
        } catch (Exception e) {
            e.printStackTrace();
            assertNotNull(e);
        }
    }

    /**
     * Test for useServerPsStmtChecksum=true , the default value is true.
     * @throws SQLException throw sql exception
     */
    @Test
    public void testChecksumOn() throws SQLException {

        Connection conn = sharedPSConnection;
        testChecksum(conn, "test_batch1");
    }

    /**
     * Test for useServerPsStmtChecksum=false
     * @throws SQLException throw sql exception
     */
    @Test
    public void testChecksumOff() throws SQLException {
        Connection conn = sharedPSConnectionWithoutChecksum;
        testChecksum(conn, "test_batch2");
    }

    @Test
    public void testChecksumReflection() throws SQLException {
        Assume.assumeTrue(sharedUsePrepare());
        Connection conn = sharedPSConnection;
        testChecksumReflection(conn, "test_batch3");
    }

}
