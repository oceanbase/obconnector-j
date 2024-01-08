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
/**
 * OceanBase Client for Java
 * <p>
 * Copyright (c) 2012-2014 Monty Program Ab.
 * Copyright (c) 2015-2020 MariaDB Corporation Ab.
 * Copyright (c) 2021 OceanBase.
 * <p>
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 * <p>
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 * <p>
 * You should have received a copy of the GNU Lesser General Public License along
 * with this library; if not, write to Monty Program Ab info@montyprogram.com.
 * <p>
 * This particular MariaDB Client for Java file is work
 * derived from a Drizzle-JDBC. Drizzle-JDBC file which is covered by subject to
 * the following copyright and notice provisions:
 * <p>
 * Copyright (c) 2009-2011, Marcus Eriksson
 * <p>
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 * Redistributions of source code must retain the above copyright notice, this list
 * of conditions and the following disclaimer.
 * <p>
 * Redistributions in binary form must reproduce the above copyright notice, this
 * list of conditions and the following disclaimer in the documentation and/or
 * other materials provided with the distribution.
 * <p>
 * Neither the name of the driver nor the names of its contributors may not be
 * used to endorse or promote products derived from this software without specific
 * prior written permission.
 * <p>
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
 */
package com.oceanbase.jdbc;

import static org.junit.Assert.*;

import java.math.BigDecimal;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.oceanbase.jdbc.extend.datatype.TIMESTAMPLTZ;
import com.oceanbase.jdbc.extend.datatype.TIMESTAMPTZ;

public class DataTypeOracleTest extends BaseOracleTest {

    /**
     * Tables initialisation.
     *
     * @throws SQLException exception
     */
    @BeforeClass()
    public static void initClass() throws SQLException {
        createTable("tt1_varchar", "id int , name varchar(20)");
        createTable("tt1_varchar2", "id int , name varchar2(20)");
        createTable("tt1_nvarchar2", "id int , name nvarchar2(20)");
        createTable("tt1_char", "id int , name char(20)");
        createTable("tt1_nchar", "id int , name nchar(20)");

        createTable("tt1_timestamp", "id int , tmp TIMESTAMP");
        createTable("tt1_timestamptz", "id int , tmp TIMESTAMP WITH TIME ZONE");
        createTable("tt1_timestampltz", "id int , tmp TIMESTAMP WITH LOCAL TIME ZONE");

        createTable("tt1_date", "id int , tmp date");

        createTable("tt1_number", "id int , tmp number");

        createTable("signedTinyIntTest", "id number(3)");
        createTable("signedSmallIntTest", "id number(5)");
        createTable("signedMediumIntTest", "id number(7)");
        createTable("signedIntTest", "id INT");
        createTable("signedBigIntTest", "id number(19)");
        createTable("testfloat", "id float(10)");

    }

    @Test
    public void TIMESTAMP() throws SQLException {
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        assertInsertSelectClientPs("insert into tt1_timestamp values(?,?)",
            new TO[] { TO.$n(T.INT, 1), TO.$n(T.TIMESTAMP, timestamp) },
            "select * from tt1_timestamp where id=?", new TO[] { TO.$n(T.INT, 1) },
            new TO[] { TO.$n(T.INT, 1), TO.$n(T.TIMESTAMP, timestamp) });

        assertInsertSelectClientPs("insert into tt1_timestamp values(?,?)",
            new TO[] { TO.$n(T.INT, 2), TO.$n(T.OBTIMESTAMP, timestamp) },
            "select * from tt1_timestamp where id=?", new TO[] { TO.$n(T.INT, 2) },
            new TO[] { TO.$n(T.INT, 2), TO.$n(T.OBTIMESTAMP, timestamp) });

        timestamp = new Timestamp(System.currentTimeMillis());
        assertInsertSelectServerPs("insert into tt1_timestamp values(?,?)",
            new TO[] { TO.$n(T.INT, 3), TO.$n(T.TIMESTAMP, timestamp) },
            "select * from tt1_timestamp where id=?", new TO[] { TO.$n(T.INT, 3) },
            new TO[] { TO.$n(T.INT, 3), TO.$n(T.TIMESTAMP, timestamp) });

        assertInsertSelectServerPs("insert into tt1_timestamp values(?,?)",
            new TO[] { TO.$n(T.INT, 4), TO.$n(T.OBTIMESTAMP, timestamp) },
            "select * from tt1_timestamp where id=?", new TO[] { TO.$n(T.INT, 4) },
            new TO[] { TO.$n(T.INT, 4), TO.$n(T.OBTIMESTAMP, timestamp) });

        assertInsertSelectClientPs("insert into tt1_timestamp values(?,?)",
            new TO[] { TO.$n(T.INT, 5), TO.$n(T.TIMESTAMP, null) },
            "select * from tt1_timestamp where id=?", new TO[] { TO.$n(T.INT, 5) },
            new TO[] { TO.$n(T.INT, 5), TO.$n(T.TIMESTAMP, null) });
        assertInsertSelectServerPs("insert into tt1_timestamp values(?,?)",
            new TO[] { TO.$n(T.INT, 6), TO.$n(T.TIMESTAMP, null) },
            "select * from tt1_timestamp where id=?", new TO[] { TO.$n(T.INT, 6) },
            new TO[] { TO.$n(T.INT, 6), TO.$n(T.TIMESTAMP, null) });
    }

    @Test
    public void TIMESTAMPTZ() throws SQLException {
        TIMESTAMPTZ timestamp = new TIMESTAMPTZ(null,
            Timestamp.valueOf("2018-12-26 16:54:19.878879"));
        assertInsertSelectClientPs("insert into tt1_timestamptz values(?,?)",
            new TO[] { TO.$n(T.INT, 1), TO.$n(T.OBTIMESTAMPTZ, timestamp) },
            "select * from tt1_timestamptz where id=?", new TO[] { TO.$n(T.INT, 1) },
            new TO[] { TO.$n(T.INT, 1), TO.$n(T.OBTIMESTAMPTZ, timestamp) });

        timestamp = new TIMESTAMPTZ(null, Timestamp.valueOf("2019-01-26 16:54:19.878879"));
        assertInsertSelectServerPs("insert into tt1_timestamptz values(?,?)",
            new TO[] { TO.$n(T.INT, 2), TO.$n(T.OBTIMESTAMPTZ, timestamp) },
            "select * from tt1_timestamptz where id=?", new TO[] { TO.$n(T.INT, 2) },
            new TO[] { TO.$n(T.INT, 2), TO.$n(T.OBTIMESTAMPTZ, timestamp) });

        assertInsertSelectClientPs("insert into tt1_timestamptz values(?,?)",
            new TO[] { TO.$n(T.INT, 5), TO.$n(T.OBTIMESTAMPTZ, null) },
            "select * from tt1_timestamptz where id=?", new TO[] { TO.$n(T.INT, 5) },
            new TO[] { TO.$n(T.INT, 5), TO.$n(T.OBTIMESTAMPTZ, null) });
        assertInsertSelectServerPs("insert into tt1_timestamptz values(?,?)",
            new TO[] { TO.$n(T.INT, 6), TO.$n(T.OBTIMESTAMPTZ, null) },
            "select * from tt1_timestamptz where id=?", new TO[] { TO.$n(T.INT, 6) },
            new TO[] { TO.$n(T.INT, 6), TO.$n(T.OBTIMESTAMPTZ, null) });
    }

    @Test
    public void TIMESTAMPLTZ() throws SQLException {
        TIMESTAMPLTZ timestamp = new TIMESTAMPLTZ(sharedConnection,
            Timestamp.valueOf("2018-12-26 16:54:19.878879"));
        assertInsertSelectClientPs("insert into tt1_timestampltz values(?,?)",
            new TO[] { TO.$n(T.INT, 1), TO.$n(T.OBTIMESTAMPLTZ, timestamp) },
            "select * from tt1_timestampltz where id=?", new TO[] { TO.$n(T.INT, 1) }, new TO[] {
                    TO.$n(T.INT, 1), TO.$n(T.OBTIMESTAMPLTZ, timestamp) });

        timestamp = new TIMESTAMPLTZ(sharedPSConnection,
            Timestamp.valueOf("2019-01-26 16:54:19.878879"));
        assertInsertSelectServerPs("insert into tt1_timestampltz values(?,?)",
            new TO[] { TO.$n(T.INT, 2), TO.$n(T.OBTIMESTAMPLTZ, timestamp) },
            "select * from tt1_timestampltz where id=?", new TO[] { TO.$n(T.INT, 2) }, new TO[] {
                    TO.$n(T.INT, 2), TO.$n(T.OBTIMESTAMPLTZ, timestamp) });

        assertInsertSelectClientPs("insert into tt1_timestampltz values(?,?)",
            new TO[] { TO.$n(T.INT, 5), TO.$n(T.OBTIMESTAMPLTZ, null) },
            "select * from tt1_timestampltz where id=?", new TO[] { TO.$n(T.INT, 5) }, new TO[] {
                    TO.$n(T.INT, 5), TO.$n(T.OBTIMESTAMPLTZ, null) });
        assertInsertSelectServerPs("insert into tt1_timestampltz values(?,?)",
            new TO[] { TO.$n(T.INT, 6), TO.$n(T.OBTIMESTAMPLTZ, null) },
            "select * from tt1_timestampltz where id=?", new TO[] { TO.$n(T.INT, 6) }, new TO[] {
                    TO.$n(T.INT, 6), TO.$n(T.OBTIMESTAMPLTZ, null) });
    }

    @Test
    public void DATE() throws SQLException { // FIXME should test use TIMESTAMP/TIMESTAMPTZ/TIMESTAMPLTZ dataype, but
        // use getDate method
        Date date = new Date(Timestamp.valueOf("2018-12-26 16:54:19.878879").getTime());
        assertInsertSelectClientPs("insert into tt1_date values(?,?)", new TO[] { TO.$n(T.INT, 1),
                TO.$n(T.DATE, date) }, "select * from tt1_date where id=?",
            new TO[] { TO.$n(T.INT, 1) }, new TO[] { TO.$n(T.INT, 1), TO.$n(T.DATE, date) });
        assertInsertSelectServerPs("insert into tt1_date values(?,?)", new TO[] { TO.$n(T.INT, 2),
                TO.$n(T.DATE, date) }, "select * from tt1_date where id=?",
            new TO[] { TO.$n(T.INT, 2) }, new TO[] { TO.$n(T.INT, 2), TO.$n(T.DATE, date) });

        assertInsertSelectClientPs("insert into tt1_date values(?,?)", new TO[] { TO.$n(T.INT, 5),
                TO.$n(T.DATE, null) }, "select * from tt1_date where id=?",
            new TO[] { TO.$n(T.INT, 5) }, new TO[] { TO.$n(T.INT, 5), TO.$n(T.DATE, null) });
        assertInsertSelectServerPs("insert into tt1_date values(?,?)", new TO[] { TO.$n(T.INT, 6),
                TO.$n(T.DATE, null) }, "select * from tt1_date where id=?",
            new TO[] { TO.$n(T.INT, 6) }, new TO[] { TO.$n(T.INT, 6), TO.$n(T.DATE, null) });
    }

    @Test
    public void varchar() throws SQLException {
        assertInsertSelect("insert into tt1_varchar values(1, 'one')", "select * from tt1_varchar",
            new TO[] { TO.$n(T.INT, 1), TO.$n(T.VARCHAR, "one") });
        assertInsertSelect("insert into tt1_varchar values(2, '')",
            "select * from tt1_varchar where id = 2", new TO[] { TO.$n(T.INT, 2), TO.$n(T.NULL) });
        assertInsertSelect("insert into tt1_varchar values(3, null)",
            "select * from tt1_varchar where id = 3", new TO[] { TO.$n(T.INT, 3), TO.$n(T.NULL) });
        assertInsertSelect("insert into tt1_varchar values(4, '张三')",
            "select * from tt1_varchar where id = 4",
            new TO[] { TO.$n(T.INT, 4), TO.$n(T.NVARCHAR2, "张三") });
    }

    @Test
    public void varchar2() throws SQLException {
        assertInsertSelect("insert into tt1_varchar2 values(1, 'one')",
            "select * from tt1_varchar2", new TO[] { TO.$n(T.INT, 1), TO.$n(T.VARCHAR2, "one") });
        assertInsertSelect("insert into tt1_varchar2 values(2, '')",
            "select * from tt1_varchar2 where id = 2", new TO[] { TO.$n(T.INT, 2), TO.$n(T.NULL) });
        assertInsertSelect("insert into tt1_varchar2 values(3, null)",
            "select * from tt1_varchar2 where id = 3", new TO[] { TO.$n(T.INT, 3), TO.$n(T.NULL) });
        assertInsertSelect("insert into tt1_varchar2 values(4, '张三')",
            "select * from tt1_varchar2 where id = 4",
            new TO[] { TO.$n(T.INT, 4), TO.$n(T.NVARCHAR2, "张三") });
    }

    @Test
    public void nvarchar2() throws SQLException {
        assertInsertSelect("insert into tt1_nvarchar2 values(1, 'one')",
            "select * from tt1_nvarchar2", new TO[] { TO.$n(T.INT, 1), TO.$n(T.NVARCHAR2, "one") });
        assertInsertSelect("insert into tt1_nvarchar2 values(2, '')",
            "select * from tt1_nvarchar2 where id = 2", new TO[] { TO.$n(T.INT, 2), TO.$n(T.NULL) });
        assertInsertSelect("insert into tt1_nvarchar2 values(3, null)",
            "select * from tt1_nvarchar2 where id = 3", new TO[] { TO.$n(T.INT, 3), TO.$n(T.NULL) });
        assertInsertSelect("insert into tt1_nvarchar2 values(4, '张三')",
            "select * from tt1_nvarchar2 where id = 4",
            new TO[] { TO.$n(T.INT, 4), TO.$n(T.NVARCHAR2, "张三") });
    }

    @Test
    public void test_char() throws SQLException {
        assertInsertSelect("insert into tt1_char values(1, 'one')", "select * from tt1_char",
            new TO[] { TO.$n(T.INT, 1), TO.$n(T.CHAR, rightPadding("one", 20)) });
        assertInsertSelect("insert into tt1_char values(2, '')",
            "select * from tt1_char where id = 2", new TO[] { TO.$n(T.INT, 2), TO.$n(T.NULL) });
        assertInsertSelect("insert into tt1_char values(3, null)",
            "select * from tt1_char where id = 3", new TO[] { TO.$n(T.INT, 3), TO.$n(T.NULL) });
        // TODO Need to consider the impact of character set .
        // insertAndSelect("insert into tt1_char values(4, '张三')", "select * from tt1_char where id =
        // 4", new TO[]{TO.$n(T.INT, 4),
        //        TO.$n(T.NVARCHAR2, rightPadding("张三", 20))});
    }

    @Test
    public void test_nchar() throws SQLException {
        assertInsertSelect("insert into tt1_nchar values(1, 'one')", "select * from tt1_nchar",
            new TO[] { TO.$n(T.INT, 1), TO.$n(T.CHAR, rightPadding("one", 20)) });
        assertInsertSelect("insert into tt1_nchar values(2, '')",
            "select * from tt1_nchar where id = 2", new TO[] { TO.$n(T.INT, 2), TO.$n(T.NULL) });
        assertInsertSelect("insert into tt1_nchar values(3, null)",
            "select * from tt1_nchar where id = 3", new TO[] { TO.$n(T.INT, 3), TO.$n(T.NULL) });
        // TODO Need to consider the impact of character set .
        assertInsertSelect("insert into tt1_nchar values(4, '张三')",
            "select * from tt1_nchar where id = 4",
            new TO[] { TO.$n(T.INT, 4), TO.$n(T.NVARCHAR2, rightPadding("张三", 20)) });
    }

    public String rightPadding(String inputString, int length) {
        if (inputString.length() >= length) {
            return inputString;
        }
        StringBuilder sb = new StringBuilder();
        sb.append(inputString);
        while (sb.length() < length) {
            sb.append(' ');
        }

        return sb.toString();
    }

    @Test
    public void test_number_ps() throws Exception {
        createTable("test_number_ps", "id int ,tmp number(5) ");
        Integer num = 2;
        assertInsertSelectClientPs("insert into test_number_ps values(?,?)",
            new TO[] { TO.$n(T.INT, 1), TO.$n(T.NUMBER, num) },
            "select * from test_number_ps where id=?", new TO[] { TO.$n(T.INT, 1) },
            new TO[] { TO.$n(T.INT, 1), TO.$n(T.NUMBER, num) });
        assertInsertSelectServerPs("insert into test_number_ps values(?,?)",
            new TO[] { TO.$n(T.INT, 2), TO.$n(T.NUMBER, num) },
            "select * from test_number_ps where id=?", new TO[] { TO.$n(T.INT, 2) },
            new TO[] { TO.$n(T.INT, 2), TO.$n(T.NUMBER, num) });
    }

    @Test
    public void test_number() throws SQLException {
        assertInsertSelect("insert into tt1_number values(1, 3)", "select id,tmp from tt1_number",
            new TO[] { TO.$n(T.INT, 1), TO.$n(T.NUMBER, 3) });
    }

    @Test
    public void test_number_float_ps() throws Exception {
        createTable("test_number_float_ps", "id int ,tmp number(5,2) ");
        float num = 5.3f;
        assertInsertSelectClientPs("insert into test_number_float_ps values(?,?)",
            new TO[] { TO.$n(T.INT, 1), TO.$n(T.NUMBER_FLOAT, num) },
            "select id,tmp from test_number_float_ps where id=?", new TO[] { TO.$n(T.INT, 1) },
            new TO[] { TO.$n(T.INT, 1), TO.$n(T.NUMBER_FLOAT, num) });
        assertInsertSelectServerPs("insert into test_number_float_ps values(?,?)",
            new TO[] { TO.$n(T.INT, 2), TO.$n(T.NUMBER_FLOAT, num) },
            "select id,tmp from test_number_float_ps where id=?", new TO[] { TO.$n(T.INT, 2) },
            new TO[] { TO.$n(T.INT, 2), TO.$n(T.NUMBER_FLOAT, num) });
    }

    @Test
    public void test_number_float() throws SQLException {
        createTable("test_number_float", "id int ,tmp number(10,2)");
        assertInsertSelect("insert into test_number_float values(1, 2.43)",
            "select * from test_number_float",
            new TO[] { TO.$n(T.INT, 1), TO.$n(T.NUMBER_FLOAT, 2.43f) });
    }

    @Test
    public void test_binary_float() throws SQLException {
        createTable("test_binary_float", "id int ,tmp number(10,2)");
        assertInsertSelect("insert into test_binary_float values(1, 1.23)",
            "select * from test_binary_float",
            new TO[] { TO.$n(T.INT, 1), TO.$n(T.BINARY_FLOAT, 1.23f) });
    }

    @Test
    public void test_binary_float_ps() throws Exception {
        createTable("test_binary_float_ps", "id int ,tmp number(5,3) ");
        float num = 2.345f;
        assertInsertSelectClientPs("insert into test_binary_float_ps values(?,?)",
            new TO[] { TO.$n(T.INT, 1), TO.$n(T.BINARY_FLOAT, num) },
            "select id,tmp from test_binary_float_ps where id=?", new TO[] { TO.$n(T.INT, 1) },
            new TO[] { TO.$n(T.INT, 1), TO.$n(T.BINARY_FLOAT, num) });
        assertInsertSelectServerPs("insert into test_binary_float_ps values(?,?)",
            new TO[] { TO.$n(T.INT, 2), TO.$n(T.BINARY_FLOAT, num) },
            "select id,tmp from test_binary_float_ps where id=?", new TO[] { TO.$n(T.INT, 2) },
            new TO[] { TO.$n(T.INT, 2), TO.$n(T.BINARY_FLOAT, num) });
    }

    @Test
    public void test_binary_double() throws SQLException {
        createTable("test_binary_double", "id int ,tmp number(10,3)");
        assertInsertSelect("insert into test_binary_double values(1, 1.234)",
            "select * from test_binary_double",
            new TO[] { TO.$n(T.INT, 1), TO.$n(T.BINARY_DOUBLE, 1.234d) });
    }

    @Test
    public void test_binary_double_ps() throws Exception {
        createTable("test_binary_double_ps", "id int ,tmp number(5,3) ");
        double num = 2.345d;
        assertInsertSelectClientPs("insert into test_binary_double_ps values(?,?)",
            new TO[] { TO.$n(T.INT, 1), TO.$n(T.BINARY_DOUBLE, num) },
            "select id,tmp from test_binary_double_ps where id=?", new TO[] { TO.$n(T.INT, 1) },
            new TO[] { TO.$n(T.INT, 1), TO.$n(T.BINARY_DOUBLE, num) });
        assertInsertSelectServerPs("insert into test_binary_double_ps values(?,?)",
            new TO[] { TO.$n(T.INT, 2), TO.$n(T.BINARY_DOUBLE, num) },
            "select id,tmp from test_binary_double_ps where id=?", new TO[] { TO.$n(T.INT, 2) },
            new TO[] { TO.$n(T.INT, 2), TO.$n(T.BINARY_DOUBLE, num) });
    }

    @Test
    public void testBinaryFloat() {
        try {
            createTable("testBinaryFloat", "c1 binary_float ");
            String sql = "insert into testBinaryFloat (c1) values(1.9),(1.9f),(1);";
            assertEquals(3, sharedConnection.createStatement().executeUpdate(sql));
            ResultSet rs = sharedConnection.prepareStatement("select c1  from testBinaryFloat")
                .executeQuery();
            BigDecimal[] bigDecimals = new BigDecimal[] { BigDecimal.valueOf(1.9f),
                    BigDecimal.valueOf(1.9f), BigDecimal.valueOf(1) };
            int i = 0;
            while (rs.next()) {
                System.out.println("i = " + i);
                assertTrue(bigDecimals[i++].compareTo(rs.getBigDecimal(1)) == 0);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testBinaryDouble() {
        try {
            createTable("testBinaryDouble", "c1 binary_double ");
            String sql = "insert into testBinaryDouble (c1) values(1.9),(1.9f),(1);";
            assertEquals(3, sharedConnection.createStatement().executeUpdate(sql));
            ResultSet rs = sharedConnection.prepareStatement("select c1  from testBinaryDouble")
                .executeQuery();
            BigDecimal[] bigDecimals = new BigDecimal[] { BigDecimal.valueOf(1.9d),
                    BigDecimal.valueOf(1.9f), BigDecimal.valueOf(1) };
            int i = 0;
            while (rs.next()) {
                System.out.println("i = " + i);
                assertTrue(bigDecimals[i++].compareTo(rs.getBigDecimal(1)) == 0);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void unsignedTinyIntTest() throws SQLException {
        sharedConnection.createStatement().execute("insert into signedTinyIntTest values (120)");
        sharedConnection.createStatement().execute("insert into signedTinyIntTest values (1)");
        sharedConnection.createStatement().execute("insert into signedTinyIntTest values (null)");
        sharedConnection.createStatement().execute("insert into signedTinyIntTest values (-1)");
        try (ResultSet rs = getResultSet("select * from signedTinyIntTest", false)) {
            assertTrue(signedTinyIntTestResult(rs));
        }

        try (ResultSet rs = getResultSet("select * from signedTinyIntTest", true)) {
            assertTrue(signedTinyIntTestResult(rs));
        }
    }

    /**
     * Get a simple Statement or a PrepareStatement.
     *
     * @param query    query
     * @param prepared flag must be a prepare statement
     * @return a statement
     * @throws SQLException exception
     */
    public static ResultSet getResultSet(String query, boolean prepared) throws SQLException {
        return getResultSet(query, prepared, sharedConnection);
    }

    /**
     * Get a simple Statement or a PrepareStatement.
     *
     * @param query      query
     * @param prepared   flag must be a prepare statement
     * @param connection the connection to use
     * @return a statement
     * @throws SQLException exception
     */
    public static ResultSet getResultSet(String query, boolean prepared, Connection connection)
                                                                                               throws SQLException {
        if (prepared) {
            PreparedStatement preparedStatement = connection.prepareStatement(query
                                                                              + " WHERE 1 = ?");
            preparedStatement.setInt(1, 1);
            return preparedStatement.executeQuery();
        } else {
            return connection.createStatement().executeQuery(query);
        }
    }

    private boolean signedTinyIntTestResult(ResultSet rs) throws SQLException {
        if (rs.next()) {
            assertEquals(120, rs.getByte(1));
            assertEquals(120, rs.getShort(1));
            assertEquals(120, rs.getInt(1));
            assertEquals(120L, rs.getLong(1));
            assertEquals(120D, rs.getDouble(1), .000001);
            assertEquals(120F, rs.getFloat(1), .000001);
            assertEquals("120", rs.getString(1));
            assertEquals(new BigDecimal("120"), rs.getBigDecimal(1));
            if (rs.next()) {
                oneNullNegativeTest(rs);
                return true;
            } else {
                fail("must have result !");
            }
        } else {
            fail("must have result !");
        }
        return false;
    }

    @Test
    public void signedSmallIntTest() throws SQLException {
        sharedConnection.createStatement().execute("insert into signedSmallIntTest values (32767)");
        sharedConnection.createStatement().execute("insert into signedSmallIntTest values (1)");
        sharedConnection.createStatement().execute("insert into signedSmallIntTest values (null)");
        sharedConnection.createStatement().execute("insert into signedSmallIntTest values (-1)");

        try (ResultSet rs = getResultSet("select * from signedSmallIntTest", false)) {
            signedSmallIntTestResult(rs);
        }

        try (ResultSet rs = getResultSet("select * from signedSmallIntTest", true)) {
            signedSmallIntTestResult(rs);
        }
    }

    private void signedSmallIntTestResult(ResultSet rs) throws SQLException {
        if (rs.next()) {
            byteMustFail(rs);
            assertEquals(32767, rs.getShort(1));
            assertEquals(32767, rs.getInt(1));
            assertEquals(32767L, rs.getLong(1));
            assertEquals(32767D, rs.getDouble(1), .000001);
            assertEquals(32767F, rs.getFloat(1), .000001);
            assertEquals(new BigDecimal("32767"), rs.getBigDecimal(1));
            assertEquals("32767", rs.getString(1));
            if (rs.next()) {
                oneNullNegativeTest(rs);
            } else {
                fail("must have result !");
            }
        } else {
            fail("must have result !");
        }
    }

    @Test
    public void signedMediumIntTest() throws SQLException {
        sharedConnection.createStatement().execute("insert into signedMediumIntTest values (8388607)");
        sharedConnection.createStatement().execute("insert into signedMediumIntTest values (1)");
        sharedConnection.createStatement().execute("insert into signedMediumIntTest values (null)");
        sharedConnection.createStatement().execute("insert into signedMediumIntTest values (-1)");

        try (ResultSet rs = getResultSet("select * from signedMediumIntTest", false)) {
            signedMediumIntTestResult(rs);
        }

        try (ResultSet rs = getResultSet("select * from signedMediumIntTest", true)) {
            signedMediumIntTestResult(rs);
        }
    }

    private void signedMediumIntTestResult(ResultSet rs) throws SQLException {
        if (rs.next()) {
            byteMustFail(rs);
            shortMustFail(rs);
            assertEquals(8388607, rs.getInt(1));
            assertEquals(8388607L, rs.getLong(1));
            assertEquals(8388607D, rs.getDouble(1), .000001);
            assertEquals(8388607F, rs.getFloat(1), .000001);
            assertEquals(new BigDecimal("8388607"), rs.getBigDecimal(1));
            assertEquals("8388607", rs.getString(1));
            if (rs.next()) {
                oneNullNegativeTest(rs);
            } else {
                fail("must have result !");
            }
        } else {
            fail("must have result !");
        }
    }

    @Test
    public void signedIntTest() throws SQLException {
        sharedConnection.createStatement().execute("insert into signedIntTest values (2147483647)");
        sharedConnection.createStatement().execute("insert into signedIntTest values (1)");
        sharedConnection.createStatement().execute("insert into signedIntTest values (null)");
        sharedConnection.createStatement().execute("insert into signedIntTest values (-1)");
        try (ResultSet rs = getResultSet("select * from signedIntTest", false)) {
            signedIntTestResult(rs);
        }

        try (ResultSet rs = getResultSet("select * from signedIntTest", true)) {
            signedIntTestResult(rs);
        }
    }

    private void signedIntTestResult(ResultSet rs) throws SQLException {
        if (rs.next()) {
            byteMustFail(rs);
            shortMustFail(rs);
            assertEquals(2147483647, rs.getInt(1));
            assertEquals(2147483647L, rs.getLong(1));
            assertEquals(2147483647D, rs.getDouble(1), .000001);
            assertEquals(2147483647F, rs.getFloat(1), .000001);
            assertEquals(new BigDecimal("2147483647"), rs.getBigDecimal(1));
            assertEquals("2147483647", rs.getString(1));
            if (rs.next()) {
                oneNullNegativeTest(rs);
            } else {
                fail("must have result !");
            }
        } else {
            fail("must have result !");
        }
    }

    @Test
    public void signedBigIntTest() throws SQLException {
        sharedConnection
                .createStatement()
                .execute("insert into signedBigIntTest values (9223372036854775807)");
        sharedConnection.createStatement().execute("insert into signedBigIntTest values (1)");
        sharedConnection.createStatement().execute("insert into signedBigIntTest values (null)");
        sharedConnection.createStatement().execute("insert into signedBigIntTest values (-1)");

        try (ResultSet rs = getResultSet("select * from signedBigIntTest", false)) {
            signedBigIntTestResult(rs);
        }

        try (ResultSet rs = getResultSet("select * from signedBigIntTest", true)) {
            signedBigIntTestResult(rs);
        }
    }

    private void signedBigIntTestResult(ResultSet rs) throws SQLException {
        if (rs.next()) {
            byteMustFail(rs);
            shortMustFail(rs);
            intMustFail(rs);
            assertEquals(9223372036854775807L, rs.getLong(1));
            assertEquals(9223372036854775807F, rs.getFloat(1), .000001);
            assertEquals(9223372036854775807D, rs.getDouble(1), .000001);
            assertEquals(new BigDecimal("9223372036854775807"), rs.getBigDecimal(1));
            assertEquals("9223372036854775807", rs.getString(1));
            if (rs.next()) {
                oneNullNegativeTest(rs);
            } else {
                fail("must have result !");
            }
        } else {
            fail("must have result !");
        }
    }

    private void byteMustFail(ResultSet rs) {
        try {
            rs.getByte(1);
            fail("getByte must have thrown error !");
        } catch (SQLException e) {
            assertEquals("22003", e.getSQLState());
        }
    }

    private void shortMustFail(ResultSet rs) {
        try {
            rs.getShort(1);
            fail("getShort must have thrown error !");
        } catch (SQLException e) {
            assertEquals("22003", e.getSQLState());
        }
    }

    private void intMustFail(ResultSet rs) {
        try {
            rs.getInt(1);
            fail("getInt must have thrown error !");
        } catch (SQLException e) {
            assertEquals("22003", e.getSQLState());
        }
    }

    private void longMustFail(ResultSet rs) {
        try {
            rs.getLong(1);
            fail("getLong must have thrown error !");
        } catch (SQLException e) {
            assertEquals("22003", e.getSQLState());
        }
    }

    private void oneNullNegativeTest(ResultSet rs) throws SQLException {
        oneNullNegativeTest(rs, false, false);
    }

    private void oneNullNegativeTest(ResultSet rs, boolean decimal, boolean floatingPoint)
                                                                                          throws SQLException {
        try {
            if (!decimal && !floatingPoint) {
                assertTrue(rs.getBoolean(1));
            }
            assertEquals(1, rs.getByte(1));
            assertEquals(1, rs.getShort(1));
            assertEquals(1, rs.getInt(1));
            assertEquals(1L, rs.getLong(1));
            assertEquals(1D, rs.getDouble(1), .000001);
            assertEquals(1F, rs.getFloat(1), .000001);
            if (decimal) {
                if (floatingPoint) {
                    BigDecimal bd = rs.getBigDecimal(1);
                    if (!bd.equals(new BigDecimal("1")) && !bd.equals(new BigDecimal("1.0"))) {
                        fail("getBigDecimal error : is " + bd.toString());
                    }
                    assertEquals("1.0", rs.getString(1));

                } else {
                    assertEquals(new BigDecimal("1.00000000000000000000"), rs.getBigDecimal(1));
                    assertEquals("1.00000000000000000000", rs.getString(1));
                }
            } else {
                assertEquals(new BigDecimal("1"), rs.getBigDecimal(1));
                assertEquals("1", rs.getString(1));
            }
        } catch (SQLException e) {
            e.printStackTrace();
            fail("must not have thrown error");
        }

        if (rs.next()) {
            nullNegativeTest(rs, decimal);
        } else {
            fail("must have result !");
        }
    }

    private void nullNegativeTest(ResultSet rs, boolean decimal) throws SQLException {
        try {
            assertFalse(rs.getBoolean(1));
            assertEquals(0, rs.getByte(1));
            assertTrue(rs.wasNull());
            assertEquals(0, rs.getShort(1));
            assertEquals(0, rs.getInt(1));
            assertEquals(0, rs.getLong(1));
            assertEquals(0, rs.getDouble(1), .00001);
            assertEquals(0, rs.getFloat(1), .00001);
            assertNull(rs.getBigDecimal(1));
            assertNull(rs.getString(1));

        } catch (SQLException e) {
            e.printStackTrace();
            fail("must not have thrown error");
        }

        if (rs.next()) {
            try {
                assertTrue(rs.getBoolean(1));
                assertFalse(rs.wasNull());
                assertEquals(-1, rs.getByte(1));
                assertEquals(-1, rs.getShort(1));
                assertEquals(-1, rs.getInt(1));
                assertEquals(-1, rs.getLong(1));
                assertEquals(-1, rs.getDouble(1), .00001);
                assertEquals(-1, rs.getFloat(1), .00001);
                if (decimal) {
                    assertTrue(new BigDecimal("-1.00000000000000000000")
                        .equals(rs.getBigDecimal(1)));
                    assertEquals("-1.00000000000000000000", rs.getString(1));
                } else {
                    assertTrue(new BigDecimal("-1").equals(rs.getBigDecimal(1)));
                    assertEquals("-1", rs.getString(1));
                }

            } catch (SQLException e) {
                e.printStackTrace();
                fail("must not have thrown error");
            }
        } else {
            fail("must have result !");
        }
    }

    @Test
    public void testAllDataType() {
        Connection conn = null;
        Map<String, Object> map = new HashMap<String, Object>();
        for (int i = 0; i <= 1; i++) {
            try {
                if (i == 0) {
                    conn = sharedConnection;
                } else {
                    conn = getOracleConnection();
                }

                Statement statement = conn.createStatement();

                try {
                    statement.execute("drop table ALL_DATATYPE_TEST");
                } catch (SQLException e) {
                }

                statement.execute("create table ALL_DATATYPE_TEST(\n" + " INT_TEST INT,\n"
                                  + " VARCHAR2_TEST VARCHAR2(32),\n"
                                  + " NVARCHAR2_TEST NVARCHAR2(32),\n"
                                  + " NUMBER_TEST NUMBER(9,5),\n" + " FLOAT_TEST FLOAT(32),\n"
                                  + " DATE_TEST DATE,\n" + " BINARY_FLOAT_TEST BINARY_FLOAT,\n"
                                  + " BINARY_DOUBLE_TEST BINARY_DOUBLE,\n" + " ROWID_TEST ROWID,\n"
                                  + " UROWID_TEST UROWID(4000),\n" + " CHAR_TEST CHAR(128),\n"
                                  + " NCHAR_TEST NCHAR(128),\n" + " CLOB_TEST CLOB,\n"
                                  + " BLOB_TEST BLOB,\n" + " TIMESTAMP_TEST TIMESTAMP,\n"
                                  + " TIMESTAMP_WTZ_TEST TIMESTAMP(8) WITH TIME ZONE,\n"
                                  + " RAW_TEST RAW(100),\n"
                                  + " TIMESTAMP_WLTZ_TEST TIMESTAMP(8) WITH LOCAL TIME ZONE,\n"
                                  + " INTERVAL_DAY_TO_SECOND_TEST INTERVAL DAY TO SECOND,\n"
                                  + " INTERVAL_YEAR_TO_MONTH_TEST INTERVAL YEAR TO MONTH\n" + ")");

                ResultSet rs = statement.executeQuery("select * FROM ALL_DATATYPE_TEST");

                for (int p = 1; p <= 19; p++) {
                    String columnTypeName = rs.getMetaData().getColumnTypeName(p);
                    int columnType = rs.getMetaData().getColumnType(p);
                    if (i == 0) {
                        map.put("columnTypeName" + p, columnTypeName);
                        map.put("columnType" + p, columnType);
                    } else {
                        System.out
                            .println("column["
                                     + p
                                     + "] --------------------------------------------------------------");
                        System.out.println("Oracle      " + "\t[TypeName|" + columnTypeName
                                           + "] \t[TypeValue|" + columnType + "] \t");
                        System.out.println("OB-Oracle   " + "\t[TypeName|"
                                           + map.get("columnTypeName" + p) + "] \t[TypeValue|"
                                           + map.get("columnType" + p) + "] \t");
                        assertEquals(columnTypeName, map.get("columnTypeName" + p));
                        assertEquals(columnType, map.get("columnType" + p));
                    }
                }

            } catch (Exception e) {
                e.printStackTrace();
                Assert.fail();
            }
        }
    }

    @Test
    public void testFloatGetLong() throws SQLException {
        createTable("test_float_getLong", "v0 float(126) , v1 binary_float , v2 number");
        Statement stmt = sharedConnection.createStatement();
        stmt.execute("insert into test_float_getLong values (0.035 , 0.035 , 0.035)");
        ResultSet rs = stmt.executeQuery("select * from test_float_getLong");
        ResultSetMetaData metaData = rs.getMetaData();
        assertEquals(2, metaData.getColumnType(1));
        assertEquals(100, metaData.getColumnType(2));
        assertEquals("NUMBER", metaData.getColumnTypeName(1));
        assertEquals("BINARY_FLOAT", metaData.getColumnTypeName(2));
        try {
            assertEquals("java.lang.Double", metaData.getColumnClassName(1));
        } catch (AssertionError e) {
            //todo ojdbc6/8 is java.lang.Double here
            System.out.println(e.getMessage());
        }
        assertEquals("java.lang.Float", metaData.getColumnClassName(2));
        assertTrue(rs.next());
        assertEquals(0, rs.getLong(1));
        assertEquals(0, rs.getLong(2));
        assertEquals(0, rs.getLong(3));
    }

}
