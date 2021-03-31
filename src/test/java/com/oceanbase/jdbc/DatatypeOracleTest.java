/**
 *  OceanBase Client for Java
 *
 *  Copyright (c) 2012-2014 Monty Program Ab.
 *  Copyright (c) 2015-2020 MariaDB Corporation Ab.
 *  Copyright (c) 2021 OceanBase Technology Co.,Ltd.
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

import java.sql.*;
import org.junit.BeforeClass;
import org.junit.Test;
import com.oceanbase.jdbc.extend.datatype.TIMESTAMPLTZ;
import com.oceanbase.jdbc.extend.datatype.TIMESTAMPTZ;

public class DatatypeOracleTest extends BaseOracleTest {

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
        createTable("test_number_ps", "id int ,tmp number(5,2) ");
        createTable("test_number_ps", "id int ,tmp number(5,2) ");
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
}
