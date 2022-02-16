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

import static org.junit.Assert.assertEquals;

import java.sql.*;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;

public class ZonedDateTimeTest extends BaseOracleTest {
    static String zonedDateTimeTable = "zonedDataTimeTest";

    @BeforeClass
    public static void initClass() throws SQLException {
        createTable(
            zonedDateTimeTable,
            "TIMESTAMP_WTZ_TEST TIMESTAMP(8) WITH TIME ZONE, TIMESTAMP_WLTZ_TEST TIMESTAMP(8) WITH LOCAL TIME ZONE");
    }

    // cursorFetch error now !
    @Ignore
    public void zonedDateTimeTest() {
        try {
            PreparedStatement preparedStatement = sharedConnection
                .prepareStatement("insert into " + zonedDateTimeTable + " values(?,?)");
            LocalDateTime ldt = LocalDateTime.of(2019, 9, 13, 17, 16, 17, 920);

            ZonedDateTime zbj = ldt.atZone(ZoneId.systemDefault());
            ZonedDateTime zny = ldt.atZone(ZoneId.of("America/New_York"));
            preparedStatement.setObject(1, zbj);
            preparedStatement.setObject(2, zny, -101, 0);
            preparedStatement.execute();
            Timestamp timestamp = Timestamp.from(zbj.toInstant());
            Timestamp timestamp2 = Timestamp.from(zny.toInstant());
            ResultSet rs = sharedConnection.createStatement().executeQuery(
                "select * from " + zonedDateTimeTable);
            while (rs.next()) {
                Timestamp t1 = rs.getTimestamp(1);
                Timestamp t2 = rs.getTimestamp(2);
                // text protocol and binary protocol cursorFetch  not same
                assertEquals(t1.toString(), "2019-09-13 17:16:17.00000092");
                assertEquals(t2.toString(), "2019-09-14 05:16:17.00000092");
            }
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

}
