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

import java.math.BigInteger;
import java.sql.*;

import org.junit.Test;

public class ScalarFunctionsTest extends BaseTest {

    @Test
    public void nativeSqlTest() throws SQLException {
        String exp;
        if (isMariadbServer() || minVersion(8, 0, 17)) {
            exp = "SELECT convert(foo(a,b,c), SIGNED INTEGER)"
                  + ", convert(convert(?, CHAR), SIGNED INTEGER)" + ", 1=?" + ", 1=?"
                  + ", convert(?,   SIGNED INTEGER   )" + ",  convert (?,   SIGNED INTEGER   )"
                  + ", convert(?, UNSIGNED INTEGER)" + ", convert(?, BINARY)"
                  + ", convert(?, BINARY)" + ", convert(?, BINARY)" + ", convert(?, BINARY)"
                  + ", convert(?, BINARY)" + ", convert(?, CHAR)" + ", convert(?, CHAR)"
                  + ", convert(?, CHAR)" + ", convert(?, CHAR)" + ", convert(?, CHAR)"
                  + ", convert(?, CHAR)" + ", convert(?, CHAR)" + ", convert(?, CHAR)"
                  + ", convert(?, CHAR)" + ", convert(?, CHAR)" + ", convert(?, CHAR)"
                  + ", convert(?, DOUBLE)" + ", convert(?, DOUBLE)" + ", convert(?, DECIMAL)"
                  + ", convert(?, DECIMAL)" + ", convert(?, DECIMAL)" + ", convert(?, DATETIME)"
                  + ", convert(?, DATETIME)";
        } else {
            exp = "SELECT convert(foo(a,b,c), SIGNED INTEGER)"
                  + ", convert(convert(?, CHAR), SIGNED INTEGER)" + ", 1=?" + ", 1=?"
                  + ", convert(?,   SIGNED INTEGER   )" + ",  convert (?,   SIGNED INTEGER   )"
                  + ", convert(?, UNSIGNED INTEGER)" + ", convert(?, BINARY)"
                  + ", convert(?, BINARY)" + ", convert(?, BINARY)" + ", convert(?, BINARY)"
                  + ", convert(?, BINARY)" + ", convert(?, CHAR)" + ", convert(?, CHAR)"
                  + ", convert(?, CHAR)" + ", convert(?, CHAR)" + ", convert(?, CHAR)"
                  + ", convert(?, CHAR)" + ", convert(?, CHAR)" + ", convert(?, CHAR)"
                  + ", convert(?, CHAR)" + ", convert(?, CHAR)" + ", convert(?, CHAR)" + ", 0.0+?"
                  + ", 0.0+?" + ", convert(?, DECIMAL)" + ", convert(?, DECIMAL)"
                  + ", convert(?, DECIMAL)" + ", convert(?, DATETIME)" + ", convert(?, DATETIME)";
        }

        assertEquals(exp,
            sharedConnection.nativeSQL("SELECT {fn convert(foo(a,b,c), SQL_BIGINT)}"
                                       + ", {fn convert({fn convert(?, SQL_VARCHAR)}, SQL_BIGINT)}"
                                       + ", {fn convert(?, SQL_BOOLEAN )}"
                                       + ", {fn convert(?, BOOLEAN)}"
                                       + ", {fn convert(?,   SMALLINT   )}"
                                       + ", {fn  convert (?,   TINYINT   )}"
                                       + ", {fn convert(?, SQL_BIT)}"
                                       + ", {fn convert(?, SQL_BLOB)}"
                                       + ", {fn convert(?, SQL_VARBINARY)}"
                                       + ", {fn convert(?, SQL_LONGVARBINARY)}"
                                       + ", {fn convert(?, SQL_ROWID)}"
                                       + ", {fn convert(?, SQL_BINARY)}"
                                       + ", {fn convert(?, SQL_NCHAR)}"
                                       + ", {fn convert(?, SQL_CLOB)}"
                                       + ", {fn convert(?, SQL_NCLOB)}"
                                       + ", {fn convert(?, SQL_DATALINK)}"
                                       + ", {fn convert(?, SQL_VARCHAR)}"
                                       + ", {fn convert(?, SQL_NVARCHAR)}"
                                       + ", {fn convert(?, SQL_LONGVARCHAR)}"
                                       + ", {fn convert(?, SQL_LONGNVARCHAR)}"
                                       + ", {fn convert(?, SQL_SQLXML)}"
                                       + ", {fn convert(?, SQL_LONGNCHAR)}"
                                       + ", {fn convert(?, SQL_CHAR)}"
                                       + ", {fn convert(?, SQL_FLOAT)}"
                                       + ", {fn convert(?, SQL_DOUBLE)}"
                                       + ", {fn convert(?, SQL_DECIMAL)}"
                                       + ", {fn convert(?, SQL_REAL)}"
                                       + ", {fn convert(?, SQL_NUMERIC)}"
                                       + ", {fn convert(?, SQL_TIMESTAMP)}"
                                       + ", {fn convert(?, SQL_DATETIME)}"));
    }

    @Test
    @SuppressWarnings("deprecation")
    public void scalarFctTest() throws SQLException {
        if (sharedUsePrepare()) {
            return;
        }
        if (!isMariadbServer()) {
            cancelForVersion(5, 5);
        }
        queryScalar("SELECT {fn convert(?, SQL_BIGINT)}", 2147483648L, 2147483648L);
        queryScalar("SELECT {fn convert(?, SQL_BIGINT)}", BigInteger.valueOf(2147483648L),
            2147483648L);
        queryScalar("SELECT {fn convert(?, SQL_BIGINT)}", 20, new Object[] { 20, 20L });
        queryScalar("SELECT {fn convert(?, SQL_BOOLEAN)}", true, new Object[] { 1, 1L });
        queryScalar("SELECT {fn convert(?, SQL_SMALLINT)}", 5000, new Object[] { 5000, 5000L });
        queryScalar("SELECT {fn convert(?, SQL_TINYINT)}", 5000, new Object[] { 5000, 5000L });
        queryScalar("SELECT {fn convert(?, SQL_BIT)}", 255,
            new Object[] { 255L, BigInteger.valueOf(255L) });
        queryScalar("SELECT {fn convert(?, SQL_BINARY)}", "test", "test".getBytes());
        queryScalar("SELECT {fn convert(?, SQL_DATETIME)}", "2020-12-31 12:13.15.12",
            new Timestamp(2020 - 1900, 11, 31, 12, 13, 15, 0));
    }

    private void queryScalar(String sql, Object val, Object res) throws SQLException {
    try (PreparedStatement prep = sharedConnection.prepareStatement(sql)) {
      prep.setObject(1, val);
      ResultSet rs = prep.executeQuery();
      assertTrue(rs.next());
      Object obj = rs.getObject(1);
      if (obj instanceof byte[]) {
        byte[] arr = (byte[]) obj;
        assertArrayEquals((byte[]) res, arr);
      } else if (res instanceof Object[]) {
        Object[] resArr = (Object[]) res;
        for (int i = 0; i < resArr.length; i++) {
          if (resArr[i].equals(obj)) {
            return;
          }
        }
        fail("not expected result");
      } else {
        assertEquals(res, rs.getObject(1));
      }
    }
  }

    @Test
    public void doubleBackslash() throws SQLException {
        Statement stmt = sharedConnection.createStatement();
        stmt.execute("DROP TABLE IF EXISTS TEST_SYNTAX_ERROR");
        stmt.execute("CREATE TABLE TEST_SYNTAX_ERROR("
                     + "     id INTEGER unsigned NOT NULL AUTO_INCREMENT, "
                     + "     str_value MEDIUMTEXT CHARACTER SET utf8mb4 NOT NULL,"
                     + "     json_value  MEDIUMTEXT CHARACTER SET utf8mb4 NOT NULL, "
                     + "    PRIMARY KEY ( id ))");
        stmt.execute("INSERT INTO TEST_SYNTAX_ERROR(str_value, json_value) VALUES ('abc\\\\', '{\"data\": \"test\"}')");
    }
}
