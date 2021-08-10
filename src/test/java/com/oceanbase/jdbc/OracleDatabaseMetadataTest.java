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

import static org.junit.Assert.assertEquals;

import java.sql.*;
import java.util.Locale;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @Author : wb-xsy @Description @Date : 2020/9/25 16:48
 */
public class OracleDatabaseMetadataTest extends BaseOracleTest {
    static String tableName2 = "getIndexInfoTest" + getRandomString(10);

    /**
     * Initialisation.
     *
     * @throws SQLException exception
     */
    @BeforeClass()
    public static void initClass() throws SQLException {
        createTable("quote_test", "id int primary key , create_time timestamp");
        createTable(tableName2, "no INT NOT NULL ,\n" + "    product_category INT NOT NULL,\n"
                                + "    product_id INT NOT NULL,\n"
                                + "    customer_id INT NOT NULL,\n" + "    PRIMARY KEY(no)\n");
    }

    @Before
    public void checkSupported() throws SQLException {
        requireMinimumVersion(5, 1);
    }

    @Test
    public void getIdentifierQuoteStringTest() throws SQLException {
        Statement stmt = null;
        ResultSet rs = null;
        stmt = sharedConnection.createStatement();
        stmt.executeUpdate("insert into quote_test values(1,sysdate)");
        rs = stmt.executeQuery("select ID,CREATE_TIME from quote_test");
        assertEquals("ID", rs.getMetaData().getColumnName(1));
        assertEquals("CREATE_TIME", rs.getMetaData().getColumnName(2));
        assertEquals(true, rs.next());
        assertEquals("\"", sharedConnection.getMetaData().getIdentifierQuoteString());
    }

    @Test
    public void getSQLKeywordsTest() throws SQLException {
        assertEquals(
            "ACCESS, ADD, ALTER, AUDIT, CLUSTER, COLUMN, COMMENT, COMPRESS, CONNECT, DATE, DROP, EXCLUSIVE, FILE, IDENTIFIED, IMMEDIATE, INCREMENT, INDEX, INITIAL, INTERSECT, LEVEL, LOCK, LONG, MAXEXTENTS, MINUS, MODE, NOAUDIT, NOCOMPRESS, NOWAIT, NUMBER, OFFLINE, ONLINE, PCTFREE, PRIOR, all_PL_SQL_reserved_ words",
            sharedConnection.getMetaData().getSQLKeywords());
    }

    @Test
    public void getNumericFunctionsTest() throws SQLException {
        assertEquals(
            "ABS,ACOS,ASIN,ATAN,ATAN2,CEILING,COS,EXP,FLOOR,LOG,LOG10,MOD,PI,POWER,ROUND,SIGN,SIN,SQRT,TAN,TRUNCATE",
            sharedConnection.getMetaData().getNumericFunctions());
    }

    @Test
    public void getStringFunctionsTest() throws SQLException {
        assertEquals(
            "ASCII,CHAR,CHAR_LENGTH,CHARACTER_LENGTH,CONCAT,LCASE,LENGTH,LTRIM,OCTET_LENGTH,REPLACE,RTRIM,SOUNDEX,SUBSTRING,UCASE",
            sharedConnection.getMetaData().getStringFunctions());
    }

    @Test
    public void getSystemFunctionsTest() throws SQLException {
        assertEquals("USER", sharedConnection.getMetaData().getSystemFunctions());
    }

    @Test
    public void getTimeDateFunctionsTest() throws SQLException {
        assertEquals(
            "CURRENT_DATE,CURRENT_TIMESTAMP,CURDATE,EXTRACT,HOUR,MINUTE,MONTH,SECOND,YEAR",
            sharedConnection.getMetaData().getTimeDateFunctions());
    }

    @Test
    public void getIndexInfoTest() {
        try {
            Connection connection = sharedConnection;
            ResultSet rs = connection.getMetaData().getIndexInfo(null, null,
                tableName2.toUpperCase(Locale.ROOT), false, false); // same as oracle driver ,Need to use uppercase table name
            int columnCount = rs.getMetaData().getColumnCount();
            Assert.assertEquals(13, columnCount);
            ResultSetMetaData resultSetMetaData = rs.getMetaData();
            Assert.assertTrue(rs.next());
            for (int i = 1; i <= columnCount; i++) {
                System.out.print("column " + resultSetMetaData.getColumnName(i));
                System.out.println(" value = " + rs.getString(i));
            }
            Assert.assertEquals("TABLE_SCHEM", resultSetMetaData.getColumnName(2));
            Assert.assertEquals("UNITTESTS", rs.getString(2));
            Assert.assertEquals("TABLE_NAME", resultSetMetaData.getColumnName(3));
            Assert.assertEquals(tableName2.toUpperCase(Locale.ROOT), rs.getString(3));
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }
}
