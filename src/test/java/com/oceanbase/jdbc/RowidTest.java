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

import java.sql.*;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class RowidTest extends BaseOracleTest {
    @BeforeClass()
    public static void initClass() throws SQLException {
        createTable("test_rowid", "col1 int, col2 varchar(20)");
        createTable("test_rowid2", "col1 int, col2 varchar(20)");
        createTable("test_rowid3", "col1 int, col2 varchar(20)");
    }

    @Test
    public void rowidTest() {
        try {
            sharedConnection.createStatement().execute(
                "insert into test_rowid(col1, col2) values(1, 'test1');");
            PreparedStatement preparedStatement = sharedConnection
                .prepareStatement("select rowid as id, col1 from test_rowid where col1 = ?");
            preparedStatement.setInt(1, 1);
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                int count = resultSet.getMetaData().getColumnCount();
                for (int i = 1; i <= count; i++) {
                    RowId rowId1 = (RowId) resultSet.getObject("id");
                    RowId rowId2 = resultSet.getRowId("id");
                    Assert.assertTrue(rowId1.equals(rowId2));
                    Assert.assertEquals(rowId1.toString(), rowId2.toString());
                    Assert.assertEquals(rowId1.toString(), resultSet.getString("id"));
                }
            }
        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void rowidTestForSelect() {
        try {
            Statement stmt = sharedConnection.createStatement();
            stmt.execute("insert into test_rowid2(col1, col2) values(1, 'test1');");
            stmt.execute("insert into test_rowid2(col1, col2) values(2, 'test2');");
            PreparedStatement preparedStatement = sharedConnection
                .prepareStatement("select rowid as id, col1 from test_rowid2 where col1 = ?");
            preparedStatement.setInt(1, 1);
            ResultSet resultSet = preparedStatement.executeQuery();
            Assert.assertTrue(resultSet.next());
            String rowidStringVal = resultSet.getString("id");
            RowId rowId1 = (RowId) resultSet.getObject("id");
            RowId rowId2 = resultSet.getRowId("id");
            Assert.assertTrue(rowId1.equals(rowId2));
            Assert.assertEquals(rowId1.toString(), rowId2.toString());
            Assert.assertEquals(rowId1.toString(), resultSet.getString("id"));

            preparedStatement = sharedConnection
                .prepareStatement("select * from test_rowid2 where rowid = ?");
            preparedStatement.setString(1, rowidStringVal);
            resultSet = preparedStatement.executeQuery();
            Assert.assertTrue(resultSet.next());
            Assert.assertEquals(1, resultSet.getInt(1));
            Assert.assertEquals("test1", resultSet.getString(2));

            preparedStatement = sharedConnection
                .prepareStatement("select * from test_rowid2 where rowid = ?");
            preparedStatement.setObject(1, rowId1);
            resultSet = preparedStatement.executeQuery();
            Assert.assertTrue(resultSet.next());
            Assert.assertEquals(1, resultSet.getInt(1));
            Assert.assertEquals("test1", resultSet.getString(2));

            preparedStatement = sharedConnection
                .prepareStatement("select * from test_rowid2 where rowid = ?");
            preparedStatement.setRowId(1, rowId2);
            resultSet = preparedStatement.executeQuery();
            Assert.assertTrue(resultSet.next());
            Assert.assertEquals(1, resultSet.getInt(1));
            Assert.assertEquals("test1", resultSet.getString(2));

        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void rowidTestForUpdate() {
        try {
            Statement stmt = sharedConnection.createStatement();
            stmt.execute("insert into test_rowid3(col1, col2) values(1, 'test1');");
            PreparedStatement preparedStatement = sharedConnection
                .prepareStatement("select rowid as id, col1 from test_rowid3 where col1 = ?");
            preparedStatement.setInt(1, 1);
            ResultSet resultSet = preparedStatement.executeQuery();
            Assert.assertTrue(resultSet.next());
            String rowidStringVal = resultSet.getString("id");
            RowId rowId1 = (RowId) resultSet.getObject("id");
            RowId rowId2 = resultSet.getRowId("id");
            Assert.assertTrue(rowId1.equals(rowId2));
            Assert.assertEquals(rowId1.toString(), rowId2.toString());
            Assert.assertEquals(rowId1.toString(), resultSet.getString("id"));

            preparedStatement = sharedConnection
                .prepareStatement("update test_rowid3 set col2 ='newvalue1', col1 = 1  where rowid = ?");
            preparedStatement.setString(1, rowidStringVal);
            preparedStatement.execute();

            preparedStatement = sharedConnection
                .prepareStatement("select rowid as id, col1 from test_rowid3 where col1 = ?");
            preparedStatement.setInt(1, 1);
            resultSet = preparedStatement.executeQuery();
            Assert.assertTrue(resultSet.next());
            rowidStringVal = resultSet.getString("id");
            rowId1 = (RowId) resultSet.getObject("id");
            rowId2 = resultSet.getRowId("id");
            Assert.assertTrue(rowId1.equals(rowId2));
            Assert.assertEquals(rowId1.toString(), rowId2.toString());
            Assert.assertEquals(rowId1.toString(), resultSet.getString("id"));

            preparedStatement = sharedConnection
                .prepareStatement("select * from test_rowid3 where rowid = ?");
            preparedStatement.setString(1, rowidStringVal);
            resultSet = preparedStatement.executeQuery();
            Assert.assertTrue(resultSet.next());
            Assert.assertEquals(1, resultSet.getInt(1));
            Assert.assertEquals("newvalue1", resultSet.getString(2));

            preparedStatement = sharedConnection
                .prepareStatement("update test_rowid3 set col2 ='newvalue2', col1 = 2  where rowid = ?");
            preparedStatement.setString(1, rowidStringVal);
            preparedStatement.execute();
            preparedStatement = sharedConnection
                .prepareStatement("select * from test_rowid3 where rowid = ?");
            preparedStatement.setObject(1, rowId1);
            resultSet = preparedStatement.executeQuery();
            Assert.assertTrue(resultSet.next());
            Assert.assertEquals(2, resultSet.getInt(1));
            Assert.assertEquals("newvalue2", resultSet.getString(2));

            preparedStatement = sharedConnection
                .prepareStatement("update test_rowid3 set col2 ='newvalue3', col1 = 3  where rowid = ?");
            preparedStatement.setString(1, rowidStringVal);
            preparedStatement.execute();

            preparedStatement = sharedConnection
                .prepareStatement("select * from test_rowid3 where rowid = ?");
            preparedStatement.setRowId(1, rowId2);
            resultSet = preparedStatement.executeQuery();
            Assert.assertTrue(resultSet.next());
            Assert.assertEquals(3, resultSet.getInt(1));
            Assert.assertEquals("newvalue3", resultSet.getString(2));

        } catch (Throwable e) {
            e.printStackTrace();
            Assert.fail();
        }
    }
}
