/*
 *
 * OceanBase Client for Java
 *
 * Copyright (c) 2023 OceanBase.
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

import org.junit.Assert;
import org.junit.Test;
import sun.security.action.GetPropertyAction;

import java.security.AccessController;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.*;
import static org.junit.Assert.fail;

public class CharsetTest extends BaseTest {

    @Test
    public void testGB18080_2022() {
        // use gb18030_2022
        try {
            assertFalse("2000".equals(AccessController.doPrivileged(new GetPropertyAction(
                "jdk.charset.GB18030"))));
            Connection conn = setConnection("&characterEncoding=gb18030_2022");

            String a = "龴";
            byte[] b = a.getBytes("GB18030");
            StringBuilder res = new StringBuilder();
            for (byte i : b) {
                res.append(String.format("%02x", i));
            }
            assertEquals("fe59", res.toString());

            PreparedStatement pstmt = conn.prepareStatement("select ? from dual");
            pstmt.setString(1, a);
            ResultSet rs = pstmt.executeQuery();
            assertTrue(rs.next());
            assertEquals("龴", rs.getString(1));
            Arrays.equals(b, rs.getBytes(1));
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }

        // compare with gb18030, need to add VM option: -Djdk.charset.GB18030=2000
        try {
            System.setProperty("jdk.charset.GB18030", "2000");
            assertTrue("2000".equals(AccessController.doPrivileged(new GetPropertyAction(
                "jdk.charset.GB18030"))));
            Connection conn = setConnection("&characterEncoding=gb18030");

            String a = "龴";
            byte[] b = a.getBytes("GB18030");
            StringBuilder res = new StringBuilder();
            for (byte i : b) {
                res.append(String.format("%02x", i));
            }
            assertEquals("82359037", res.toString());

            PreparedStatement pstmt = conn.prepareStatement("select ? from dual");
            pstmt.setString(1, a);
            ResultSet rs = pstmt.executeQuery();
            assertTrue(rs.next());
            assertEquals("龴", rs.getString(1));
            Arrays.equals(b, rs.getBytes(1));
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testForDima2025063000109725751() throws ExecutionException, InterruptedException, SQLException {
        // init table
        String[] columns = new String[40];
        String[] values = new String[40];
        String[] marks = new String[40];
        for (int i = 0; i < 40; i++) {
            String col = "c" + i + " varchar(10)";
            columns[i] = col;
            values[i] = "aaabbb" + i;
            marks[i] = "?";
        }
        String sql = "create table z_t1 (" + String.join(",", columns) + ") default charset utf8mb4 collate utf8mb4_unicode_ci";

        Connection connection = setConnection("&rewriteBatchedStatements=true&allowMultiQueries=true&useServerPrepStmts=true");
        Statement statement = connection.createStatement();
        statement.execute("drop table if exists z_t1");
        statement.execute(sql);

        String insertSql = "insert into z_t1 () values (" + String.join(",", marks) + ")";
        PreparedStatement preparedStatement = connection.prepareStatement(insertSql);
        for (int i = 0; i < 1000; i++) {
            for (int j = 0; j < 40; j++) {
                preparedStatement.setString(j + 1, values[j]);
            }
            preparedStatement.addBatch();
        }
        preparedStatement.executeBatch();

        ExecutorService threadPool = Executors.newFixedThreadPool(50);
        List<Future<Long>> futureTasks = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            Future<Long> task = threadPool.submit(() -> {
                try {
                    String charsets[] = new String[]{"utf8", "gbk", "latin1"};
                    int index = new Random().nextInt(charsets.length);
                    String charset = charsets[index];
                    Connection conn = setConnection("&characterEncoding=" + charset);
                    Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY );
                    ResultSet rs = stmt.executeQuery("select * from z_t1");
                    long start = System.currentTimeMillis();
                    int count = 0;
                    while (rs.next()) {
                        for (int j = 1; j <= 40; j++) {
                            rs.getString(j);
                        }
                        count++;
                        if (count % 1000 == 0 && count / 1000 < 500) {
                            rs.beforeFirst();
                        }
                    }
                    long end = System.currentTimeMillis();
                    long costTime = end - start;
                    stmt.close();
                    conn.close();
                    return costTime;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            futureTasks.add(task);
        }

        long totalCost = 0;
        for (Future<Long> futureTask : futureTasks) {
            totalCost += futureTask.get();
        }
        Assert.assertTrue(totalCost < 400000);
    }

}
