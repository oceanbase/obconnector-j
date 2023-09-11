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

import org.junit.Test;
import sun.security.action.GetPropertyAction;

import java.security.AccessController;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;

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

}
