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

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.Assume;
import org.junit.Test;

import com.oceanbase.jdbc.internal.protocol.Protocol;

public class AttributeTest extends BaseTest {
    /*
    No supported performance_schema now
     */
    @Test
  public void testServerHost() throws Exception {
    // test for _server_host attribute
    // session_connect_attrs does not exist in MySQL 5.5, or before MariaDB 10.0.5, and need
    // performance_schema is ON

    // check whether session_connect_attrs table exists
    Statement checkStatement = sharedConnection.createStatement();
    ResultSet checkResult =
        checkStatement.executeQuery(
            "select count(*) as count from information_schema.tables where table_schema='performance_schema' and table_name='session_connect_attrs';");
    checkResult.next();
    Assume.assumeFalse(checkResult.getInt("count") == 0);

    // check if performance_schema is ON
    checkResult = checkStatement.executeQuery("show variables like 'performance_schema';");
    checkResult.next();
    Assume.assumeFalse(checkResult.getString("Value").equals("OFF"));

    try (Connection connection = setConnection("")) {
      Field protocolField = OceanBaseConnection.class.getDeclaredField("protocol");
      protocolField.setAccessible(true);
      Protocol protocolVal = (Protocol) protocolField.get(connection);

      Statement attributeStatement = connection.createStatement();
      ResultSet result =
          attributeStatement.executeQuery(
              "select * from performance_schema.session_connect_attrs where ATTR_NAME='_server_host' and processlist_id = connection_id()");
      while (result.next()) {
        String str = result.getString("ATTR_NAME");
        String strVal = result.getString("ATTR_VALUE");
        Assume.assumeTrue(protocolVal.getHost().matches(strVal));
      }
    }
  }
}
