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
package com.oceanbase.jdbc.failover;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.InvalidObjectException;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;

import org.junit.*;

import com.oceanbase.jdbc.HostAddress;
import com.oceanbase.jdbc.ObPrepareStatement;
import com.oceanbase.jdbc.ObResultSet;
import com.oceanbase.jdbc.OceanBaseConnection;
import com.oceanbase.jdbc.extend.datatype.INTERVALDS;
import com.oceanbase.jdbc.internal.com.read.resultset.SelectResultSet;
import com.oceanbase.jdbc.internal.failover.BlackList.BlackListConfig;
import com.oceanbase.jdbc.internal.failover.BlackList.append.AppendStrategy;
import com.oceanbase.jdbc.internal.failover.BlackList.append.NormalAppend;
import com.oceanbase.jdbc.internal.failover.BlackList.append.RetryDuration;
import com.oceanbase.jdbc.internal.failover.BlackList.recover.RemoveStrategy;
import com.oceanbase.jdbc.internal.failover.BlackList.recover.TimeoutRecover;
import com.oceanbase.jdbc.internal.failover.LoadBalanceStrategy.GroupRotationStrategy;
import com.oceanbase.jdbc.internal.failover.LoadBalanceStrategy.RandomStrategy;
import com.oceanbase.jdbc.internal.failover.LoadBalanceStrategy.RotationStrategy;
import com.oceanbase.jdbc.internal.failover.LoadBalanceStrategy.ServerAffinityStrategy;
import com.oceanbase.jdbc.internal.failover.impl.LoadBalanceAddressList;
import com.oceanbase.jdbc.internal.failover.impl.LoadBalanceInfo;
import com.oceanbase.jdbc.internal.protocol.Protocol;
import com.oceanbase.jdbc.internal.util.JsonParser;
import com.oceanbase.jdbc.internal.util.constant.HaMode;

public class OracleLoadBalanceFailoverTest extends BaseMultiHostTest {

    /** Initialisation. */
    @BeforeClass()
    public static void beforeClass2() {
        proxyUrl = proxyLoadbalanceUrl;
        Assume.assumeTrue(initialLoadbalanceUrl != null);
    }

    /** Initialisation. */
    @Before
    public void init() {
        defaultUrl = initialLoadbalanceUrl;
        currentType = HaMode.LOADBALANCE;
    }

    @Test
    public void failover() throws Throwable {
        try (Connection connection = getNewConnection("&retriesAllDown=6", true)) {
            int master1ServerId = getServerId(connection);
            stopProxy(master1ServerId);
            connection.createStatement().executeQuery("SELECT 1 from dual");
            int secondServerId = getServerId(connection);
            Assert.assertNotEquals(master1ServerId, secondServerId);
        }
    }

    @Test
    public void randomConnection() throws Throwable {
        Assume.assumeTrue(initialLoadbalanceUrl.contains("loadbalance"));
        Map<String, MutableInt> connectionMap = new HashMap<>();
        for (int i = 0; i < 20; i++) {
            try (Connection connection = getNewConnection(false)) {
                int serverId = getServerId(connection);
                MutableInt count = connectionMap.get(String.valueOf(serverId));
                if (count == null) {
                    connectionMap.put(String.valueOf(serverId), new MutableInt());
                } else {
                    count.increment();
                }
            }
        }

        Assert.assertTrue(connectionMap.size() >= 2);
        for (String key : connectionMap.keySet()) {
            Integer connectionCount = connectionMap.get(key).get();
            Assert.assertTrue(connectionCount > 1);
        }
    }

    @Test
    public void testReadonly() {
        try (Connection connection = getNewConnection(false)) {
            connection.setReadOnly(true);

            Statement stmt = connection.createStatement();
            stmt.execute("drop table  if exists multinode");
            stmt.execute(
                    "create table multinode (id int not null primary key auto_increment, test VARCHAR(10))");
        } catch (SQLException sqle) {
            // normal exception
        }
    }

    class MutableInt {

        private int value = 1; // note that we start at 1 since we're counting

        public void increment() {
            ++value;
        }

        public int get() {
            return value;
        }
    }

    /**
     * A single connection instance executes the query, and the conn will not switch the host each time
     */
    public void testLoadBalanceSingleConn() throws Throwable {
        Connection conn = getNewConnection();
        String preUrl = null;
        String curUrl = null;
        int count = 0;
        for (int i = 0; i < 20; i++) {
            Protocol protocol = getProtocolFromConnection(conn);
            HostAddress hostAddress = protocol.getHostAddress();
            curUrl = hostAddress.host;
            System.out.println("curUrl = " + curUrl);
            if (i > 0 && preUrl.equals(curUrl) == false) {
                count++;
            }
            preUrl = curUrl;
            PreparedStatement cstmt1 = null;
            String sql = "select c1, c2 from test_intervalds";
            cstmt1 = conn.prepareStatement(sql);
            cstmt1.execute();
            ResultSet resultSet = cstmt1.getResultSet();
            try {
                while (resultSet.next()) {
                    Object obj1 = resultSet.getObject(1);
                    Object obj2 = ((SelectResultSet) resultSet).getINTERVALDS(2);
                }
            } finally {
                resultSet.close();
                cstmt1.close();
            }
        }
        System.out.println("count = " + count);
        assertEquals(0, count);
    }

    /**
     * A single connection instance executes the query and starts the transaction, and each conn does not switch the host
     */
    public void testLoadBalanceSingleConnWithTrans() throws Throwable {
        Connection conn = getNewConnection("&retriesAllDown=6", false);
        String preUrl = null;
        String curUrl;
        int count = 0;
        for (int i = 0; i < 20; i++) {
            conn.setAutoCommit(false);
            Protocol protocol = getProtocolFromConnection(conn);
            HostAddress hostAddress = protocol.getHostAddress();
            curUrl = hostAddress.host;
            System.out.println("curUrl = " + curUrl);
            if (i > 0 && preUrl.equals(curUrl) == false) {
                count++;
            }
            preUrl = curUrl;
            PreparedStatement cstmt1 = null;
            String sql = "select c1, c2 from test_intervalds";
            cstmt1 = conn.prepareStatement(sql);
            cstmt1.execute();
            SelectResultSet resultSet = (SelectResultSet) cstmt1.getResultSet();
            conn.commit();
            try {
                while (resultSet.next()) {
                    Object obj1 = resultSet.getObject(1);
                    Object obj2 = resultSet.getINTERVALDS(2);
                }
            } finally {
                resultSet.close();
                cstmt1.close();
            }
        }
        System.out.println("count = " + count);
        assertTrue(count == 0);
    }

    /**
     * Open transaction and execute prepareCall connection host will not switch
     * @throws Exception
     */
    public void testLoadBalanceSingleConnWithProcedure() {
        try {
            Connection conn = getNewConnection();
            String preUrl = null;
            String curUrl = null;
            int count = 0;
            try {
                conn.createStatement().execute("drop table test_t1");
            } catch (SQLException throwables) {
            }
            conn.createStatement().execute("create table test_t1 (c1 int)");
            conn.createStatement().execute(
                "create or replace procedure lbTest(a in int) is \n" + "begin \n"
                        + "insert  into test_t1 values(a);\n" + "end ");
            CallableStatement cStmt = conn.prepareCall("call lbTest(?)");

            conn.setAutoCommit(false);

            for (int i = 0; i < 20; i++) {
                Protocol protocol = getProtocolFromConnection(conn);
                HostAddress hostAddress = protocol.getHostAddress();
                curUrl = hostAddress.host;
                System.out.println("curUrl = " + curUrl);
                if (i > 0 && preUrl.equals(curUrl) == false) {
                    count++;
                }
                preUrl = curUrl;

                cStmt.setInt(1, 1);
                cStmt.execute();
                //            cStmt.close();
                conn.commit();
            }
            System.out.println("LBwithProcedure finish");
            System.out.println("count = " + count);
            assertTrue(count == 0);
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (Exception ee) {
            ee.printStackTrace();
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
    }

    /**
     * Multiple Connection instances execute query, and each conn switches randomly
     */
    public void testLoadBalanceMultiConn() throws Throwable {
        String preUrl = null;
        String curUrl = null;
        int count = 0;
        for (int i = 0; i < 20; i++) {
            OceanBaseConnection conn = (OceanBaseConnection) getNewConnection("&retriesAllDown=6",
                false);
            Protocol protocol = getProtocolFromConnection(conn);
            HostAddress hostAddress = protocol.getHostAddress();
            curUrl = hostAddress.host;
            System.out.println("curUrl = " + curUrl);
            if (i > 0 && preUrl.equals(curUrl) == false) {
                count++;
                System.out.println("preUrl = " + preUrl);
            }
            preUrl = curUrl;
            String sql;
            sql = "insert into test_intervalds values(?, ?)";
            PreparedStatement cstmt1 = null;
            sql = "select c1, c2 from test_intervalds";
            cstmt1 = conn.prepareStatement(sql);
            cstmt1.execute();
            ObResultSet resultSet = (ObResultSet) cstmt1.getResultSet();
            try {
                while (resultSet.next()) {
                    Object obj1 = resultSet.getObject(1);
                    Object obj2 = resultSet.getINTERVALDS(2);
                }
            } finally {
                resultSet.close();
                cstmt1.close();
                conn.close();
            }
        }
        System.out.println("count = " + count);
        assertTrue(count > 0);

    }

    @Test
    public void testLoadBalance() throws Throwable {
        Connection conn = getNewConnection();
        System.out.println("test intervalds only");
        try {
            conn.createStatement().execute("drop table test_intervalds");
        } catch (SQLException throwables) {
        }
        try {
            conn.createStatement().execute(
                "create table test_intervalds (c1 int primary key,"
                        + " c2 interval day(6) to second(5))");
        } catch (SQLException throwables) {
        }
        String sql;
        sql = "insert into test_intervalds values(?, ?)";
        PreparedStatement cstmt1 = conn.prepareStatement(sql);
        cstmt1.setInt(1, 1);
        ((ObPrepareStatement) cstmt1).setINTERVALDS(2, new INTERVALDS("2324 5:12:10.212"));
        cstmt1.execute();
        sql = "insert into test_intervalds values(2, '-122324 5:12:10.22222222')";
        cstmt1 = conn.prepareStatement(sql);
        cstmt1.execute();
        Statement stmt = conn.createStatement();

        testLoadBalanceSingleConn();
        testLoadBalanceMultiConn();
        testLoadBalanceSingleConnWithTrans();
        testLoadBalanceSingleConnWithProcedure();
        stmt.execute("drop table test_intervalds");
        conn.close();
    }

    @Test
    public void testParseBalanceInfoJson1() throws InvalidObjectException {
        String balanceJson = "{\n"
                            + "    \"OBLB\" : \"ON\" ,\n"
                            + "    \"OBLB_RETRY_ALL_DOWNS\" : 110 ,\n"
                            + "    \"OBLB_GROUP_STRATEGY\" : \"ROTATION\" ,\n"
                            + "    \"OBLB_STRATEGY\" : \"ROTATION\" ,\n"
                            + "    \"OBLB_BLACKLIST\" : {\n"
                            + "        \"REMOVE_STRATEGY\" : {\n"
                            + "            \"NAME\" : \"TIMEOUT\" , \n"
                            + "            \"TIMEOUT\" : 100\n"
                            + "        } ,\n"
                            + "        \"APPEND_STRATEGY\" : {\n"
                            + "            \"NAME\" : \"RETRYDURATION\" , \n"
                            + "            \"RETRYTIMES\" : 3 , \n"
                            + "            \"DURATION\" : 100\n"
                            + "        }\n"
                            + "    } ,\n"
                            + "    \"ADDRESS_LIST\" : [\n"
                            + "        {   \n"
                            + "            \"OBLB_BLACKLIST\" : {\n"
                            + "                \"REMOVE_STRATEGY\" : {\n"
                            + "                    \"NAME\" : \"TIMEOUT\" , \n"
                            + "                    \"TIMEOUT\" : 88\n"
                            + "                } ,\n"
                            + "                \"APPEND_STRATEGY\" : {\n"
                            + "                    \"NAME\" : \"RETRYDURATION\" , \n"
                            + "                    \"RETRYTIMES\" : 5 , \n"
                            + "                    \"DURATION\" : 200\n"
                            + "                }\n"
                            + "            } ,\n"
                            + "            \"ADDRESS\" : [\n"
                            + "                {\n"
                            + "                    \"PROTOCOL\" : \"tcp\" , \n"
                            + "                    \"HOST\" : \"host1\" , \n"
                            + "                    \"PORT\" : 1111\n"
                            + "                } ,\n"
                            + "                {\n"
                            + "                    \"PROTOCOL\" : \"tcp\" , \n"
                            + "                    \"HOST\" : \"host2\" , \n"
                            + "                    \"PORT\" : 2222\n"
                            + "                }\n"
                            + "            ]\n"
                            + "        } , \n"
                            + "        {\n"
                            + "            \"OBLB_STRATEGY\" : \"RANDOM\" ,\n"
                            + "            \"ADDRESS\" : [\n"
                            + "                {\n"
                            + "                    \"PROTOCOL\" : \"tcp\" , \n"
                            + "                    \"HOST\" : \"host3\" , \n"
                            + "                    \"PORT\" : 3333\n"
                            + "                } ,\n"
                            + "                {\n"
                            + "                    \"PROTOCOL\" : \"tcp\" , \n"
                            + "                    \"HOST\" : \"host4\" , \n"
                            + "                    \"PORT\" : 4444\n"
                            + "                }\n"
                            + "            ]\n"
                            + "        } , \n"
                            + "        {\n"
                            + "            \"OBLB_STRATEGY\" : \"SERVERAFFINITY\" ,\n"
                            + "            \"ADDRESS\" : [\n"
                            + "                {\n"
                            + "                    \"PROTOCOL\" : \"tcp\" , \n"
                            + "                    \"HOST\" : \"host5\" , \n"
                            + "                    \"PORT\" : 5555,\n"
                            + "                    \"WEIGHT\" : 1\n"
                            + "                } ,\n"
                            + "                {\n"
                            + "                    \"PROTOCOL\" : \"tcp\" , \n"
                            + "                    \"HOST\" : \"host6\" , \n"
                            + "                    \"PORT\" : 6666,\n"
                            + "                    \"WEIGHT\" : 10\n"
                            + "                } ,\n"
                            + "                {\n"
                            + "                    \"PROTOCOL\" : \"tcp\" , \n"
                            + "                    \"HOST\" : \"host7\" , \n"
                            + "                    \"PORT\" : 7777,\n"
                            + "                    \"WEIGHT\" : 0\n"
                            + "                }\n"
                            + "            ]\n"
                            + "        }\n"
                            + "    ] ,\n"
                            + "    \"CONNECT_DATA\" : {\n"
                            + "        \"SERVICE_NAME\" : \"OBDATABASE\"\n"
                            + "    }\n"
                            + "}";

        JsonParser parsing = new JsonParser(balanceJson);
        parsing.parse();
        HashMap mapDescription = (HashMap) parsing.getStack().peek();
        LoadBalanceInfo balanceInfo = new LoadBalanceInfo(mapDescription);

        Assert.assertEquals("OBDATABASE", balanceInfo.getServiceName());
        Assert.assertEquals(110, balanceInfo.getRetryAllDowns());

        Assert.assertTrue(balanceInfo.getGroupBalanceStrategy() instanceof GroupRotationStrategy);
        Assert.assertNull(balanceInfo.getGroupBalanceStrategyConfigs());
        Assert.assertTrue(balanceInfo.getBalanceStrategy() instanceof RotationStrategy);
        Assert.assertNull(balanceInfo.getBalanceStrategyConfigs());

        BlackListConfig blackListConfig = balanceInfo.getBlackListConfig();
        Assert.assertNotNull(blackListConfig);
        Assert.assertNull(blackListConfig.getRemoveStrategyConfigs());
        Assert.assertNull(blackListConfig.getAppendStrategyConfigs());

        RemoveStrategy removeStrategy = blackListConfig.getRemoveStrategy();
        Assert.assertTrue(removeStrategy instanceof TimeoutRecover);
        Assert.assertEquals(100, ((TimeoutRecover) removeStrategy).getTimeout());

        AppendStrategy appendStrategy = blackListConfig.getAppendStrategy();
        Assert.assertTrue(appendStrategy instanceof RetryDuration);
        Assert.assertEquals(100, ((RetryDuration) appendStrategy).getDuration());
        Assert.assertEquals(3, ((RetryDuration) appendStrategy).getRetryTimes());

        Assert.assertEquals(3, balanceInfo.getGroups().size());

        LoadBalanceAddressList addressList1 = balanceInfo.getGroups().get(0);
        Assert.assertTrue(addressList1.getBalanceStrategy() instanceof RotationStrategy);
        Assert.assertNull(addressList1.getBlackListConfig());
        Assert.assertNull(addressList1.getBalanceStrategyConfigs());
        Assert.assertEquals(2, addressList1.getAddressList().size());

        LoadBalanceAddressList addressList2 = balanceInfo.getGroups().get(1);
        Assert.assertTrue(addressList2.getBalanceStrategy() instanceof RandomStrategy);
        Assert.assertNull(addressList2.getBlackListConfig());
        Assert.assertNull(addressList2.getBalanceStrategyConfigs());
        Assert.assertEquals(2, addressList2.getAddressList().size());

        LoadBalanceAddressList addressList3 = balanceInfo.getGroups().get(2);
        Assert.assertTrue(addressList3.getBalanceStrategy() instanceof ServerAffinityStrategy);
        Assert.assertNull(addressList3.getBlackListConfig());
        Assert.assertNull(addressList3.getBalanceStrategyConfigs());
        Assert.assertEquals(3, addressList3.getAddressList().size());
    }

    @Test
    public void testParseBalanceInfoJson2() throws InvalidObjectException {
        String balanceJson = "{\n"
                            + "    \"OBLB\" : \"ON\" ,\n"
                            + "    \"ADDRESS_LIST\" : [\n"
                            + "        {   \n"
                            + "            \"OBLB_STRATEGY\" : \"ROTATION\" ,\n"
                            + "            \"ADDRESS\" : [\n"
                            + "                {\n"
                            + "                    \"PROTOCOL\" : \"tcp\" , \n"
                            + "                    \"HOST\" : \"host1\" , \n"
                            + "                    \"PORT\" : 1111\n"
                            + "                } ,\n"
                            + "                {\n"
                            + "                    \"PROTOCOL\" : \"tcp\" , \n"
                            + "                    \"HOST\" : \"host2\" , \n"
                            + "                    \"PORT\" : 2222\n"
                            + "                }\n"
                            + "            ]\n"
                            + "        } , \n"
                            + "        {\n"
                            + "            \"ADDRESS\" : [\n"
                            + "                {\n"
                            + "                    \"PROTOCOL\" : \"tcp\" , \n"
                            + "                    \"HOST\" : \"host3\" , \n"
                            + "                    \"PORT\" : 3333\n"
                            + "                } ,\n"
                            + "                {\n"
                            + "                    \"PROTOCOL\" : \"tcp\" , \n"
                            + "                    \"HOST\" : \"host4\" , \n"
                            + "                    \"PORT\" : 4444\n"
                            + "                }\n"
                            + "            ]\n"
                            + "        } , \n"
                            + "        {\n"
                            + "            \"OBLB_STRATEGY\" : \"SERVERAFFINITY\" ,\n"
                            + "            \"ADDRESS\" : [\n"
                            + "                {\n"
                            + "                    \"PROTOCOL\" : \"tcp\" , \n"
                            + "                    \"HOST\" : \"host5\" , \n"
                            + "                    \"PORT\" : 5555,\n"
                            + "                    \"WEIGHT\" : 1\n"
                            + "                } ,\n"
                            + "                {\n"
                            + "                    \"PROTOCOL\" : \"tcp\" , \n"
                            + "                    \"HOST\" : \"host6\" , \n"
                            + "                    \"PORT\" : 6666,\n"
                            + "                    \"WEIGHT\" : 10\n"
                            + "                } ,\n"
                            + "                {\n"
                            + "                    \"PROTOCOL\" : \"tcp\" , \n"
                            + "                    \"HOST\" : \"host7\" , \n"
                            + "                    \"PORT\" : 7777,\n"
                            + "                    \"WEIGHT\" : 0\n"
                            + "                }\n"
                            + "            ]\n"
                            + "        }\n"
                            + "    ] ,\n"
                            + "    \"CONNECT_DATA\" : {\n"
                            + "        \"SERVICE_NAME\" : \"OBDATABASE\"\n"
                            + "    }\n"
                            + "}";

        JsonParser parsing = new JsonParser(balanceJson);
        parsing.parse();
        HashMap mapDescription = (HashMap) parsing.getStack().peek();
        LoadBalanceInfo balanceInfo = new LoadBalanceInfo(mapDescription);

        Assert.assertEquals("OBDATABASE", balanceInfo.getServiceName());
        Assert.assertEquals(120, balanceInfo.getRetryAllDowns());

        Assert.assertTrue(balanceInfo.getGroupBalanceStrategy() instanceof GroupRotationStrategy);
        Assert.assertNull(balanceInfo.getGroupBalanceStrategyConfigs());
        Assert.assertTrue(balanceInfo.getBalanceStrategy() instanceof RandomStrategy);
        Assert.assertNull(balanceInfo.getBalanceStrategyConfigs());

        BlackListConfig blackListConfig = balanceInfo.getBlackListConfig();
        Assert.assertNotNull(blackListConfig);
        Assert.assertNull(blackListConfig.getRemoveStrategyConfigs());
        Assert.assertNull(blackListConfig.getAppendStrategyConfigs());

        RemoveStrategy removeStrategy = blackListConfig.getRemoveStrategy();
        Assert.assertTrue(removeStrategy instanceof TimeoutRecover);
        Assert.assertEquals(50, ((TimeoutRecover) removeStrategy).getTimeout());

        AppendStrategy appendStrategy = blackListConfig.getAppendStrategy();
        Assert.assertTrue(appendStrategy instanceof NormalAppend);

        Assert.assertEquals(3, balanceInfo.getGroups().size());

        LoadBalanceAddressList addressList1 = balanceInfo.getGroups().get(0);
        Assert.assertTrue(addressList1.getBalanceStrategy() instanceof RotationStrategy);
        Assert.assertNull(addressList1.getBlackListConfig());
        Assert.assertNull(addressList1.getBalanceStrategyConfigs());
        Assert.assertEquals(2, addressList1.getAddressList().size());

        LoadBalanceAddressList addressList2 = balanceInfo.getGroups().get(1);
        Assert.assertTrue(addressList2.getBalanceStrategy() instanceof RandomStrategy);
        Assert.assertNull(addressList2.getBlackListConfig());
        Assert.assertNull(addressList2.getBalanceStrategyConfigs());
        Assert.assertEquals(2, addressList2.getAddressList().size());

        LoadBalanceAddressList addressList3 = balanceInfo.getGroups().get(2);
        Assert.assertTrue(addressList3.getBalanceStrategy() instanceof ServerAffinityStrategy);
        Assert.assertNull(addressList3.getBlackListConfig());
        Assert.assertNull(addressList3.getBalanceStrategyConfigs());
        Assert.assertEquals(3, addressList3.getAddressList().size());
    }

    @Test
    public void testEmptyAddressList() throws InvalidObjectException {
        String balanceJson = "{\n" +
                "            \"ADDRESS_LIST\":[\n" +
                "\n" +
                "            ],\n" +
                "            \"CONNECT_DATA\":{\n" +
                "                \"SERVICE_NAME\":\"OBDATABASE\"\n" +
                "            },\n" +
                "            \"OBLB\":\"ON\",\n" +
                "            \"OBLB_BLACKLIST\":{\n" +
                "                \"APPEND_STRATEGY\":{\n" +
                "                    \"DURATION\":100,\n" +
                "                    \"NAME\":\"RETRYDURATION\",\n" +
                "                    \"RETRYTIMES\":3\n" +
                "                },\n" +
                "                \"REMOVE_STRATEGY\":{\n" +
                "                    \"NAME\":\"TIMEOUT\",\n" +
                "                    \"TIMEOUT\":50\n" +
                "                }\n" +
                "            },\n" +
                "            \"OBLB_GROUP_STRATEGY\":\"ROTATION\",\n" +
                "            \"OBLB_RETRY_ALL_DOWNS\":120\n" +
                "        }";

        JsonParser parsing = new JsonParser(balanceJson);
        parsing.parse();
        HashMap mapDescription = (HashMap) parsing.getStack().peek();
        try {
            LoadBalanceInfo balanceInfo = new LoadBalanceInfo(mapDescription);
        } catch (InvalidObjectException e) {
            Assert.assertEquals("ADDRESS_LIST can't be empty!", e.getMessage());
        }
    }
}
