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

import java.sql.*;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Ignore;
import org.junit.Test;

public class ClientPreparedStatementOracleTest extends BaseOracleTest {

    @Ignore
    @Test
    public void closedStatement() throws SQLException {

        PreparedStatement preparedStatement = sharedConnection
            .prepareStatement("SELECT ? FROM DUAL");
        preparedStatement.setString(1, "1");
        preparedStatement.execute();

        preparedStatement.setString(1, "1");
        preparedStatement.executeQuery();

        preparedStatement.setString(1, "1");
        preparedStatement.executeUpdate();

        preparedStatement.close();

        try {
            preparedStatement.setString(1, "1");
            preparedStatement.execute();
            fail();
        } catch (SQLException e) {
            // TODO need to optimize next.
            // it will be throw "java.lang.NullPointerException" for this.connection.getProtocol().isOracleMode()
            assertTrue(e.getMessage().contains("is called on closed statement"));
        }

        try {
            preparedStatement.setString(1, "1");
            preparedStatement.executeQuery();
            fail();
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("is called on closed statement"));
        }

        try {
            preparedStatement.setString(1, "1");
            preparedStatement.executeUpdate();
            fail();
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("is called on closed statement"));
        }
    }

    @Test
    public void timeoutStatement() throws SQLException {

        PreparedStatement preparedStatement = sharedConnection
            .prepareStatement("SELECT ? FROM DUAL");
        preparedStatement.setQueryTimeout(10);
        preparedStatement.setString(1, "1");
        preparedStatement.execute();

        preparedStatement.setString(1, "1");
        preparedStatement.executeQuery();

        preparedStatement.setString(1, "1");
        preparedStatement.executeUpdate();
    }

    @Test
    public void paramNumber() throws SQLException {
        try {
            sharedConnection.prepareStatement("SELECT ?, ?");
        } catch (SQLException e) {
            Assert.assertTrue(e.getMessage().contains(
                "ORA-00900: You have an error in your SQL syntax"));
        }
        if (!sharedOptions().emulateUnsupportedPstmts) {
            Connection newConn = setConnection("&emulateUnsupportedPstmts=true");
            PreparedStatement preparedStatement = newConn.prepareStatement("SELECT ?, ?");
            try {
                preparedStatement.setString(1, "1");
                preparedStatement.execute();
                fail();
            } catch (SQLException e) {
                assertTrue(e.getMessage().contains("Parameter at position 2 is not set"));
            }
            try {
                preparedStatement.setString(1, "1");
                preparedStatement.executeQuery();
                fail();
            } catch (SQLException e) {
                assertTrue(e.getMessage().contains("Parameter at position 2 is not set"));
            }
            try {
                preparedStatement.setString(1, "1");
                preparedStatement.executeUpdate();
                fail();
            } catch (SQLException e) {
                assertTrue(e.getMessage().contains("Parameter at position 2 is not set"));
            }
            try {
                sharedConnection.prepareStatement("SELECT ?, ?");
            } catch (SQLSyntaxErrorException e) {
                e.printStackTrace();
            }
            newConn.close();
        }
    }

    @Test
    public void correctWithShap() throws SQLException {
        Assume.assumeFalse(sharedUsePrepare());
        String sql = "drop table test_execute_client_ps#";

        PreparedStatement preparedStatement = sharedConnection.prepareStatement(sql);
        try {
            preparedStatement.executeUpdate();
        } catch (Exception e) {
            //skip
        }
        preparedStatement
            .execute("create table test_execute_client_ps#(c int, c1 varchar(10), c2 int, c3 int, c# varchar2(10))");

        preparedStatement = sharedConnection
            .prepareStatement("insert into test_execute_client_ps# values (?, ?, ?, ?, ?)");
        preparedStatement.setInt(1, 101);
        preparedStatement.setString(2, "101");
        preparedStatement.setInt(3, 101);
        preparedStatement.setInt(4, 101);
        preparedStatement.setString(5, "102");
        preparedStatement.executeUpdate();

        ResultSet rs = preparedStatement.executeQuery("select * from test_execute_client_ps#");

        while (rs.next()) {
            assertTrue(101 == rs.getInt(1));
            assertTrue("101".equals(rs.getString(2)));
        }
    }

    @Test
    public void testVarcharWithApostrophe() throws SQLException {
        Assume.assumeFalse(sharedUsePrepare());
        String sql = "drop table test_varchar_with_symbols";

        PreparedStatement preparedStatement = sharedConnection.prepareStatement(sql);
        try {
            preparedStatement.executeUpdate();
        } catch (Exception e) {
            //skip
        }
        preparedStatement
            .execute("CREATE TABLE test_varchar_with_symbols (col1 NUMBER(5,2), col2 varchar (200), col3 varchar(100))");
        sql = "INSERT INTO test_varchar_with_symbols VALUES (15, 'PJ>PK:ROf\\', 'PJ>PK?ROf')"; //str is: `INSERT INTO test_nvarchar2 VALUES (15, 'PJ>PK:ROf\', 'PJ>PK?ROf')`
        preparedStatement = sharedConnection.prepareStatement(sql);
        preparedStatement.executeUpdate();
        sql = "INSERT INTO test_varchar_with_symbols VALUES (12, 'PJ>PK\":ROf\\', 'PJ>PK?ROf')"; //str is: `INSERT INTO test_nvarchar2 VALUES (12, 'PJ>PK":ROf\', 'PJ>PK?ROf')`
        preparedStatement = sharedConnection.prepareStatement(sql);
        preparedStatement.executeUpdate();
        sql = "select col1, col2, col3 from test_varchar_with_symbols";
        PreparedStatement cstmt1 = sharedConnection.prepareStatement(sql);
        cstmt1.execute();
        ResultSet resultSet = cstmt1.getResultSet();
        try {
            while (resultSet.next()) {
                Object obj1 = resultSet.getObject(1);
                Object obj2 = resultSet.getObject(2);
                Object obj3 = resultSet.getObject(3);
                System.out.println("OBJECT:" + obj1 + " " + obj2 + " " + obj3);
                //                if (Integer.parseInt((String) obj1) == 15) {
                //                    //Assert.assertEquals("PJ>PK:ROf\\", obj2);
                //                    //Assert.assertEquals("PJ>PK?ROf", obj3);
                //                }
            }
        } catch (Exception e) {
            fail();
            e.printStackTrace();
        }
    }

    @Test
    public void testVarcharWithApostrophe2() throws SQLException {

        String sql = "drop table test_symbols";

        Statement statement = sharedConnection.createStatement();
        //PreparedStatement preparedStatement = sharedConnection.prepareStatement(sql);
        try {
            statement.executeUpdate(sql);
        } catch (Exception e) {
            //skip
        }
        statement.execute("CREATE TABLE test_symbols (cc varchar(100))");
        sql = "INSERT INTO test_symbols VALUES ('^+c$k\"(!YX'),('db=(qB,(Q '),('/~~`zLD3/('),('FnAEFwPl{L'),('t[Nb-&%?7 '),('\"~[qD`|Iz<'),('CL(5w=(L7}'),('x?>}V\\Dz''T'),('O\\rIC/e7Pl'),('h8=v\\odkI8'),('v0z=ukdfe]'),('ZLfJ:BxPxw'),('u22tr)|FsX'),('2n1vX)k_4 '),('*3EhYn4CV-'),('xu6igHpNR '),('G;Su2+F\"i^'),('\"[]Wv.t>mu'),('''m L9\"]^n!'),('&[CB]-Li%t'),('>`%mCA%|L '),('}S}E7E0D+&'),('E?|=/3qW6G'),('q=6&@+STA-'),('c;-`Q2;Px='),('civzF)F@MQ'),('r`%78CC]&a'),('+}QFS/W-zG'),('`n2hH$w?\\ '),(']z*j/?~2m-'),('Lh;1ml8-I)'),('/6`1^7xZh '),('q7A(rJ z\"M'),('Y7 l@n2b}M'),('=,&8~OIh+`'),('J>3I9oQ9k9'),(',.G9?&z$;x'),('jB%z5d&|;y'),('pN6=ZD~I<_'),('Y4X_S[RC:='),('3v>$oD[*c('),('|&]4L''e=Vc'),('HP{\"fm7T90'),('gCEYfy yTJ'),('<9L6:aD_H '),('H{h^}fy1A~'),('rX,aXjw3bQ'),('3f\"8oK(@<{'),('PJ>PK:ROf\\'),(':;]yQ3ID}L'),('|8m`L_d4cf'),('/f;(:\"8Jww'),('~)cZ>lCD<2'),(']+51KX!1M9'),('E;iL'']DX;K'),('`UN0-EmC^x'),('9[=HdY{B_ '),('TTZ+i\\9{hf'),('Co61c6wyl1'),(']]PA/LkDM*'),('07!]uvKh61'),('hk!.):1+! '),('Lki>ALUg{1'),('^e*KV&E;{S'),('zqLmp(.cPb'),('/p53m;B=3q'),('<tm)Y>=H(U'),('wIt}{=>N@o'),('Fo}TVMa^}!'),('3ljFV0X}=6'),('''U2q_Re W?'),('b=8|C,N4T,'),('9b|N4;6MxT'),('$UzgL@\\6ja'),('A*BT5([\\Dd'),(';\"XY9?B8eP'),('2}K;qbIyC]'),('ZcLYt+I2!@'),('NmmnF ,+\"H'),('*g.l5`vRbi'),('s!%Jg2Y1@<'),('[/`etVG_me'),('-^l65z,C1O'),('0[]\"RvN;}I'),('?m,[~PGc\"D'),('J,(J;PnB`&'),('l/q0:Yz!:-'),('To\"wW!o|W '),('?8-cUaV;CY'),('GTxLb>T-I6'),('h]|DZzb&Ud'),('26/hh1}^al'),('qqW[I5Y.Sk'),('}Hm@PSEvJl'),('+1@.]4@y/~'),('zL4m[Qn[Le'),('xCpY[Pu&Ak'),('8S=vE?JM?@'),('EnD}4D\\f`5'),('\"yX~m@hi)J');";
        statement.executeUpdate(sql);
        sql = "select * from test_symbols";
        PreparedStatement cstmt1 = sharedConnection.prepareStatement(sql);
        cstmt1.execute();
        ResultSet resultSet = cstmt1.getResultSet();
        try {
            while (resultSet.next()) {
                Object obj1 = resultSet.getObject(1);
                System.out.println("OBJECT:  " + obj1);
            }
        } catch (Exception e) {
            fail();
            e.printStackTrace();
        }
    }

    @Test
    public void testVarcharWithApostrophe3() throws SQLException {

        String sql = "drop table test_symbols";

        PreparedStatement preparedStatement = sharedConnection.prepareStatement(sql);
        try {
            preparedStatement.executeUpdate();
        } catch (Exception e) {
            //skip
        }
        preparedStatement.execute("CREATE TABLE test_symbols (cc varchar(100))");
        sql = "INSERT INTO test_symbols VALUES ('^+c$k\"(!YX'),('db=(qB,(Q '),('/~~`zLD3/('),('FnAEFwPl{L'),('t[Nb-&%?7 '),('\"~[qD`|Iz<'),('CL(5w=(L7}'),('x?>}V\\Dz''T'),('O\\rIC/e7Pl'),('h8=v\\odkI8'),('v0z=ukdfe]'),('ZLfJ:BxPxw'),('u22tr)|FsX'),('2n1vX)k_4 '),('*3EhYn4CV-'),('xu6igHpNR '),('G;Su2+F\"i^'),('\"[]Wv.t>mu'),('''m L9\"]^n!'),('&[CB]-Li%t'),('>`%mCA%|L '),('}S}E7E0D+&'),('E?|=/3qW6G'),('q=6&@+STA-'),('c;-`Q2;Px='),('civzF)F@MQ'),('r`%78CC]&a'),('+}QFS/W-zG'),('`n2hH$w?\\ '),(']z*j/?~2m-'),('Lh;1ml8-I)'),('/6`1^7xZh '),('q7A(rJ z\"M'),('Y7 l@n2b}M'),('=,&8~OIh+`'),('J>3I9oQ9k9'),(',.G9?&z$;x'),('jB%z5d&|;y'),('pN6=ZD~I<_'),('Y4X_S[RC:='),('3v>$oD[*c('),('|&]4L''e=Vc'),('HP{\"fm7T90'),('gCEYfy yTJ'),('<9L6:aD_H '),('H{h^}fy1A~'),('rX,aXjw3bQ'),('3f\"8oK(@<{'),('PJ>PK:ROf\\'),(':;]yQ3ID}L'),('|8m`L_d4cf'),('/f;(:\"8Jww'),('~)cZ>lCD<2'),(']+51KX!1M9'),('E;iL'']DX;K'),('`UN0-EmC^x'),('9[=HdY{B_ '),('TTZ+i\\9{hf'),('Co61c6wyl1'),(']]PA/LkDM*'),('07!]uvKh61'),('hk!.):1+! '),('Lki>ALUg{1'),('^e*KV&E;{S'),('zqLmp(.cPb'),('/p53m;B=3q'),('<tm)Y>=H(U'),('wIt}{=>N@o'),('Fo}TVMa^}!'),('3ljFV0X}=6'),('''U2q_Re W?'),('b=8|C,N4T,'),('9b|N4;6MxT'),('$UzgL@\\6ja'),('A*BT5([\\Dd'),(';\"XY9?B8eP'),('2}K;qbIyC]'),('ZcLYt+I2!@'),('NmmnF ,+\"H'),('*g.l5`vRbi'),('s!%Jg2Y1@<'),('[/`etVG_me'),('-^l65z,C1O'),('0[]\"RvN;}I'),('?m,[~PGc\"D'),('J,(J;PnB`&'),('l/q0:Yz!:-'),('To\"wW!o|W '),('?8-cUaV;CY'),('GTxLb>T-I6'),('h]|DZzb&Ud'),('26/hh1}^al'),('qqW[I5Y.Sk'),('}Hm@PSEvJl'),('+1@.]4@y/~'),('zL4m[Qn[Le'),('xCpY[Pu&Ak'),('8S=vE?JM?@'),('EnD}4D\\f`5'),('\"yX~m@hi)J');";
        System.out.println("SQL:" + sql);
        preparedStatement = sharedConnection.prepareStatement(sql);
        preparedStatement.executeUpdate();
        sql = "select * from test_symbols";
        PreparedStatement cstmt1 = sharedConnection.prepareStatement(sql);
        cstmt1.execute();
        ResultSet resultSet = cstmt1.getResultSet();
        try {
            while (resultSet.next()) {
                Object obj1 = resultSet.getObject(1);
                System.out.println("OBJECT:  " + obj1);
            }
        } catch (Exception e) {
            //fail();
            e.printStackTrace();
        }
    }

    @Test
    public void setParameterError() throws SQLException {
        Assume.assumeFalse(sharedOptions().useServerPrepStmts);
        PreparedStatement preparedStatement = sharedConnection
            .prepareStatement("SELECT ?, ? FROM DUAL");
        preparedStatement.setString(1, "a");
        preparedStatement.setString(2, "a");

        try {
            preparedStatement.setString(3, "a");
            fail();
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains(
                "Could not set parameter at position 3 (values was 'a')"));
        }

        try {
            preparedStatement.setString(0, "a");
            fail();
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains(
                "Could not set parameter at position 0 (values was 'a')"));
        }
    }

    @Ignore
    @Test
    public void closedBatchError() throws SQLException {
        PreparedStatement preparedStatement = sharedConnection
            .prepareStatement("SELECT ?, ? FROM DUAL");
        preparedStatement.setString(1, "1");
        preparedStatement.setString(2, "1");
        preparedStatement.addBatch();
        preparedStatement.executeBatch();

        preparedStatement.close();

        /* The connection has been set to null after execute preparedStatement.close(). It will be throw
        *  java.lang.NullPointerException for judge this.connection.getProtocol.isOracleMode().
        * **/
        try {
            preparedStatement.setString(1, "2");
            preparedStatement.setString(2, "2");
            preparedStatement.addBatch();
            preparedStatement.executeBatch();
            fail();
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("Cannot do an operation on a closed statement"));
        }
    }

    @Test
    public void executeLargeBatchError() throws SQLException {
        try {
            sharedConnection.prepareStatement("INSERT INTO unknownTable values (?, ?)");
        } catch (SQLSyntaxErrorException e) {
            Assert.assertTrue(e.getMessage().contains("ORA-00942"));
        }
        //        PreparedStatement preparedStatement = sharedConnection
        if (!sharedOptions().emulateUnsupportedPstmts) {
            Connection newConn = setConnection("&emulateUnsupportedPstmts=true");
            PreparedStatement preparedStatement = newConn
                .prepareStatement("INSERT INTO unknownTable values (?, ?)");
            preparedStatement.setString(1, "1");
            preparedStatement.setString(2, "1");
            preparedStatement.addBatch();

            try {
                preparedStatement.executeLargeBatch();
                fail();
            } catch (SQLException e) {
                e.printStackTrace();
                assertTrue(e.getMessage().contains("does not exist"));
            } finally {
                newConn.close();
            }
        }
    }

    @Test
  public void executeBatchOneByOne() throws SQLException {
    try (Connection connection =
        setConnection(
            "&rewriteBatchedStatements=false&useBulkStmts=false&useBatchMultiSend=false")) {
      Statement stmt = connection.createStatement();
        try {
            stmt.execute("drop table executeBatchOneByOne");
        } catch (SQLException e) {
        }
        stmt.execute("CREATE TABLE executeBatchOneByOne (c1 varchar(16), c2 varchar(16))");
      PreparedStatement preparedStatement =
          connection.prepareStatement("INSERT INTO executeBatchOneByOne values (?, ?)");
      preparedStatement.setString(1, "1");
      preparedStatement.setString(2, "1");
      preparedStatement.addBatch();
      preparedStatement.setQueryTimeout(10);
      assertEquals(1, preparedStatement.executeBatch().length);
      stmt.execute("DROP TABLE executeBatchOneByOne");
    }
  }

    @Test
    public void metaDataForWrongQuery() throws SQLException {
        if (!sharedOptions().emulateUnsupportedPstmts) {
            Connection newConn = setConnection("&emulateUnsupportedPstmts=true");
            PreparedStatement preparedStatement = newConn.prepareStatement("WRONG QUERY");
            assertNull(preparedStatement.getMetaData());
            try {
                preparedStatement = sharedConnection.prepareStatement("WRONG QUERY");
            } catch (SQLSyntaxErrorException e) {
                e.printStackTrace();
            } finally {
                newConn.close();
            }
        }
    }

    @Ignore
    @Test
    public void getMultipleMetaData() throws SQLException {
        PreparedStatement preparedStatement = sharedConnection
            .prepareStatement("SELECT 1 as a FROM DUAL");
        ResultSetMetaData meta = preparedStatement.getMetaData();
        //TODO check MetaData values returned (meta.getColumnName(1) -> 'A')
        assertEquals("a", meta.getColumnName(1));

        preparedStatement.execute();

        meta = preparedStatement.getMetaData();
        assertEquals("a", meta.getColumnName(1));
    }

    @Test
    public void prepareToString() throws SQLException {
        try {
            PreparedStatement preparedStatement = sharedConnection
                .prepareStatement("SELECT ? as a, ? as b");
            preparedStatement.setString(1, "a");
            preparedStatement.setNull(2, Types.VARCHAR);
            assertEquals("sql : 'SELECT ? as a, ? as b', parameters : ['a',<null>]",
                preparedStatement.toString());
        } catch (SQLSyntaxErrorException e) {
            //eat
        }
        if (!sharedOptions().emulateUnsupportedPstmts) {
            Connection newConn = setConnection("&emulateUnsupportedPstmts=true");
            PreparedStatement preparedStatement = newConn
                .prepareStatement("SELECT ? as a, ? as b from dual");
            preparedStatement.setString(1, "a");
            preparedStatement.setNull(2, Types.VARCHAR);
            assertEquals("sql : 'SELECT ? as a, ? as b from dual', parameters : ['a',<null>]",
                preparedStatement.toString());
            preparedStatement.executeQuery();
            newConn.close();
        }

    }

}
