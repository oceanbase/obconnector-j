package com.alipay.oceanbase.jdbc;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class OracleConnectionCompatibilityTest extends BaseOracleTest {

    @BeforeClass
    public static void initClass() throws SQLException {
        createTable("oracle_conn__test", "id int primary key");
    }

    @Before
    public void checkSupported() throws SQLException {
        requireMinimumVersion(5, 1);
    }

    @Test
    public void testAutoCommitAndRollback() throws SQLException {
        Statement stmt = sharedConnection.createStatement();
        sharedConnection.setAutoCommit(true);
        stmt.execute("delete from oracle_conn__test;");
        assertEquals(true, sharedConnection.getAutoCommit());
        sharedConnection.setAutoCommit(false);
        assertEquals(false, sharedConnection.getAutoCommit());
        stmt.execute("insert into oracle_conn__test values(10);");
        sharedConnection.rollback();
        stmt.execute("insert into oracle_conn__test values(11);");
        sharedConnection.commit();
        ResultSet rs = stmt.executeQuery("select * from oracle_conn__test;");
        assertEquals(true, rs.next());
        assertEquals(false, rs.next());

    }

    @Test
    public void testTransactionIsolation() throws SQLException {
        boolean support = true;

        assertEquals(2, sharedConnection.getTransactionIsolation());

        try {
            sharedConnection.setTransactionIsolation(Connection.TRANSACTION_NONE);
        } catch (SQLException e) {
            support = false;
        }

        assertEquals(false, support);

        assertEquals(Connection.TRANSACTION_READ_COMMITTED,
            sharedConnection.getTransactionIsolation());

        support = true;
        try {
            sharedConnection.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
        } catch (SQLException e) {
            support = false;
        }
        assertEquals(false, support);
        assertEquals(Connection.TRANSACTION_READ_COMMITTED,
            sharedConnection.getTransactionIsolation());

        support = true;
        try {
            sharedConnection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
        } catch (SQLException e) {
            support = false;
        }
        assertEquals(false, support);
        assertEquals(Connection.TRANSACTION_READ_COMMITTED,
            sharedConnection.getTransactionIsolation());

        support = true;
        try {
            sharedConnection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
        } catch (SQLException e) {
            support = false;
        }
        assertEquals(true, support);
        assertEquals(Connection.TRANSACTION_SERIALIZABLE,
            sharedConnection.getTransactionIsolation());
    }

    @Test
    public void testReadOnly() throws SQLException {
        assertEquals(false, sharedConnection.isReadOnly());
        sharedConnection.setReadOnly(true);
        assertEquals(true, sharedConnection.isReadOnly());
    }

    @Test
    public void testSavePoint() throws SQLException {
        Statement stmt = sharedConnection.createStatement();

        boolean succeed = true;
        try {
            sharedConnection.setSavepoint();
        } catch (SQLException e) {
            //ignore , autoCommit is true , setSavepoint Is bound to fail
            succeed = false;
        }
        assertEquals(false, succeed);

        sharedConnection.setAutoCommit(false);
        stmt.execute("insert into oracle_conn__test values(0);");
        Savepoint savepoint = sharedConnection.setSavepoint();
        stmt.execute("insert into oracle_conn__test values(1);");
        sharedConnection.rollback(savepoint);
        sharedConnection.commit();
        ResultSet rs = stmt.executeQuery("select * from oracle_conn__test");
        assertEquals(true, rs.next());
        assertEquals(false, rs.next());
    }

    @Test
    public void testGetSchema() throws SQLException {
        String schema = sharedConnection.getSchema();
        assertEquals(schema, sharedConnection.getMetaData().getUserName());
    }

    @Test
    public void testTypeMap() throws SQLException {
        HashMap map = new HashMap<String, Class<?>>();
        map.put("1", Integer.class);
        map.put("2", Double.class);
        map.put("3", Float.class);

        sharedConnection.setTypeMap(map);
        Map<String, Class<?>> typeMap = sharedConnection.getTypeMap();
        assertEquals(map.size(),typeMap.size());
        typeMap.keySet().forEach(s -> assertEquals(typeMap.get(s),map.get(s)));
    }

    @Test
    public void testCatalog() throws SQLException {

        //In Oracle, return null here
        assertEquals(null, sharedConnection.getCatalog());

        //In Oracle,This is not implemented
        sharedConnection.setCatalog("test");
        assertEquals(null, sharedConnection.getCatalog());
    }
}
