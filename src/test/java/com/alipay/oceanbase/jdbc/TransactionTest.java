package com.alipay.oceanbase.jdbc;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TransactionTest extends BaseTest {

    /**
     * Tables initialisation.
     *
     * @throws SQLException exception
     */
    @Before
    public void before() throws SQLException {
        if (testSingleHost) {
            Statement stmt = sharedConnection.createStatement();
            stmt.execute("drop table if exists tx_fore_key");
            stmt.execute("drop table if exists tx_prim_key");
            createTable("tx_prim_key", "id int not null primary key", "engine=innodb");
            createTable(
                "tx_fore_key",
                "id int not null primary key, id_ref int not null, "
                        + "foreign key (id_ref) references tx_prim_key(id) on delete restrict on update restrict",
                "engine=innodb");
        }
    }

    /**
     * Clean up created tables.
     *
     * @throws SQLException exception
     */
    @After
    public void after() throws SQLException {
        if (testSingleHost) {
            Statement stmt = sharedConnection.createStatement();
            stmt.execute("drop table if exists tx_fore_key");
            stmt.execute("drop table if exists tx_prim_key");
        }
    }

    @Test
  public void testProperRollback() throws Exception {
    try (Statement st = sharedConnection.createStatement()) {
      st.executeUpdate("insert into tx_prim_key(id) values(32)");
      st.executeUpdate("insert into tx_fore_key(id, id_ref) values(42, 32)");
    }

    // 2. try to delete entry in Primary table in a transaction - which will fail due
    // foreign key.
    sharedConnection.setAutoCommit(false);
    try (Statement st = sharedConnection.createStatement()) {
      st.executeUpdate("delete from tx_prim_key where id = 32");
      sharedConnection.commit();
      fail("Expected SQLException");
    } catch (SQLException e) {
      // This exception is expected
      assertTrue(e.getMessage().contains("a foreign key constraint fails"));
      sharedConnection.rollback();
    }

    try (Connection conn2 = openNewConnection(connUri);
        Statement st = conn2.createStatement()) {
      st.setQueryTimeout(30000);
      st.executeUpdate("delete from tx_fore_key where id = 42");
      st.executeUpdate("delete from tx_prim_key where id = 32");
    }
  }
}
