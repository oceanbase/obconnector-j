package com.alipay.oceanbase.jdbc;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class AsSqlTest extends BaseTest {
    @BeforeClass
    public static void initClass() throws SQLException {
        createTable("astest1", "c1 int");
    }

    @Test
    public void testAsSql() {
        Assume.assumeFalse(sharedUsePrepare());
        try {
            PreparedStatement pstmt = sharedConnection
                .prepareStatement("insert into astest1 values(?),(?)");
            pstmt.setInt(1, 2);
            pstmt.setInt(2, 2);

            pstmt.addBatch();
            pstmt.executeUpdate();
            Assert.assertEquals("insert into astest1 values(2),(2)",
                ((JDBC4PreparedStatement) pstmt).asSql());
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }
}
