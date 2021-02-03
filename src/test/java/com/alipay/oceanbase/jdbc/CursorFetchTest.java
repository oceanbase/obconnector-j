package com.alipay.oceanbase.jdbc;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.*;

public class CursorFetchTest extends BaseTest {
    static String tableName1 = "test_cursor_mysql";

    @BeforeClass()
    public static void initClass() throws SQLException {
        createTable(tableName1, "c1 int,c2 varchar(30)");

    }

    @Test
    public void testCursorFetch() {
        try {
            Connection conn = setConnection("&useCursorFetch=true");
            Statement statement = conn.createStatement();
            int i;
            String query = "insert into " + tableName1 + " values(1,'1+string')";
            for (i = 2; i < 100; i++) {
                query += ",(" + i + ",'" + i + "+string')";
            }
            System.out.println("query = " + query);
            statement.execute(query);

            PreparedStatement preparedStatement = conn.prepareStatement("select * from "
                                                                        + tableName1);
            preparedStatement.setFetchSize(1);
            ResultSet rs = preparedStatement.executeQuery();
            int count = 1;
            int j = 1;
            while (rs.next()) {
                Assert.assertEquals(j, rs.getInt(1));
                Assert.assertEquals(j + "+string", rs.getString(2));
                j++;
                count++;
            }
            Assert.assertEquals(count, 100);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Ignore
    public void testCursorFetchWithReflect() {
        try {
            Connection conn = setConnection("&useCursorFetch=true");
            Statement statement = conn.createStatement();
            try {
                statement.execute("drop table executeBatchOneByOne");
            } catch (SQLException e) {
                // eat exception
            }
            statement.execute("CREATE TABLE executeBatchOneByOne (c1 varchar(16), c2 varchar(16))");
            PreparedStatement preparedStatement = conn
                .prepareStatement("INSERT INTO executeBatchOneByOne values (?, ?)");
            preparedStatement.setString(1, "1");
            preparedStatement.setString(2, "1");
            preparedStatement.addBatch();
            preparedStatement.executeBatch();

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
