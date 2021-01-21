package com.alipay.oceanbase.jdbc;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import java.sql.*;

public class RowidTest extends BaseOracleTest {
    @BeforeClass()
    public static void initClass() throws SQLException {
        createTable("test_rowid", "col1 int, col2 varchar(20)");
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
                    System.out.println(resultSet.getMetaData().getColumnName(i) + ":"
                                       + resultSet.getString("id"));
                }
            }
        } catch (SQLException e) {
            Assert.assertNull(e);
        }
    }
}
