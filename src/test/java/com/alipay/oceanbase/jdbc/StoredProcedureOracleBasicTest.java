package com.alipay.oceanbase.jdbc;

import org.junit.Test;

import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.Types;

import static org.junit.Assert.assertEquals;

public class StoredProcedureOracleBasicTest extends BaseOracleTest {
    @Test
    public void testAdd() throws Exception {
        String procedureName = "calc_add_" + System.currentTimeMillis();
        createProcedure(
                procedureName,
                "(a1 IN int, a2 IN int, a3 OUT int) is "
                        + "begin " + "  a3:=a1+a2; " + "end;"
        );
        try (CallableStatement callableStatement = sharedConnection.prepareCall("call " + procedureName + "(?, ?, ?)")) {
            callableStatement.setInt(1, 1);
            callableStatement.setInt(2, 2);
            callableStatement.registerOutParameter(3, Types.INTEGER);
            callableStatement.execute();
            //System.out.println(callableStatement.getInt(2));
            ResultSet rs = callableStatement.getResultSet();
            System.out.println("rs is:" + callableStatement.getDouble(3));
            if (rs != null && rs.next()) {
                System.out.println("THE RESULT IS:" + rs.getInt(3));
            }
            assertEquals(3 == callableStatement.getDouble(3), true);

        } finally {
            dropProcedure(procedureName);
        }
    }

    @Test
    public void testLongStoreProcedureWithFullName() throws Exception {
        //for issue https://aone.alibaba-inc.com/v2/project/906355/req/32951377
        String procedureName = "calc_add_" + System.currentTimeMillis();

        createProcedureWithPackage(
                "test", procedureName,
                "(a1 IN int, a2 IN int, a3 OUT int); end;",
                "(a1 IN int, a2 IN int, a3 OUT int) is "
                        + "begin " + "  a3:=a1+a2; " + "end;end;"
        );
        try (CallableStatement callableStatement = sharedConnection.prepareCall("call sys.test." + procedureName + "(?, ?, ?)")) {
            callableStatement.setInt(1, 1);
            callableStatement.setInt(2, 2);
            callableStatement.registerOutParameter(3, Types.INTEGER);
            callableStatement.execute();
            //System.out.println(callableStatement.getInt(2));
            ResultSet rs = callableStatement.getResultSet();
            System.out.println("rs is:" + callableStatement.getDouble(3));
            if (rs != null && rs.next()) {
                System.out.println("THE RESULT IS:" + rs.getInt(3));
            }
            assertEquals(3 == callableStatement.getDouble(3), true);

        } finally {
            dropProcedure("test." + procedureName);
        }
    }
}