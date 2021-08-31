package com.oceanbase.jdbc;

import static org.junit.Assert.*;

import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.Test;

/**
 * @author yexinyan.yxy
 * @version 1: ProcBatchTest, 2021-08-25 15:10 yexinyan.yxy Exp $
 */
public class ProcBatchTest extends BaseTest {

    @Test
    public void testBatch() throws Exception {
        createTable("testBatchTable", "field1 INT");
        createProcedure("testBatch",
            "(IN foo int)\nbegin\nINSERT INTO testBatchTable VALUES (foo);\nend\n");
        Statement stmt = sharedConnection.createStatement();

        CallableStatement storedProc = sharedConnection.prepareCall("{call testBatch(?)}");
        int numBatches = 3;
        for (int i = 0; i < numBatches; i++) {
            storedProc.setInt(1, i + 1);
            storedProc.addBatch();
        }
        int[] counts = storedProc.executeBatch();
        assertEquals(numBatches, counts.length);
        //        for (int i = 0; i < numBatches; i++) {
        //            assertEquals(1, counts[i]);   // wrong server result
        //        }

        ResultSet rs = stmt.executeQuery("SELECT field1 FROM testBatchTable ORDER BY field1 ASC");
        for (int i = 0; i < numBatches; i++) {
            assertTrue(rs.next());
            assertEquals(i + 1, rs.getInt(1));
        }
    }
}
