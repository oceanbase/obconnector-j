package com.oceanbase.jdbc;

import java.sql.*;
import java.util.Calendar;

import org.junit.Assert;
import org.junit.Test;

public class ObProtocolV20OracleTest extends BaseOracleTest {

    //    private Connection conn;
    //
    //    @BeforeClass
    //    public static void getOb20Connection() throws SQLException {
    //        Connection conn = setConnection("&useOceanBaseProtocolV20=true&enableFullLinkTrace=true");
    //    }

    @Test
    public void testAppInfo() throws SQLException {
        Statement stmt = sharedConnection.createStatement();

        ((OceanBaseConnection) sharedConnection).setFullLinkTraceIdentifier("OB_JDBC");
        ((OceanBaseConnection) sharedConnection).setFullLinkTraceModule("mod", "act");
        stmt.execute("select 1 from dual");
        //testControlInfo(conn);
        //        String clientIdentifier = ((OceanBaseConnection) conn).getFullLinkTraceIdentifier();
        //        String module = ((OceanBaseConnection) conn).getFullLinkTraceModule();
        //        String action = ((OceanBaseConnection) conn).getFullLinkTraceAction();

        ((OceanBaseConnection) sharedConnection).setFullLinkTraceAction("act1");
        stmt.execute("select 1 from dual where 1=0");
        //testControlInfo(conn);
        //        action = ((OceanBaseConnection) conn).getFullLinkTraceAction();

        ((OceanBaseConnection) sharedConnection)
            .setFullLinkTraceClientInfo("OceanBase connector-j 2.2.9");
        stmt.execute("select 1 from dual where 1=0");
        //testControlInfo(conn);
        //        String clientInfo = ((OceanBaseConnection) conn).getFullLinkTraceClientInfo();
    }

    //@Test
    public void testControlInfo(Connection sharedConnection) throws SQLException {
        byte level = ((OceanBaseConnection) sharedConnection).getFullLinkTraceLevel();
        double samplePercentage = ((OceanBaseConnection) sharedConnection)
            .getFullLinkTraceSamplePercentage();
        byte recordPolicy = ((OceanBaseConnection) sharedConnection).getFullLinkTraceRecordPolicy();
        double printSamplePercentage = ((OceanBaseConnection) sharedConnection)
            .getFullLinkTracePrintSamplePercentage();
        long slowQueryThreshold = ((OceanBaseConnection) sharedConnection)
            .getFullLinkTraceSlowQueryThreshold();

        System.out.println("\n\n[control info] level: " + level + ", sample_pct: "
                           + samplePercentage + ", rp: " + recordPolicy
                           + ", print_sample_percentage: " + printSamplePercentage
                           + ", slow_query_threshold: " + slowQueryThreshold);
    }

    @Test
    public void testSlowQuery() throws SQLException {
        Statement stmt = sharedConnection.createStatement();

        stmt.execute("select test_sleep(10) from dual where 1=0");
    }

    @Test
    public void testTransaction() throws SQLException {
        Statement stmt = sharedConnection.createStatement();
        //false
        try {
            stmt.execute("begin DBMS_MONITOR.OB_TRACE_ENABLE(2, 1, 'ALL');end;");
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        //true

        // autoCommit = false
        try {
            stmt.execute("begin DBMS_MONITOR.OB_TRACE_ENABLE(2, 1, 'ALL');end;");
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        //true
        sharedConnection.setAutoCommit(false);
        //false
        stmt.execute("begin");
        //true
        stmt.execute("select 1 from dual");
        //true
        stmt.execute("select 1 from dual where 1=0");
        //true
        stmt.execute("select 1 from dual");
        //true
        sharedConnection.commit();
        //false

        // autoCommit = true
        try {
            stmt.execute("begin DBMS_MONITOR.OB_TRACE_ENABLE(2, 1, 'ALL');end;");
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        //true
        sharedConnection.setAutoCommit(true);
        //false
        stmt.execute("begin");
        //true
        stmt.execute("select 1 from dual");
        //true
        stmt.execute("select 1 from dual where 1=0");
        //true
        stmt.execute("select 1 from dual");
        //true
        sharedConnection.commit();
        //false

        try {
            stmt.execute("begin DBMS_MONITOR.OB_TENANT_TRACE_DISABLE('');end;");
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        stmt.close();
        sharedConnection.close();
    }

    @Test
    public void testSendSplitPayload() throws SQLException {
        String tableName = "test_send_split_payload";
        createTable(tableName, "c1 int, c2 varchar(30), constraint pk_yxy primary key(c1)");

        Statement stmt = sharedConnection.createStatement();

        ((OceanBaseConnection) sharedConnection).setFullLinkTraceIdentifier("OB_JDBC");
        stmt.execute("select 1 from dual");
        //testControlInfo(conn);

        String query = "insert all";
        for (int i = 1; i <= 100; i++) {
            query += " into " + tableName + " values(" + i + ",'" + i + "+string')";
        }
        query += "select * from dual";
        System.out.println("\n************* insert all into *************");
        stmt.execute(query);

        stmt.execute("select 1 from dual");
    }

    @Test
    public void testReceiveSplitPayload() throws SQLException {
        Statement stmt = sharedConnection.createStatement();

        System.out.println("\n************* select * from gv$sql_audit *************");
        stmt.execute("select * from sys.gv$ob_sql_audit");
    }

    @Test
    public void fix20220418() throws SQLException {
        Statement stmt = sharedConnection.createStatement();
        //设置租户级别的控制信息
        try {
            stmt.execute("begin DBMS_MONITOR.OB_TRACE_ENABLE(2,1,'ALL');end;");
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains(
                "Tracing for tenant is already enabled not allowed"));
        }

        //当前连接也生效
        int level = ((OceanBaseConnection) sharedConnection).getFullLinkTraceLevel();
        System.out.println("level:" + level);
        if (level != 2) {
            throw new AssertionError("level not expect");
        }
        try {
            stmt.execute("drop table test_full_link_trace");
        } catch (Exception ee) {
            System.out.println(ee.getMessage());
        }
        stmt.execute("create table test_full_link_trace(c0 int)");
        stmt.execute("begin DBMS_MONITOR.OB_TENANT_TRACE_DISABLE('');end;");
        stmt.execute("drop table test_full_link_trace");
        stmt.execute("create table test_full_link_trace(c0 int)");
        stmt.execute("insert into test_full_link_trace values(10)");
        level = ((OceanBaseConnection) sharedConnection).getFullLinkTraceLevel();
        System.out.println("level:" + level);
        if (level != -1) {
            throw new AssertionError("level not expect");
        }
        stmt.close();
        sharedConnection.close();
    }

    @Test
    public void fix40961126() throws SQLException {
        Statement stmt = sharedConnection.createStatement();

        try {
            stmt.execute("begin DBMS_MONITOR.OB_MOD_ACT_TRACE_ENABLE('OB_JDBC','OB_JDBC', 2,1,'ALL');end;");
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        ((OceanBaseConnection) sharedConnection).setFullLinkTraceModule("OB_JDBC", "OB_JDBC");
        try {
            stmt.execute("drop table test_full_link_trace");
        } catch (SQLException ee) {
            System.out.println(ee.getMessage());
        }
        stmt.execute("create table test_full_link_trace(c0 int)");
        int level = ((OceanBaseConnection) sharedConnection).getFullLinkTraceLevel();
        System.out.println("level1:" + level);
        if (level != 2) {
            throw new AssertionError("level not expect");
        }
        stmt.execute("begin DBMS_MONITOR.OB_MOD_ACT_TRACE_DISABLE('OB_JDBC','OB_JDBC');end;");
        stmt.execute("drop table test_full_link_trace");
        stmt.execute("create table test_full_link_trace(c0 int)");

        //disable之后应该失效
        level = ((OceanBaseConnection) sharedConnection).getFullLinkTraceLevel();
        System.out.println("level2:" + level);
        if (level != -1) {
            throw new AssertionError("level not expect");
        }
        stmt.close();
    }

    @Test
    public void fix41103844() throws SQLException {
        sharedConnection.setAutoCommit(false);
        Statement stmt = sharedConnection.createStatement();

        //stmt.execute("begin");
        //设置租户级别的控制信息
        try {
            stmt.execute("begin DBMS_MONITOR.OB_TRACE_ENABLE(2,1,'ALL');end;");
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        try {
            stmt.execute("drop table test_full_link_trace");
        } catch (Exception ee) {
            System.out.println(ee.getMessage());
        }
        try {
            stmt.execute("drop table test_full_link_trace");
        } catch (Exception ee) {
            System.out.println(ee.getMessage());
        }
        stmt.execute("create table test_full_link_trace(c0 int)");
        try {
            stmt.execute("create table test_full_link_trace(c0 int)");
        } catch (Exception ee) {
            System.out.println(ee.getMessage());
        }
        stmt.execute("insert into test_full_link_trace values(10)");
        sharedConnection.commit();
        stmt.execute("begin DBMS_MONITOR.OB_TENANT_TRACE_DISABLE('');end;");
        stmt.close();
    }

    @Test
    public void fix41249367() throws SQLException {
        Connection conn = sharedConnection;
        Statement stmt = conn.createStatement();
        conn.setAutoCommit(false);
        //设置租户级别的控制信息
        try {
            stmt.execute("begin DBMS_MONITOR.OB_TRACE_ENABLE(1,0.002,'ALL');end;");
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        try {
            stmt.execute("drop table test_full_link_trace");
        } catch (Exception ee) {
            System.out.println(ee.getMessage());
        }
        System.out.println("level:" + ((OceanBaseConnection) conn).getFullLinkTraceLevel());
        System.out.println("sample_percentage:"
                           + ((OceanBaseConnection) conn).getFullLinkTraceSamplePercentage());
        System.out.println("slow_query_threshold:"
                           + ((OceanBaseConnection) conn).getFullLinkTraceSlowQueryThreshold());
        System.out.println("print_sample_percentage:"
                           + ((OceanBaseConnection) conn).getFullLinkTracePrintSamplePercentage());
        stmt.execute("create table test_full_link_trace(c0 int, c1 int)");
        for (int i = 0; i < 1000; i++) {
            stmt.execute("insert into test_full_link_trace values(1,1)");
        }
        conn.commit();
        PreparedStatement statement2 = conn
            .prepareStatement("begin DBMS_MONITOR.OB_TENANT_TRACE_DISABLE(?);end;");
        statement2.setString(1, "oracle");
        statement2.execute();
        statement2.close();
        stmt.close();
    }

    @Test
    public void fix41301627() throws SQLException {
        Statement stmt = sharedConnection.createStatement();
        try {
            stmt.execute("begin DBMS_MONITOR.OB_TRACE_ENABLE(1, 1, 'ONLY_SLOW_QUERY');end;");
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        try {
            stmt.execute("drop table test_full_link_trace");
        } catch (Exception ee) {
            System.out.println(ee.getMessage());
        }
        stmt.execute("create table test_full_link_trace(c0 int)");

        stmt.execute("begin DBMS_MONITOR.OB_TENANT_TRACE_DISABLE('');end;");
    }

    @Test
    public void fix41561698() throws SQLException, ClassNotFoundException {
        String driver = "com.alipay.oceanbase.jdbc.Driver";
        Class.forName(driver);
        String username = "test@oracle";
        String password = "test";

        String host = "100.88.161.71";
        int port = 3063;
        //&useServerPrepStmts=true
        String url = String
            .format(
                "jdbc:oceanbase://%s:%d/%s?serverTimezone=UTC&useSSL=false&allowPublicKeyRetrieval=true&useOceanBaseProtocolV20=true&enableFullLinkTrace=true",
                host, port, "test");

        Connection conn1 = DriverManager.getConnection(url, username, password);
        Statement stmt1 = conn1.createStatement();
        try {
            stmt1.execute("begin DBMS_MONITOR.OB_TRACE_ENABLE(1, 1, 'SAMPLE_AND_SLOW_QUERY');end;");
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }

        Connection conn = DriverManager.getConnection(url, username, password);
        Statement stmt = conn.createStatement();
        System.out.println("level:" + ((OceanBaseConnection) conn).getFullLinkTraceLevel());
        System.out.println("sample_percentage:"
                           + ((OceanBaseConnection) conn).getFullLinkTraceSamplePercentage());
        System.out.println("slow_query_threshold:"
                           + ((OceanBaseConnection) conn).getFullLinkTraceSlowQueryThreshold());
        System.out.println("print_sample_percentage:"
                           + ((OceanBaseConnection) conn).getFullLinkTracePrintSamplePercentage());
        try {
            stmt.execute("drop table test_full_link_trace");
        } catch (Exception ee) {
            System.out.println(ee.getMessage());
        }
        stmt.execute("create table test_full_link_trace(c0 int, c1 int)");
        stmt.execute("insert into test_full_link_trace values(1, 1)");

        PreparedStatement statement2 = conn
            .prepareStatement("begin DBMS_MONITOR.OB_TENANT_TRACE_DISABLE(?);end;");
        statement2.setString(1, "oracle");
        statement2.execute();
        statement2.close();
        stmt.close();
        conn.close();
    }

    @Test
    public void fix42579495() {
        String host = "100.83.231.1";
        int port = 2828;
        String username = "test@oracle";
        String password = "test";
        String url = String
            .format(
                "jdbc:oceanbase://%s:%d/%s?user=%s&password=%s&serverTimezone=UTC&useSSL=false&allowPublicKeyRetrieval=true&useOceanBaseProtocolV20=true&enableFullLinkTrace=true"
                        + "&useServerPrepStmts=false&useOraclePrepareExecute=true&rewriteBatchedStatements=true",
                host, port, "test", username, password);

        try {
            Connection connection = openNewConnection(url);
            Statement stmt = connection.createStatement();
            //            try {
            //                stmt.execute("begin dbms_monitor.ob_tenant_trace_disable('');end;");
            //            } catch (SQLException e) {
            //                System.out.println(e.getMessage());
            //            }
            //            stmt.execute("begin dbms_monitor.ob_tenant_trace_enable(1,1,'only_slow_query');end;");

            try {
                stmt.execute("drop table t0");
            } catch (SQLException e) {
                System.out.println(e.getMessage());
            }
            stmt.execute("create table t0(c0 int)");

            connection.setAutoCommit(false);
            stmt.execute("insert into t0 values(10);");
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.getMessage();
            }
            stmt.execute("insert into t0 values(20)");
            stmt.execute("insert into t0 values(30);");
            connection.commit();

            connection.close();
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        } catch (Exception e) {

        }
    }

    @Test
    public void testSlowQueryFailed() {
        String host = "100.83.231.1";
        int port = 2828;
        String username = "test@oracle";
        String password = "test";
        String url = String
            .format(
                "jdbc:oceanbase://%s:%d/%s?user=%s&password=%s&serverTimezone=UTC&useSSL=false&allowPublicKeyRetrieval=true&useOceanBaseProtocolV20=true&enableFullLinkTrace=true"
                        + "&useServerPrepStmts=false&useOraclePrepareExecute=true&rewriteBatchedStatements=true",
                host, port, "test", username, password);

        try {
            Connection connection = openNewConnection(url);
            Statement stmt = connection.createStatement();
            try {
                stmt.execute("begin dbms_monitor.ob_tenant_trace_disable('');end;");
            } catch (SQLException e) {
                System.out.println(e.getMessage());
            }
            stmt.execute("begin dbms_monitor.ob_tenant_trace_enable(1,1,'only_slow_query');end;");

            try {
                stmt.execute("drop table t0");
            } catch (SQLException e) {
                System.out.println(e.getMessage());
            }
            try {
                stmt.execute("drop table t1");
            } catch (SQLException e) {
                System.out.println(e.getMessage());
            }

            stmt.execute("create table t0(c0 int)");
            stmt.execute("create table t1(c0 int)");
            /*connection.setAutoCommit(false);
            stmt.execute("insert into t0 values(10);");
            stmt.execute("insert into t0 values(20)");
            stmt.execute("insert into t0 values(30);");
            PreparedStatement ps = connection.prepareStatement("insert into t1 values(?)");
            for(int i=0;i<1000;i++){
                ps.setInt(1, i);
                ps.addBatch();
            }
            ps.executeBatch();*/
            // connection.commit();
            connection.close();

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        } catch (Exception e) {

        }
    }

    @Test
    public void fix42878386() {
        String host = "100.83.231.1";
        int port = 2828;
        String username = "test@oracle";
        String password = "test";
        String url = String
            .format(
                "jdbc:oceanbase://%s:%d/%s?user=%s&password=%s&serverTimezone=UTC&useSSL=false&allowPublicKeyRetrieval=true&useOceanBaseProtocolV20=true&enableFullLinkTrace=true&useServerPrepStmts=true&useOraclePrepareExecute=true&rewriteBatchedStatements=false",
                host, port, "test", username, password);
        try {
            Connection connection = openNewConnection(url);
            Statement stmt = connection.createStatement();
            connection.setAutoCommit(false);
            try {
                stmt.execute("drop table t0");
                stmt.execute("drop table t1");
            } catch (SQLException e) {
                System.out.println(e.getMessage());
            }

            stmt.execute("create table t0(c0 int)");
            stmt.execute("create table t1(c0 int)");
            PreparedStatement ps = connection.prepareStatement("insert into t0 values(?)");
            for (int i = 0; i < 200; i++) {
                ps.setInt(1, i);
                ps.addBatch();
            }
            Calendar Cld = Calendar.getInstance();
            int MI = Cld.get(Calendar.MILLISECOND);
            System.out.println(MI);
            ps.executeBatch();
            Cld = Calendar.getInstance();
            MI = Cld.get(Calendar.MILLISECOND);
            System.out.println(MI);
            PreparedStatement ps2 = connection.prepareStatement("insert into t1 values(?)");
            for (int i = 0; i < 200; i++) {
                ps2.setInt(1, i);
                ps2.addBatch();
            }
            ps2.executeBatch();

            stmt.execute("insert into t0 values(10)");
            stmt.execute("select * from t0,t1");
            stmt.execute("select t1.c0, t0.c0 from t1,t0");
            stmt.execute("select count(*) from t00");
            connection.commit();

            connection.close();
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    @Test
    public void fix44336056() throws SQLException {
        //        String username = "test@oracle";
        //        String password = "test";
        //
        //        String host = "100.88.161.71";
        //        int port = 4063;
        //
        //        String url = String
        //            .format(
        //                "jdbc:oceanbase://%s:%d/%s?user=%s&password=%s&serverTimezone=UTC&useSSL=false&allowPublicKeyRetrieval=true&useOceanBaseProtocolV20=true&enableFullLinkTrace=true&useServerPrepStmts=true",
        //                host, port, "test", username, password);
        //
        //        Connection conn = openNewConnection(url);
        Connection conn = sharedConnection;
        String tableName = "test_send_split_payload";
        createTable(tableName, "c1 int, c2 varchar(30), constraint pk_yxy primary key(c1)");

        ((OceanBaseConnection) conn).setFullLinkTraceIdentifier("OB_JDBC");
        Statement stmt = conn.createStatement();
        stmt.execute("select 1 from dual");

        String x = "";
        for (int j = 0; j < 1000; j++) {
            x += "hello000000000012313123";
        }

        String query = "insert all";
        for (int i = 1; i <= 800; i++) {
            query += " into " + tableName + " values(" + i + ",'" + i + x + "')";
        }
        query += "select * from dual";
        System.out.println("\n************* insert all into *************");
        System.out.println("\n query len:" + query.length());
        stmt.execute(query);
        stmt.execute("select 1 from dual");
        conn.commit();
        stmt.close();
        conn.close();
    }
}
