package com.oceanbase.jdbc;

import org.junit.Assert;
import org.junit.Test;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class ResultSetMetaDataOracleTest extends BaseOracleTest {

    @Test
    public void testForAone57111946() throws SQLException {
        Connection conn = setConnection("&useNewResultSetMetaData=true&compatibleOjdbcVersion=6");
        createTable("t_meta", "c0 char(1), c1 char(51), c2 nchar(1), c3 nchar(51), c4 varchar2(10), c5 varchar2(51), " +
                "c6 varchar(1), c7 varchar(51), c8 nvarchar2(1), c9 nvarchar2(51), c10 number, c11 number(7,2), c12 decimal, c13 decimal(7,2), " +
                "c14 integer, c15 float, c16 float(3), c17 BINARY_FLOAT, c18 BINARY_DOUBLE, c19 DATE, c20 timestamp, c21 timestamp(3)," +
                "c22 timestamp with local time zone, c23 timestamp(2) with local time zone, c24 timestamp with time zone, c25 timestamp(2) with time zone," +
                "c26 interval year to month, c27 interval year(9) to month, c28 interval day to second, c29 interval day(3) to second(4), c30 raw(1)," +
                "c31 raw(51), c32 ROWID, c33 urowid, c34 urowid(2), c35 BLOB, c36 clob, c37 xmltype, c38 json");
        class metaInfo{
            private String columnName;
            private String columnDefine;
            private int[] metainfos;
            metaInfo(String columnName, String columnDefine, int[] metainfos){
                this.columnName = columnName;
                this.columnDefine = columnDefine;
                this.metainfos = metainfos;
            }
        }

        List<metaInfo> metaInfoListlist = new ArrayList<>();
        metaInfoListlist.add(new metaInfo("c0", "char(1)", new int[]{1, 1, 0}));
        metaInfoListlist.add(new metaInfo("c1", "char(51)", new int[]{51, 51, 0}));
        metaInfoListlist.add(new metaInfo("c2", "nchar(1)", new int[]{1, 1, 0}));
        metaInfoListlist.add(new metaInfo("c3", "nchar(51)", new int[]{51, 51, 0}));
        metaInfoListlist.add(new metaInfo("c4", "varchar2(10)", new int[]{10, 10, 0}));
        metaInfoListlist.add(new metaInfo("c5", "varchar2(51)", new int[]{51, 51, 0}));
        metaInfoListlist.add(new metaInfo("c6", "varchar(1)", new int[]{1, 1, 0}));
        metaInfoListlist.add(new metaInfo("c7", "varchar(51)", new int[]{51, 51, 0}));
        metaInfoListlist.add(new metaInfo("c8", "nvarchar2(1)", new int[]{1, 1, 0}));
        metaInfoListlist.add(new metaInfo("c9", "nvarchar2(51)", new int[]{51, 51, 0}));
        metaInfoListlist.add(new metaInfo("c10", "number", new int[]{39, 0, 0}));
        metaInfoListlist.add(new metaInfo("c11", "number(7,2)", new int[]{9, 7, 2}));
        metaInfoListlist.add(new metaInfo("c12", "decimal", new int[]{39, 0, 0}));
        metaInfoListlist.add(new metaInfo("c13", "decimal(7,2)", new int[]{9, 7, 2}));
        metaInfoListlist.add(new metaInfo("c14", "integer", new int[]{39, 0, 0}));
        metaInfoListlist.add(new metaInfo("c15", "float", new int[]{39, 126, -127}));
        metaInfoListlist.add(new metaInfo("c16", "float(3)", new int[]{2, 3, -127}));
        metaInfoListlist.add(new metaInfo("c17", "BINARY_FLOAT", new int[]{12, 0, 0}));
        metaInfoListlist.add(new metaInfo("c18", "BINARY_DOUBLE", new int[]{22, 0, 0}));
        metaInfoListlist.add(new metaInfo("c19", "DATE", new int[]{19, 0, 0}));
        metaInfoListlist.add(new metaInfo("c20", "timestamp", new int[]{26, 0, 6}));
        metaInfoListlist.add(new metaInfo("c21", "timestamp(3)", new int[]{23, 0, 3}));
        metaInfoListlist.add(new metaInfo("c22", "timestamp with local time zone", new int[]{26, 0, 6}));
        metaInfoListlist.add(new metaInfo("c23", "timestamp(2) with local time zone", new int[]{22, 0, 2}));
        metaInfoListlist.add(new metaInfo("c24", "timestamp with time zone", new int[]{122, 0, 6}));
        metaInfoListlist.add(new metaInfo("c25", "timestamp(2) with time zone", new int[]{118, 0, 2}));
        metaInfoListlist.add(new metaInfo("c26", "interval year to month", new int[]{6, 2, 0}));
        metaInfoListlist.add(new metaInfo("c27", "interval year(9) to month", new int[]{13, 9, 0}));
        metaInfoListlist.add(new metaInfo("c28", "interval day to second", new int[]{19, 2, 6}));
        metaInfoListlist.add(new metaInfo("c29", "interval day(3) to second(4)", new int[]{18, 3, 4}));
        metaInfoListlist.add(new metaInfo("c30", "raw(1)", new int[]{1, 0, 0}));
        metaInfoListlist.add(new metaInfo("c31", "raw(51)", new int[]{51, 0, 0}));
        metaInfoListlist.add(new metaInfo("c32", "ROWID", new int[]{4000, 0, 0}));
        metaInfoListlist.add(new metaInfo("c33", "urowid", new int[]{4000, 0, 0}));
        metaInfoListlist.add(new metaInfo("c34", "urowid(2)", new int[]{2, 0, 0}));
        metaInfoListlist.add(new metaInfo("c35", "BLOB", new int[]{536870911, -1, 0}));
        metaInfoListlist.add(new metaInfo("c36", "clob", new int[]{2147483644, -1, 0}));
        metaInfoListlist.add(new metaInfo("c37", "xmlty", new int[]{536870911, 0, 0}));
        metaInfoListlist.add(new metaInfo("c38",  "json", new int[]{2147483644, -1, 0}));

        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("select * from t_meta");
        ResultSetMetaData metaData = rs.getMetaData();
        int size = metaData.getColumnCount();
        for (int i = 1; i <= size; i++) {
            metaInfo metaInfo = metaInfoListlist.get(i - 1);
            Assert.assertEquals(metaInfo.metainfos[0], metaData.getColumnDisplaySize(i));
            Assert.assertEquals(metaInfo.metainfos[1], metaData.getPrecision(i));
            Assert.assertEquals(metaInfo.metainfos[2], metaData.getScale(i));
        }
    }

}
