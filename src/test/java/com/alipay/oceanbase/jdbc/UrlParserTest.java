package com.alipay.oceanbase.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.SQLException;

import com.alipay.oceanbase.jdbc.internal.util.constant.HaMode;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author hongwei.yhw
 * @since 2020-Sep-16
 */
public class UrlParserTest {

    @Test
    public void test_acceptsUrl() {
        assertTrue(UrlParser.acceptsUrl("jdbc:oceanbase://127.1:3306/schema"));
        assertTrue(UrlParser.acceptsUrl("jdbc:oceanbase://127.1:3306/schema?a=b"));
        assertTrue(UrlParser.acceptsUrl("jdbc:oceanbase://127.1:3306"));
        assertTrue(UrlParser.acceptsUrl("jdbc:oceanbase://127.1:3306/?a=b"));
        assertTrue(UrlParser.acceptsUrl("jdbc:oceanbase:oracle://127.1:3306/schema"));
        assertTrue(UrlParser.acceptsUrl("jdbc:oceanbase:oracle://127.1:3306/schema?a=b"));
        assertTrue(UrlParser.acceptsUrl("jdbc:oceanbase:oracle://127.1:3306/?a=b"));
    }

    @Test
    public void test_parse() throws SQLException {
        UrlParser parser = UrlParser.parse("jdbc:oceanbase://127.1:3306/schema?a=b");
        assertEquals("jdbc:oceanbase://127.1:3306/schema?a=b", parser.getInitialUrl());
        assertEquals("127.1", parser.getHostAddresses().get(0).host);
        assertEquals(3306, parser.getHostAddresses().get(0).port);
        assertEquals("schema", parser.getDatabase());
        Assert.assertEquals(HaMode.NONE, parser.getHaMode());

        parser = UrlParser.parse("jdbc:oceanbase:loadbalance://127.1:3306/schema?a=b");
        assertEquals("jdbc:oceanbase:loadbalance://127.1:3306/schema?a=b", parser.getInitialUrl());
        assertEquals("127.1", parser.getHostAddresses().get(0).host);
        assertEquals(3306, parser.getHostAddresses().get(0).port);
        assertEquals("schema", parser.getDatabase());
        Assert.assertEquals(HaMode.LOADBALANCE, parser.getHaMode());

        parser = UrlParser.parse("jdbc:oceanbase:oracle://127.1:3306/schema?a=b");
        assertEquals("jdbc:oceanbase:oracle://127.1:3306/schema?a=b", parser.getInitialUrl());
        assertEquals("127.1", parser.getHostAddresses().get(0).host);
        assertEquals(3306, parser.getHostAddresses().get(0).port);
        assertEquals("schema", parser.getDatabase());
        Assert.assertEquals(HaMode.NONE, parser.getHaMode());

        parser = UrlParser.parse("jdbc:oceanbase:oracle:loadbalance://127.1:3306/schema?a=b");
        assertEquals("jdbc:oceanbase:oracle:loadbalance://127.1:3306/schema?a=b",
            parser.getInitialUrl());
        assertEquals("127.1", parser.getHostAddresses().get(0).host);
        assertEquals(3306, parser.getHostAddresses().get(0).port);
        assertEquals("schema", parser.getDatabase());
        Assert.assertEquals(HaMode.LOADBALANCE, parser.getHaMode());
    }
}
