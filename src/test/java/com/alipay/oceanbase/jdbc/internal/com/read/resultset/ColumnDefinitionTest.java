package com.alipay.oceanbase.jdbc.internal.com.read.resultset;

import com.alipay.oceanbase.jdbc.internal.ColumnType;
import org.junit.Assert;
import org.junit.Test;

public class ColumnDefinitionTest {

    @Test
    public void fastPathTest() {
        String column4BytesUtf8 = "_\uD83D\uDE0E\uD83D\uDE0E\uD83C\uDF36\uD83C\uDF36\uD83C\uDFA4\uD83C\uDFA4\uD83E\uDD42\uD83E\uDD42";
//        String column4BytesUtf8 = "myname1111111111111111111111111111111111";
        ColumnDefinition col = ColumnDefinition.create(column4BytesUtf8 + column4BytesUtf8,
            ColumnType.STRING, false);
        Assert.assertEquals("", col.getDatabase());
        Assert.assertEquals("", col.getTable());
        Assert.assertEquals("", col.getOriginalTable());
        String str1 = column4BytesUtf8 + column4BytesUtf8;
        String str2 = col.getName();
        Assert.assertEquals(column4BytesUtf8 + column4BytesUtf8, col.getName());
        Assert.assertEquals(column4BytesUtf8 + column4BytesUtf8, col.getOriginalName());
        Assert.assertEquals(33, col.getCharsetNumber());
    }
}
