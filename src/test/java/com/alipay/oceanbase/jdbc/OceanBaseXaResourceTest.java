package com.alipay.oceanbase.jdbc;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class OceanBaseXaResourceTest {
    @Test
    public void xidToString() {
        assertEquals("0x00,0x01,0x05", OceanBaseXaResource.xidToString(new OceanBaseXid(5,
            new byte[] { 0x00 }, new byte[] { 0x01 })));
        assertEquals(
            "0x,0x000100,0x0400",
            OceanBaseXaResource.xidToString(new OceanBaseXid(1024, new byte[] {}, new byte[] {
                    0x00, 0x01, 0x00 })));
        assertEquals("0x00,0x000100,0xC3C20186", OceanBaseXaResource.xidToString(new OceanBaseXid(
            -1010695802, new byte[] { 0x00 }, new byte[] { 0x00, 0x01, 0x00 })));
    }
}
