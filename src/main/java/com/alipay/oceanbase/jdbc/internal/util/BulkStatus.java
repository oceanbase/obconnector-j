package com.alipay.oceanbase.jdbc.internal.util;

public class BulkStatus {

    public volatile int     sendSubCmdCounter;
    public volatile boolean sendEnded;
    public int              sendCmdCounter = 0;
}
