package com.alipay.oceanbase.jdbc.internal.util.exceptions;

import java.io.IOException;

public class MaxAllowedPacketException extends IOException {

    private final boolean mustReconnect;

    public MaxAllowedPacketException(String message, boolean mustReconnect) {
        super(message);
        this.mustReconnect = mustReconnect;
    }

    public boolean isMustReconnect() {
        return mustReconnect;
    }
}
