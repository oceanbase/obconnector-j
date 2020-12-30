package com.alipay.oceanbase.jdbc.internal.failover;

public class HandleErrorResult {

    public boolean mustThrowError = true;
    public boolean isReconnected  = false;

    public Object  resultObject   = null;

    public HandleErrorResult() {
    }

    public HandleErrorResult(boolean isReconnected) {
        this.isReconnected = isReconnected;
    }

    public HandleErrorResult(boolean isReconnected, boolean mustThrowError) {
        this.isReconnected = isReconnected;
        this.mustThrowError = mustThrowError;
    }

    @Override
    public String toString() {
        return "HandleErrorResult{" + "mustThrowError=" + mustThrowError + ", isReconnected="
               + isReconnected + ", resultObject=" + resultObject + "}";
    }
}
