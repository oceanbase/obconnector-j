package com.alipay.oceanbase.jdbc.internal.failover.tools;

public class SearchFilter {

    private boolean fineIfFoundOnlyMaster;
    private boolean fineIfFoundOnlySlave;
    private boolean initialConnection;
    private boolean failoverLoop;

    public SearchFilter() {
        // empty constructor
    }

    /**
     * Constructor.
     *
     * @param fineIfFoundOnlyMaster stop searching if master found
     * @param fineIfFoundOnlySlave stop searching if slave found
     */
    public SearchFilter(boolean fineIfFoundOnlyMaster, boolean fineIfFoundOnlySlave) {
        this.fineIfFoundOnlyMaster = fineIfFoundOnlyMaster;
        this.fineIfFoundOnlySlave = fineIfFoundOnlySlave;
    }

    /**
     * Constructor.
     *
     * @param initialConnection initial connection flag
     */
    public SearchFilter(boolean initialConnection) {
        this.initialConnection = initialConnection;
    }

    public boolean isInitialConnection() {
        return initialConnection;
    }

    public boolean isFineIfFoundOnlyMaster() {
        return fineIfFoundOnlyMaster;
    }

    public boolean isFineIfFoundOnlySlave() {
        return fineIfFoundOnlySlave;
    }

    public boolean isFailoverLoop() {
        return failoverLoop;
    }

    public void setFailoverLoop(boolean failoverLoop) {
        this.failoverLoop = failoverLoop;
    }

    @Override
    public String toString() {
        return "SearchFilter{" + ", fineIfFoundOnlyMaster=" + fineIfFoundOnlyMaster
               + ", fineIfFoundOnlySlave=" + fineIfFoundOnlySlave + ", initialConnection="
               + initialConnection + ", failoverLoop=" + failoverLoop + "}";
    }
}
