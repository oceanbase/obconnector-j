package com.alipay.oceanbase.jdbc.internal.util.dao;

public class CallableStatementCacheKey {

    private final String database;
    private final String query;

    public CallableStatementCacheKey(String database, String query) {
        this.database = database;
        this.query = query;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }
        CallableStatementCacheKey that = (CallableStatementCacheKey) object;
        return database.equals(that.database) && query.equals(that.query);
    }

    @Override
    public int hashCode() {
        int result = database.hashCode();
        result = 31 * result + query.hashCode();
        return result;
    }
}
