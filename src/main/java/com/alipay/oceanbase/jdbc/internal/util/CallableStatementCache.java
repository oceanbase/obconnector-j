package com.alipay.oceanbase.jdbc.internal.util;

import java.sql.CallableStatement;
import java.util.LinkedHashMap;
import java.util.Map;
import com.alipay.oceanbase.jdbc.internal.util.dao.CallableStatementCacheKey;

public class CallableStatementCache extends
                                   LinkedHashMap<CallableStatementCacheKey, CallableStatement> {

    private final int maxSize;

    private CallableStatementCache(int size) {
        super(size, .75f, true);
        maxSize = size;
    }

    public static CallableStatementCache newInstance(int size) {
        return new CallableStatementCache(size);
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<CallableStatementCacheKey, CallableStatement> eldest) {
        return this.size() > maxSize;
    }
}
