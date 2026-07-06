package com.oceanbase.jdbc.internal.util;

import com.oceanbase.jdbc.internal.com.read.resultset.ColumnDefinition;

import java.util.HashMap;


/**
 * Cache for ResultSet metadata of PreparedStatements.
 * This cache is bound to a Connection's lifecycle and stores column definitions
 * to avoid redundant network transmission.
 *
 */
public class ResultSetMetaDataCache extends HashMap<Integer, ColumnDefinition[]> {

    /**
     * @param key statementId
     *
     */
    @Override
    public synchronized ColumnDefinition[] get(Object key) {
        return super.get(key);
    }

    /**
     * Store metadata in cache
     * 
     * @param key statementId
     * @param value column definitions
     * @return previous value
     */
    @Override
    public synchronized ColumnDefinition[] put(Integer key, ColumnDefinition[] value) {
        return super.put(key, value);
    }

    /**
     * Clear the cache and reset statistics
     */
    @Override
    public synchronized void clear() {
        super.clear();
    }

}
