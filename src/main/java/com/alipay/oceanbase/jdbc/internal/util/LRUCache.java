package com.alipay.oceanbase.jdbc.internal.util;

import java.util.LinkedHashMap;
import java.util.Map;

public class LRUCache extends LinkedHashMap<Object, Object> {
    private static final long serialVersionUID = 1L;
    protected int             maxElements;

    public LRUCache(int maxSize) {
        super(maxSize, 0.75F, true);
        this.maxElements = maxSize;
    }

    /*
     * (non-Javadoc)
     *
     * @see java.util.LinkedHashMap#removeEldestEntry(java.util.Map.Entry)
     */
    @Override
    protected boolean removeEldestEntry(Map.Entry<Object, Object> eldest) {
        return (size() > this.maxElements);
    }
}