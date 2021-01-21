package com.alipay.oceanbase.jdbc.internal.util;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

public class StringCacheUtil {
    /**
     * SQL String objects are generally passed directly to the underlying data source , since the implementation of the underlying data source will enable the PsCache function .
     * PsCache is basically implemented by LRU cache . As time gets longer , SQL String objects will migrate to the Old area of memory , which will cause the Old area to become larger and larger .
     * In order to alleviate this problem , the String cache of SQL can be stored here . The temporarily generated SQL String object quickly executes YGC (such as the new String after replacing the table name) .
     * <p>
     * Reference link ：http://gitlab.alipay-inc.com/alipay-data/zdal5/issues/29
     */
    private static int                  DefaultSQLStringCacheSize = 1024 * 50;

    public static Cache<String, String> sqlStringCache            = CacheBuilder
                                                                      .newBuilder()
                                                                      .maximumSize(
                                                                          DefaultSQLStringCacheSize)
                                                                      .build();
}