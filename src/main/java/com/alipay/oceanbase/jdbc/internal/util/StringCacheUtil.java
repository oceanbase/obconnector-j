package com.alipay.oceanbase.jdbc.internal.util;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

public class StringCacheUtil {
    /**
     * SQL String 对象一般直接传递到底层数据源，由于底层数据源实现都会开启 PsCache 功能
     * 而 PsCache 基本都采用 LRU 缓存实现，随着时间的变长，SQL String 对象会迁移到内存的 Old 区
     * 从而导致 Old 区会越来越大，为了缓解这个问题，在这里存储 SQL 的 String 缓存
     * 可以让临时生成的 SQL String 对象快速 YGC 掉 （如替换表名后的新 String）
     * <p>
     * 完整说明可以参考：http://gitlab.alipay-inc.com/alipay-data/zdal5/issues/29
     */
    private static int                  DefaultSQLStringCacheSize = 1024 * 50;

    public static Cache<String, String> sqlStringCache            = CacheBuilder
                                                                      .newBuilder()
                                                                      .maximumSize(
                                                                          DefaultSQLStringCacheSize)
                                                                      .build();
}