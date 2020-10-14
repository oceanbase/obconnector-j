package com.alipay.oceanbase.jdbc.internal.util;

import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import com.alipay.oceanbase.jdbc.internal.protocol.Protocol;
import com.alipay.oceanbase.jdbc.internal.util.dao.ServerPrepareResult;

public final class ServerPrepareStatementCache extends LinkedHashMap<String, ServerPrepareResult> {

    private final int      maxSize;
    private final Protocol protocol;

    private ServerPrepareStatementCache(int size, Protocol protocol) {
        super(size, .75f, true);
        this.maxSize = size;
        this.protocol = protocol;
    }

    public static ServerPrepareStatementCache newInstance(int size, Protocol protocol) {
        return new ServerPrepareStatementCache(size, protocol);
    }

    /**
     * Remove eldestEntry.
     *
     * @param eldest eldest entry
     * @return true if eldest entry must be removed
     */
    @Override
    public boolean removeEldestEntry(Map.Entry eldest) {
        boolean mustBeRemoved = this.size() > maxSize;

        if (mustBeRemoved) {
            ServerPrepareResult serverPrepareResult = ((ServerPrepareResult) eldest.getValue());
            serverPrepareResult.setRemoveFromCache();
            if (serverPrepareResult.canBeDeallocate()) {
                try {
                    protocol.forceReleasePrepareStatement(serverPrepareResult.getStatementId());
                } catch (SQLException e) {
                    // eat exception
                }
            }
        }
        return mustBeRemoved;
    }

    /**
     * Associates the specified value with the specified key in this map. If the map previously
     * contained a mapping for the key, the existing cached prepared result shared counter will be
     * incremented.
     *
     * @param key key
     * @param result new prepare result.
     * @return the previous value associated with key if not been deallocate, or null if there was no
     *     mapping for key.
     */
    public synchronized ServerPrepareResult put(String key, ServerPrepareResult result) {
        ServerPrepareResult cachedServerPrepareResult = super.get(key);
        // if there is already some cached data (and not been deallocate), return existing cached data
        if (cachedServerPrepareResult != null && cachedServerPrepareResult.incrementShareCounter()) {
            return cachedServerPrepareResult;
        }
        // if no cache data, or been deallocate, put new result in cache
        result.setAddToCache();
        super.put(key, result);
        return null;
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder("ServerPrepareStatementCache.map[");
        for (Map.Entry<String, ServerPrepareResult> entry : this.entrySet()) {
            stringBuilder.append("\n").append(entry.getKey()).append("-")
                .append(entry.getValue().getShareCounter());
        }
        stringBuilder.append("]");
        return stringBuilder.toString();
    }
}
