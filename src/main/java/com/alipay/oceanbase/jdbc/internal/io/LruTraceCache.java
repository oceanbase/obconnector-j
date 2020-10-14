package com.alipay.oceanbase.jdbc.internal.io;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import com.alipay.oceanbase.jdbc.internal.util.Utils;

public class LruTraceCache extends LinkedHashMap<String, TraceObject> {

    private AtomicLong increment = new AtomicLong();

    public LruTraceCache() {
        super(16, 1.0f, false);
    }

    /**
     * Add value to map.
     *
     * @param value value to add
     * @return added value
     */
    public TraceObject put(TraceObject value) {
        String key = increment.incrementAndGet() + "- "
                     + DateTimeFormatter.ISO_INSTANT.format(Instant.now());
        return put(key, value);
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<String, TraceObject> eldest) {
        return size() > 10;
    }

    /**
     * Value of trace cache in a readable format.
     *
     * @return trace cache value
     */
    public synchronized String printStack() {
        StringBuilder sb = new StringBuilder();
        boolean finished = false;
        while (!finished) {
            try {
                Map.Entry<String, TraceObject>[] arr = entrySet().toArray(
                    (Map.Entry<String, TraceObject>[]) new Map.Entry[0]);
                for (Map.Entry<String, TraceObject> entry : arr) {
                    TraceObject traceObj = entry.getValue();
                    if (traceObj.getBuf() != null) {
                        String key = entry.getKey();
                        String indicator = "";

                        switch (traceObj.getIndicatorFlag()) {
                            case TraceObject.COMPRESSED_PROTOCOL_NOT_COMPRESSED_PACKET:
                                indicator = " (compressed protocol - packet not compressed)";
                                break;
                            case TraceObject.COMPRESSED_PROTOCOL_COMPRESSED_PACKET:
                                indicator = " (compressed protocol - packet compressed)";
                                break;
                            default:
                                break;
                        }
                        sb.append("\nthread:").append(traceObj.getThreadId());
                        if (traceObj.isSend()) {
                            sb.append(" send at -exchange:");
                        } else {
                            sb.append(" read at -exchange:");
                        }

                        sb.append(key).append(indicator).append(Utils.hexdump(traceObj.getBuf()));
                    }
                }
                finished = true;
            } catch (ConcurrentModificationException cc) {
                // eat
            }
        }

        this.clear();
        return sb.toString();
    }

    /** Permit to clear array's of array, to help garbage. */
    public synchronized void clearMemory() {
        try {
            Collection<TraceObject> traceObjects = values();
            for (TraceObject traceObject : traceObjects) {
                traceObject.remove();
            }
        } catch (ConcurrentModificationException c) {
            // eat
        }
        this.clear();
    }
}
