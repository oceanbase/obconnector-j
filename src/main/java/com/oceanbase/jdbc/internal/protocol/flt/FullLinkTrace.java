/**
 *  OceanBase Client for Java
 *
 *  Copyright (c) 2012-2014 Monty Program Ab.
 *  Copyright (c) 2015-2020 MariaDB Corporation Ab.
 *  Copyright (c) 2021 OceanBase.
 *
 *  This library is free software; you can redistribute it and/or modify it under
 *  the terms of the GNU Lesser General Public License as published by the Free
 *  Software Foundation; either version 2.1 of the License, or (at your option)
 *  any later version.
 *
 *  This library is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 *  FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 *  for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public License along
 *  with this library; if not, write to Monty Program Ab info@montyprogram.com.
 *
 *  This particular MariaDB Client for Java file is work
 *  derived from a Drizzle-JDBC. Drizzle-JDBC file which is covered by subject to
 *  the following copyright and notice provisions:
 *
 *  Copyright (c) 2009-2011, Marcus Eriksson
 *
 *  Redistribution and use in source and binary forms, with or without modification,
 *  are permitted provided that the following conditions are met:
 *  Redistributions of source code must retain the above copyright notice, this list
 *  of conditions and the following disclaimer.
 *
 *  Redistributions in binary form must reproduce the above copyright notice, this
 *  list of conditions and the following disclaimer in the documentation and/or
 *  other materials provided with the distribution.
 *
 *  Neither the name of the driver nor the names of its contributors may not be
 *  used to endorse or promote products derived from this software without specific
 *  prior written permission.
 *
 *  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS  AND CONTRIBUTORS "AS IS"
 *  AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 *  WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 *  IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 *  INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 *  NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 *  PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 *  WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 *  ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
 *  OF SUCH DAMAGE.
 */
package com.oceanbase.jdbc.internal.protocol.flt;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import com.oceanbase.jdbc.internal.com.read.Buffer;

/***
 * the key_value information of extra_info in OB2.0 protocol will be expressed as follows:
 *
 * ...
 * | ExtraInfoKeyType (2 bytes) | ExtraInfoValueLength (4 bytes)
 * | ExtraInfoValueStream (ExtraInfoValueLength bytes) |
 * ...
 */
public class FullLinkTrace {

    private static final int  TYPE_LENGTH              = 2;
    private static final int  LEN_LENGTH               = 4;
    private static final int  TYPE_LEN_LENGTH          = TYPE_LENGTH + LEN_LENGTH;
    private static final int  FLT_SIZE                 = 4;
    private static final int  DBL_SIZE                 = 8;
    private static final int  MAX_DRIVER_LOG_SIZE      = 1024;
    private static final int  MAX_SHOW_TRACE_SPAN_SIZE = 1 << 31 - 1;

    /**
     * 0~999      belong to ob client
     * 1000~1999  belong to ob proxy
     * 2000~65535 belong to ob full link
     */
    private enum FullLinkTraceExtraInfoId {
        FLT_DRIVER_SPAN(1), // MYSQL_TYPE_VAR_STRING
        // here to add driver's id
        FLT_DRIVER_END(1000), // MYSQL_TYPE_NOT_DEFINED
        // here to add proxy's id
        FLT_PROXY_END(2000), // MYSQL_TYPE_NOT_DEFINED
        // APP_INFO
        FLT_CLIENT_IDENTIFIER(2001), // MYSQL_TYPE_VAR_STRING
        FLT_MODULE(2002), // MYSQL_TYPE_VAR_STRING
        FLT_ACTION(2003), // MYSQL_TYPE_VAR_STRING
        FLT_CLIENT_INFO(2004), // MYSQL_TYPE_VAR_STRING
        FLT_APPINFO_TYPE(2005), // MYSQL_TYPE_TINY
        // QUERY_INFO
        FLT_QUERY_START_TIMESTAMP(2010), // MYSQL_TYPE_LONGLONG
        FLT_QUERY_END_TIMESTAMP(2011), // MYSQL_TYPE_LONGLONG
        // CONTROL_INFO
        FLT_LEVEL(2020), // MYSQL_TYPE_TINY
        FLT_SAMPLE_PERCENTAGE(2021), // MYSQL_TYPE_DOUBLE
        FLT_RECORD_POLICY(2022), // MYSQL_TYPE_TINY
        FLT_PRINT_SAMPLE_PCT(2023), // MYSQL_TYPE_DOUBLE
        FLT_SLOW_QUERY_THRES(2024), // MYSQL_TYPE_LONGLONG
        FLT_SHOW_TRACE_ENABLE(2025), // MYSQL_TYPE_TINY
        // SPAN_INFO
        FLT_TRACE_ENABLE(2030), // MYSQL_TYPE_TINY
        FLT_FORCE_PRINT(2031), // MYSQL_TYPE_TINY
        FLT_TRACE_ID(2032), // MYSQL_TYPE_VAR_STRING
        FLT_REF_TYPE(2033), // MYSQL_TYPE_TINY
        FLT_SPAN_ID(2034), // MYSQL_TYPE_VAR_STRING
        // SHOW_TRACE
        FLT_DRIVER_SHOW_TRACE_SPAN(2050), // MYSQL_TYPE_VAR_STRING
        FLT_PROXY_SHOW_TRACE_SPAN(2051), // MYSQL_TYPE_VAR_STRING
        FLT_EXTRA_INFO_ID_END(65535);

        private final short value;

        FullLinkTraceExtraInfoId(int v) {
            value = (short) v;
        }

        private short getValue() {
            return value;
        }

    }

    enum FullLinkTraceExtraInfoType {
        FLT_DRV_LOG(1),
        FLT_EXTRA_INFO_DRIVE_END(1000),
        FLT_APP_INFO(2001),
        FLT_QUERY_INFO(2002),
        FLT_CONTROL_INFO(2003),
        FLT_SPAN_INFO(2004),
        FLT_SHOW_TRACE_SPAN(2005),
        FLT_EXTRA_INFO_TYPE_END(65535);

        private final short value;

        FullLinkTraceExtraInfoType(int v) {
            value = (short) v;
        }

        short getValue() {
            return value;
        }
    }

    private enum RecordPolicy {
        RP_ALL,
        RP_ONLY_SLOW_QUERY,
        RP_SAMPLE_AND_SLOW_QUERY,
        MAX_RECORD_POLICY;

        private static RecordPolicy valueOf(byte v) throws UnsupportedEncodingException {
            switch (v) {
                case 1:
                    return RP_ALL;
                case 2:
                    return RP_ONLY_SLOW_QUERY;
                case 3:
                    return RP_SAMPLE_AND_SLOW_QUERY;
                case 4:
                    return MAX_RECORD_POLICY;
                default:
                    throw new UnsupportedEncodingException("Unknown RecordPolicy: " + v);
            }
        }
    }

    public enum TagKey {
        COMMAND_NAME("command_name"),
        CLIENT_HOST("client_host");

        private final String keyStr;

        TagKey(String str) {
            keyStr = str;
        }

        public String getString() {
            return keyStr;
        }
    }

    abstract class FullLinkTraceInfoBase {
        FullLinkTraceExtraInfoType type;

        protected abstract int getSerializeSize();

        protected abstract void serialize(Buffer buf) throws IOException;

        protected abstract void deserialize(Buffer buf, int infoEndPos) throws IOException;
    }

    private class DriveLog extends FullLinkTraceInfoBase {
        private String log;

        private DriveLog() {
            type = FullLinkTraceExtraInfoType.FLT_DRV_LOG;
        }

        @Override
        protected int getSerializeSize() {
            int localize = 0;

            if (log != null && log.length() != 0) {
                localize += TYPE_LEN_LENGTH;
                localize += getStoreStringSize(log.length());
            }

            return localize;
        }

        @Override
        protected void serialize(Buffer buf) throws IOException {
            int originalPos = buf.getPosition();

            // reserve for type and len
            buf.checkRemainder(TYPE_LEN_LENGTH);
            buf.setPosition(originalPos + TYPE_LEN_LENGTH);

            storeString(buf, log, log.length(), FullLinkTraceExtraInfoId.FLT_DRIVER_SPAN.getValue());

            // fill type and len in the head
            int totalLen = buf.getPosition() - originalPos - TYPE_LEN_LENGTH;
            buf.setPosition(originalPos);
            storeTypeAndLen(buf, type.getValue(), totalLen);
            buf.setPosition(buf.getPosition() + totalLen);
        }

        @Override
        protected void deserialize(Buffer buf, final int infoEndPos) throws IOException {
            while (buf.getPosition() < infoEndPos) {
                short extraId = resolveType(buf);
                int valLen = resolveLength(buf);

                if (extraId == FullLinkTraceExtraInfoId.FLT_DRIVER_SPAN.value) {
                    log = getString(buf, valLen);
                } else {
                    buf.skipBytes(valLen);
                }
            }
        }

        private void reset() {
            log = null;
        }
    }

    public class ControlInfo extends FullLinkTraceInfoBase {
        byte         level;
        double       samplePercentage;
        RecordPolicy recordPolicy;
        double       printSamplePercentage;
        long         slowQueryThresholdMs;   // receive in microseconds
        boolean      showTraceEnabled;

        private ControlInfo() {
            type = FullLinkTraceExtraInfoType.FLT_CONTROL_INFO;
        }

        protected int getSerializeSize() {
            int localize = 0;

            localize += TYPE_LEN_LENGTH;
            localize += getStoreInt1Size();   // level
            localize += getStoreDoubleSize(); // samplePercentage
            localize += getStoreInt1Size();   // recordPolicy
            localize += getStoreDoubleSize(); // printSamplePercentage
            localize += getStoreInt8Size();   // slowQueryThresholdMs
            localize += getStoreInt1Size();   // showTraceEnabled

            return localize;
        }

        protected void serialize(Buffer buf) throws IOException {
            int originalPos = buf.getPosition();

            // reserve for type and len
            buf.checkRemainder(TYPE_LEN_LENGTH);
            buf.setPosition(originalPos + TYPE_LEN_LENGTH);

            storeInt1(buf, level, FullLinkTraceExtraInfoId.FLT_LEVEL.getValue());
            storeDouble(buf, samplePercentage,
                FullLinkTraceExtraInfoId.FLT_SAMPLE_PERCENTAGE.getValue());
            storeInt1(buf, (byte) recordPolicy.ordinal(),
                FullLinkTraceExtraInfoId.FLT_RECORD_POLICY.getValue());
            storeDouble(buf, printSamplePercentage,
                FullLinkTraceExtraInfoId.FLT_PRINT_SAMPLE_PCT.getValue());
            storeInt8(buf, slowQueryThresholdMs,
                FullLinkTraceExtraInfoId.FLT_SLOW_QUERY_THRES.getValue());
            storeInt1(buf, showTraceEnabled ? (byte) 1 : (byte) 0,
                FullLinkTraceExtraInfoId.FLT_SHOW_TRACE_ENABLE.getValue());

            // fill type and len in the head
            int totalLen = buf.getPosition() - originalPos - TYPE_LEN_LENGTH;
            buf.setPosition(originalPos);
            storeTypeAndLen(buf, type.getValue(), totalLen);
            buf.setPosition(buf.getPosition() + totalLen);
        }

        protected void deserialize(Buffer buf, final int infoEndPos) throws IOException {
            while (buf.getPosition() < infoEndPos) {
                short extraId = resolveType(buf);
                int valLen = resolveLength(buf);

                if (extraId == FullLinkTraceExtraInfoId.FLT_LEVEL.value) {
                    level = getInt1(buf, valLen);
                } else if (extraId == FullLinkTraceExtraInfoId.FLT_SAMPLE_PERCENTAGE.value) {
                    samplePercentage = getDouble(buf, valLen);
                } else if (extraId == FullLinkTraceExtraInfoId.FLT_RECORD_POLICY.value) {
                    byte v = getInt1(buf, valLen);
                    recordPolicy = RecordPolicy.valueOf(v);
                } else if (extraId == FullLinkTraceExtraInfoId.FLT_PRINT_SAMPLE_PCT.value) {
                    printSamplePercentage = getDouble(buf, valLen);
                } else if (extraId == FullLinkTraceExtraInfoId.FLT_SLOW_QUERY_THRES.value) {
                    slowQueryThresholdMs = getInt8(buf, valLen);
                } else if (extraId == FullLinkTraceExtraInfoId.FLT_SHOW_TRACE_ENABLE.value) {
                    showTraceEnabled = getInt1(buf, valLen) == 1;
                } else {
                    buf.skipBytes(valLen);
                }
            }
        }

        public byte getLevel() {
            return level;
        }

        public double getSamplePercentage() {
            return samplePercentage;
        }

        public byte getRecordPolicy() {
            return (byte) (recordPolicy.ordinal() + 1);
        }

        public double getPrintSamplePercentage() {
            return printSamplePercentage;
        }

        public long getSlowQueryThreshold() {
            return slowQueryThresholdMs;
        }

        private boolean isValid() {
            return level > 0 && recordPolicy != RecordPolicy.MAX_RECORD_POLICY
                   && samplePercentage >= 0 && samplePercentage <= 1 && printSamplePercentage >= 0
                   && printSamplePercentage <= 1;
        }
    }

    public class AppInfo extends FullLinkTraceInfoBase {
        private String  clientIdentifier;
        private String  module;
        private String  action;
        private String  clientInfo;
        private boolean isSet;

        private AppInfo() {
            type = FullLinkTraceExtraInfoType.FLT_APP_INFO;
        }

        protected int getSerializeSize() {
            int localize = 0;

            if (isSet) {
                if (null != clientIdentifier) {
                    localize += getStoreStringSize(clientIdentifier.length());
                }
                if (null != module) {
                    localize += getStoreStringSize(module.length());
                }
                if (null != action) {
                    localize += getStoreStringSize(action.length());
                }
                if (null != clientInfo) {
                    localize += getStoreStringSize(clientInfo.length());
                }
                if (0 != localize) {
                    localize += TYPE_LEN_LENGTH;
                }
            }
            return localize;
        }

        protected void serialize(Buffer buf) throws IOException {
            int originalPos = buf.getPosition();

            // reserve for type and len
            buf.checkRemainder(TYPE_LEN_LENGTH);
            buf.setPosition(originalPos + TYPE_LEN_LENGTH);

            if (null != clientIdentifier) {
                storeString(buf, clientIdentifier, clientIdentifier.length(),
                    FullLinkTraceExtraInfoId.FLT_CLIENT_IDENTIFIER.getValue());
            }
            if (null != module) {
                storeString(buf, module, module.length(),
                    FullLinkTraceExtraInfoId.FLT_MODULE.getValue());
            }
            if (null != action) {
                storeString(buf, action, action.length(),
                    FullLinkTraceExtraInfoId.FLT_ACTION.getValue());
            }
            if (null != clientInfo) {
                storeString(buf, clientInfo, clientInfo.length(),
                    FullLinkTraceExtraInfoId.FLT_CLIENT_INFO.getValue());
            }

            // fill type and len in the head
            int totalLen = buf.getPosition() - originalPos - TYPE_LEN_LENGTH;
            buf.setPosition(originalPos);
            storeTypeAndLen(buf, type.getValue(), totalLen);
            buf.setPosition(buf.getPosition() + totalLen);
        }

        protected void deserialize(Buffer buf, final int infoEndPos) throws IOException {
            while (buf.getPosition() < infoEndPos) {
                short extraId = resolveType(buf);
                int valLen = resolveLength(buf);

                if (extraId == FullLinkTraceExtraInfoId.FLT_CLIENT_IDENTIFIER.value) {
                    clientIdentifier = getString(buf, valLen);
                } else if (extraId == FullLinkTraceExtraInfoId.FLT_MODULE.value) {
                    module = getString(buf, valLen);
                } else if (extraId == FullLinkTraceExtraInfoId.FLT_ACTION.value) {
                    action = getString(buf, valLen);
                } else if (extraId == FullLinkTraceExtraInfoId.FLT_CLIENT_INFO.value) {
                    clientInfo = getString(buf, valLen);
                } else {
                    buf.skipBytes(valLen);
                }
            }
        }

        private void reset() {
            isSet = false;
            clientIdentifier = null;
            module = null;
            action = null;
            clientInfo = null;
        }

        public void setModule(String module, String action) {
            isSet = true;
            this.module = module;
            this.action = action;
        }

        public String getModule() {
            return module;
        }

        public void setAction(String action) {
            isSet = true;
            this.action = action;
        }

        public String getAction() {
            return action;
        }

        public void setClientInfo(String clientInfo) {
            isSet = true;
            this.clientInfo = clientInfo;
        }

        public String getClientInfo() {
            return clientInfo;
        }

        public void setClientIdentifier(String clientIdentifier) {
            isSet = true;
            this.clientIdentifier = clientIdentifier;
        }

        public String getClientIdentifier() {
            return clientIdentifier;
        }
    }

    class QueryInfo extends FullLinkTraceInfoBase {
        long queryStartTimestamp;
        long queryEndTimestamp;

        private QueryInfo() {
            type = FullLinkTraceExtraInfoType.FLT_QUERY_INFO;
        }

        protected int getSerializeSize() {
            int localize = 0;

            localize += TYPE_LEN_LENGTH;
            localize += getStoreInt8Size();
            localize += getStoreInt8Size();

            return localize;
        }

        protected void serialize(Buffer buf) throws IOException {
            int originalPos = buf.getPosition();

            // reserve for type and len
            buf.checkRemainder(TYPE_LEN_LENGTH);
            buf.setPosition(originalPos + TYPE_LEN_LENGTH);

            storeInt8(buf, queryStartTimestamp,
                FullLinkTraceExtraInfoId.FLT_QUERY_START_TIMESTAMP.getValue());
            storeInt8(buf, queryEndTimestamp,
                FullLinkTraceExtraInfoId.FLT_QUERY_END_TIMESTAMP.getValue());

            int totalLen = buf.getPosition() - originalPos - TYPE_LEN_LENGTH;
            buf.setPosition(originalPos);
            storeTypeAndLen(buf, type.getValue(), totalLen);
            buf.setPosition(buf.getPosition() + totalLen);
        }

        protected void deserialize(Buffer buf, final int infoEndPos) throws IOException {
            while (buf.getPosition() < infoEndPos) {
                short extraId = resolveType(buf);
                int valLen = resolveLength(buf);

                if (extraId == FullLinkTraceExtraInfoId.FLT_QUERY_START_TIMESTAMP.value) {
                    queryStartTimestamp = getInt8(buf, valLen);
                } else if (extraId == FullLinkTraceExtraInfoId.FLT_QUERY_END_TIMESTAMP.value) {
                    queryEndTimestamp = getInt8(buf, valLen);
                } else {
                    buf.skipBytes(valLen);
                }
            }
        }

    }

    class SpanInfo extends FullLinkTraceInfoBase {
        private boolean traceEnable;
        private boolean forcePrint;
        private byte    refType;
        private UUID    traceId;
        private UUID    spanId;

        private SpanInfo() {
            type = FullLinkTraceExtraInfoType.FLT_SPAN_INFO;
        }

        protected int getSerializeSize() {
            int localize = 0;

            if (traceEnable) {
                localize += TYPE_LEN_LENGTH;
                localize += getStoreInt1Size();
                localize += getStoreInt1Size();
                localize += getStoreUuidSize();
                //localize += getStoreStringSize(span_info.traceId.toString().length()); // todo: check
                localize += getStoreInt1Size();
                localize += getStoreUuidSize();
                //localize += getStoreStringSize(span_info.spanId.toString().length());
            }

            return localize;
        }

        protected void serialize(Buffer buf) throws IOException {
            if (traceEnable) {
                int originalPos = buf.getPosition();

                // reserve for type and len
                buf.checkRemainder(TYPE_LEN_LENGTH);
                buf.setPosition(originalPos + TYPE_LEN_LENGTH);

                storeInt1(buf, traceEnable ? (byte) 1 : (byte) 0,
                    FullLinkTraceExtraInfoId.FLT_TRACE_ENABLE.getValue());
                storeInt1(buf, forcePrint ? (byte) 1 : (byte) 0,
                    FullLinkTraceExtraInfoId.FLT_FORCE_PRINT.getValue());
                storeUUID(buf, traceId, FullLinkTraceExtraInfoId.FLT_TRACE_ID.getValue());
                storeInt1(buf, refType, FullLinkTraceExtraInfoId.FLT_REF_TYPE.getValue());
                storeUUID(buf, spanId, FullLinkTraceExtraInfoId.FLT_SPAN_ID.getValue());

                // fill type and len in the head
                int totalLen = buf.getPosition() - originalPos - TYPE_LEN_LENGTH;
                buf.setPosition(originalPos);
                storeTypeAndLen(buf, type.getValue(), totalLen);
                buf.setPosition(buf.getPosition() + totalLen);
            }
        }

        protected void deserialize(Buffer buf, final int infoEndPos) throws IOException {
            while (buf.getPosition() < infoEndPos) {
                short extraId = resolveType(buf);
                int valLen = resolveLength(buf);

                if (extraId == FullLinkTraceExtraInfoId.FLT_TRACE_ENABLE.value) {
                    traceEnable = getInt1(buf, valLen) == 1;
                } else if (extraId == FullLinkTraceExtraInfoId.FLT_FORCE_PRINT.value) {
                    forcePrint = getInt1(buf, valLen) == 1;
                } else if (extraId == FullLinkTraceExtraInfoId.FLT_TRACE_ID.value) {
                    traceId = getUUID(buf, valLen);
                } else if (extraId == FullLinkTraceExtraInfoId.FLT_REF_TYPE.value) {
                    refType = getInt1(buf, valLen);
                } else if (extraId == FullLinkTraceExtraInfoId.FLT_SPAN_ID.value) {
                    spanId = getUUID(buf, valLen);
                } else {
                    buf.skipBytes(valLen);
                }
            }
        }

        private void reset() {
            traceEnable = false;
            forcePrint = false;
            refType = 0;
            traceId = null;
            spanId = null;
        }
    }

    private class ShowTraceSpan extends FullLinkTraceInfoBase {
        private String trace;

        private ShowTraceSpan() {
            type = FullLinkTraceExtraInfoType.FLT_SHOW_TRACE_SPAN;
        }

        @Override
        protected int getSerializeSize() {
            int localize = 0;

            if (trace != null && trace.length() != 0) {
                localize += TYPE_LEN_LENGTH;
                localize += getStoreStringSize(trace.length());
            }

            return localize;
        }

        @Override
        protected void serialize(Buffer buf) throws IOException {
            int originalPos = buf.getPosition();

            // reserve for type and len
            buf.checkRemainder(TYPE_LEN_LENGTH);
            buf.setPosition(originalPos + TYPE_LEN_LENGTH);

            storeString(buf, trace, trace.length(),
                FullLinkTraceExtraInfoId.FLT_DRIVER_SHOW_TRACE_SPAN.getValue());

            // fill type and len in the head
            int totalLen = buf.getPosition() - originalPos - TYPE_LEN_LENGTH;
            buf.setPosition(originalPos);
            storeTypeAndLen(buf, type.getValue(), totalLen);
            buf.setPosition(buf.getPosition() + totalLen);
        }

        @Override
        protected void deserialize(Buffer buf, final int infoEndPos) throws IOException {
            while (buf.getPosition() < infoEndPos) {
                short extraId = resolveType(buf);
                int valLen = resolveLength(buf);

                if (extraId == FullLinkTraceExtraInfoId.FLT_DRIVER_SHOW_TRACE_SPAN.value) {
                    trace = getString(buf, valLen);
                } else {
                    buf.skipBytes(valLen);
                }
            }
        }

        private void reset() {
            trace = null;
        }

    }

    public class TraceInfo {
        private DriveLog   driverLog     = new DriveLog();
        public ControlInfo controlInfo   = new ControlInfo();
        public AppInfo     appInfo       = new AppInfo();
        QueryInfo          queryInfo     = new QueryInfo();
        SpanInfo           spanInfo      = new SpanInfo();
        ShowTraceSpan      showTraceSpan = new ShowTraceSpan();
        private boolean    useNewExtraInfo;
        // use ObObj
        private ObObj      key;
        private ObObj      value;
        // use ByteStream(NewExtraInfo)
        private byte[]     valueData;

        private UUID       traceId;
        private UUID       spanId;

        private TraceInfo(boolean useNewExtraInfo) {
            this.useNewExtraInfo = useNewExtraInfo;

            controlInfo.recordPolicy = RecordPolicy.RP_ONLY_SLOW_QUERY;

            if (!this.useNewExtraInfo) {
                key = new ObObj();
                value = new ObObj();
                String extraKey = OceanBaseProtocolV20.ExtraInfoKey.FULL_TRC.name().toLowerCase(
                    Locale.ROOT);
                key.setVarchar(extraKey.getBytes(), extraKey.length());
            }
        }
    }

    private class TagContext {
        private int    tagType;
        private String tagKey;
        private String tagValue;

        private TagContext(int tagType, String tagKey, String tagValue) {
            this.tagType = tagType;
            this.tagKey = tagKey;
            this.tagValue = tagValue;
        }

        private String getJsonString() {
            return "{\"" + tagKey + "\":\"" + tagValue + "\"}";
        }
    }

    private class SpanContext {
        private int                    spanType;
        private UUID                   spanId;
        private UUID                   sourceSpanId;
        private boolean                isFollow;
        private long                   startTimestamp;               // get in milliseconds, send in microseconds
        private long                   startNano;                    // get in nanoseconds
        private long                   endTimestamp;
        private long                   endNano;
        private LinkedList<TagContext> tagList;
        private String                 moduleName = "oceanbase_jdbc";

        public SpanContext(int spanType, boolean isFollow) {
            this.spanType = spanType;
            this.spanId = UUID.randomUUID();
            this.sourceSpanId = new UUID(0, 0);
            this.isFollow = isFollow;
            this.startNano = System.nanoTime();
            this.startTimestamp = System.currentTimeMillis();
            this.endNano = 0;
            this.endTimestamp = 0;
            this.tagList = new LinkedList<TagContext>();
        }

        private String getJsonString() {
            String spanStr = "\"name\":\"" + moduleName + "\",\"id\":\"" + toStringUUID(spanId)
                             + "\",\"parent_id\":\"" + toStringUUID(sourceSpanId)
                             + "\",\"start_ts\":" + startTimestamp * 1000 + ",\"end_ts\":"
                             + endTimestamp * 1000 + ",\"is_follow\":"
                             + (isFollow ? "true" : "false");

            StringBuilder sb = new StringBuilder();
            sb.append(spanStr);

            if (endTimestamp == 0 && ! tagList.isEmpty()) {
                sb.append(",\"tags\":[");

                boolean firstOne = true;
                for (TagContext tag : tagList) {
                    if (!firstOne) {
                        sb.append(",");
                    } else {
                        firstOne = false;
                    }
                    sb.append(tag.getJsonString());
                }

                sb.append(']');
            }

            return sb.toString();
        }
    }

    private boolean                              traceEnable;
    private boolean                              forcePrint;
    private boolean                              autoFlush;
    private boolean                              showTraceEnable;

    public TraceInfo                             traceInfo;
    private UUID                                 traceId;
    private UUID                                 rootSpanId;
    private ConcurrentHashMap<UUID, SpanContext> activeSpanMap = new ConcurrentHashMap();
    private SpanContext                          lastActiveSpan;
    private byte                                 level;

    private byte[]                               logBuf        = new byte[MAX_DRIVER_LOG_SIZE];
    private int                                  logBufOffset;
    private byte[]                               showTraceBuf  = new byte[MAX_DRIVER_LOG_SIZE];
    private int                                  showTraceBufOffset;

    // todo: consider whether threads share one connection
    public FullLinkTrace(boolean useNewExtraInfo) {
        traceInfo = new TraceInfo(useNewExtraInfo);
        traceId = new UUID(0, 0);
        rootSpanId = new UUID(0, 0);
    }

    public boolean isShowTraceEnabled() {
        return this.traceInfo.controlInfo.showTraceEnabled;
    }

    public void beginTrace() {
        traceEnable = false;
        showTraceEnable = false;
        forcePrint = false;

        if (isShowTraceEnabled()) {
            showTraceEnable = true;
            traceEnable = true;
        } else if (traceInfo.controlInfo.isValid()) {
            traceEnable = getPercentage() < traceInfo.controlInfo.samplePercentage;
        }

        if (traceEnable) {
            if (isShowTraceEnabled()) {
                forcePrint = true;
            } else {
                switch (traceInfo.controlInfo.recordPolicy) {
                    case RP_ALL:
                        forcePrint = true;
                        break;
                    case RP_SAMPLE_AND_SLOW_QUERY:
                        forcePrint = getPercentage() < traceInfo.controlInfo.printSamplePercentage;
                        break;
                    default: // RP_ONLY_SLOW_QUERY, MAX_RECORD_POLICY
                        forcePrint = false;
                        break;
                }
            }

            traceId = UUID.randomUUID();
            level = traceInfo.controlInfo.level;
            autoFlush = forcePrint;
        } else {
            traceId = new UUID(0, 0);
            level = 0;
            autoFlush = false;
        }
    }

    public void endTrace() {
        if (isInitedUUID(traceId)) {
            for (SpanContext span : activeSpanMap.values()) {
                if (0 == span.endTimestamp) {
                    span.endNano = System.nanoTime();
                    span.endTimestamp = System.currentTimeMillis();
                }
            }

            // if find a slow transaction, then flush the whole transaction
            activeSpanMap.clear();
            lastActiveSpan = null;
        }
    }

    private void flushTransaction() throws IOException {
        for (SpanContext span : activeSpanMap.values()) {
            String log = flushQuery(span);

            if (0 != span.endTimestamp) {
                writeShowTraceIntoBuffer(log);
            }
        }
    }

    private String flushQuery(SpanContext span) throws IOException {
        String log = writeLogIntoBuffer(span.getJsonString());
        span.tagList.clear();

        if (0 != span.endTimestamp) {
            activeSpanMap.remove(span.spanId);
        }
        return log;
    }

    // todo: add span type and span level mapper
    public UUID beginSpan(int spanType) {
        return beginChildSpan(spanType);
    }

    private UUID beginChildSpan(int spanType) {
        return beginSpanInternal(spanType, (byte) 1, false);
    }

    private UUID beginFollowSpan(int spanType) {
        return beginSpanInternal(spanType, (byte) 1, true);
    }

    private UUID beginSpanInternal(int spanType, byte level, boolean isFollow) {
        UUID spanId = null;
        if (isInitedUUID(traceId) && level <= this.level) {
            SpanContext newSpan = new SpanContext(spanType, isFollow);

            SpanContext parent = getCurrentSpan();
            if (parent != null) {
                newSpan.sourceSpanId = parent.spanId;
            } else {
                rootSpanId = newSpan.spanId;
            }

            activeSpanMap.put(newSpan.spanId, newSpan);
            lastActiveSpan = newSpan;

            spanId = newSpan.spanId;
        }
        return spanId;
    }

    public void endSpan(final UUID spanId) throws IOException {
        if (!isInitedUUID(traceId) || !isInitedUUID(spanId)) {
            return;
        }

        boolean slowQueryFound = false;
        SpanContext span = activeSpanMap.get(spanId);
        if (span != null) {
            span.endNano = System.nanoTime();
            span.endTimestamp = System.currentTimeMillis();

            if ((traceInfo.controlInfo.recordPolicy == RecordPolicy.RP_ONLY_SLOW_QUERY || traceInfo.controlInfo.recordPolicy == RecordPolicy.RP_SAMPLE_AND_SLOW_QUERY)
                    && traceInfo.controlInfo.slowQueryThresholdMs > 0
                    && ((span.endNano - span.startNano) / 1000 >= traceInfo.controlInfo.slowQueryThresholdMs)) {
                // if a slow query is found, send log to server for print
                slowQueryFound = true;
            }
        }

        if (forcePrint) {
            flushTransaction();
        } else if (slowQueryFound) {
            flushQuery(span);
        }
    }

    private SpanContext getCurrentSpan() {
        if (null == lastActiveSpan || 0 != lastActiveSpan.endTimestamp) {
            lastActiveSpan = null;
            for (SpanContext span : activeSpanMap.values()) {
                // jump end span
                if (0 == span.endTimestamp) {
                    lastActiveSpan = span;
                    break;
                }
            }
        }
        return lastActiveSpan;
    }

    public void setSpanTag(int tagType, String tagKey, String tagValue) {
        SpanContext span = getCurrentSpan();
        if (span != null && tagKey != null) {
            TagContext tag = new TagContext(tagType, tagKey, tagValue);
            span.tagList.addFirst(tag);
        }
    }

    public void buildRequest(OceanBaseProtocolV20 ob20) throws IOException {
        SpanContext span = getCurrentSpan();
        if (span != null & traceEnable) {
            traceInfo.traceId = traceId;
            traceInfo.spanId = span.spanId;

            traceInfo.spanInfo.traceId = traceId;
            traceInfo.spanInfo.spanId = span.spanId;
            traceInfo.spanInfo.traceEnable = traceEnable;
            traceInfo.spanInfo.forcePrint = forcePrint;

            if (forcePrint) {
                flushTransaction();
            }
        }

        // prepare log
        if (logBufOffset > 0) {
            traceInfo.driverLog.log = new String(logBuf, 0, logBufOffset) + '\0'; // server needs '\0'
        }
        if (showTraceEnable && showTraceBufOffset > 0) {
            traceInfo.showTraceSpan.trace = '[' + new String(showTraceBuf, 0, showTraceBufOffset) + ']' + '\0';
        }

        // get size of info
        int driverLogSize = traceInfo.driverLog.getSerializeSize();
        int appInfoSize = traceInfo.appInfo.getSerializeSize();
        int spanInfoSize = traceInfo.spanInfo.getSerializeSize();
        int showTraceSize = traceInfo.showTraceSpan.getSerializeSize();
        int totalSize = driverLogSize + appInfoSize + spanInfoSize + showTraceSize;

        if (totalSize > 0) {
            traceInfo.valueData = new byte[totalSize + 1];
            Buffer buf = new Buffer(traceInfo.valueData, totalSize);

            // serialize driver log
            if (driverLogSize != 0) {
                traceInfo.driverLog.serialize(buf);
                traceInfo.driverLog.reset();
                if (buf.getPosition() != driverLogSize) {
                    throw new IOException("Unexpected end position in EXTRA INFO: actual = " + buf.getPosition()
                            + ", expected = " + driverLogSize);
                }
            }
            // serialize app info
            if (appInfoSize != 0) {
                traceInfo.appInfo.serialize(buf);
                traceInfo.appInfo.reset();
                if (buf.getPosition() != driverLogSize + appInfoSize) {
                    throw new IOException("Unexpected end position in EXTRA INFO: actual = " + buf.getPosition()
                            + ", expected = " + (driverLogSize + appInfoSize));
                }
            }
            // serialize span info
            if (spanInfoSize != 0) {
                traceInfo.spanInfo.serialize(buf);
                traceInfo.spanInfo.reset();
                if (buf.getPosition() != driverLogSize + appInfoSize + spanInfoSize) {
                    throw new IOException("Unexpected end position in EXTRA INFO: actual = " + buf.getPosition()
                            + ", expected = " + (driverLogSize + appInfoSize + spanInfoSize));
                }
            }
            // serialize show trace span
            if (showTraceSize != 0) {
                traceInfo.showTraceSpan.serialize(buf);
                traceInfo.showTraceSpan.reset();
                if (buf.getPosition() != totalSize) {
                    throw new IOException("Unexpected end position in EXTRA_INFO: actual = " + buf.getPosition()
                            + ", expected = " + totalSize);
                }
            }

            logBufOffset = 0;
            showTraceBufOffset = 0;

            // fill into ob20 protocol packet
            if (!traceInfo.useNewExtraInfo) {
                // use ObObj
                traceInfo.valueData[totalSize] = '\0';
                traceInfo.value.setVarchar(traceInfo.valueData, totalSize);
                ob20.setExtraInfo(traceInfo.key, traceInfo.value);
            } else {
                // use ByteStream(NewExtraInfo)
                ob20.setExtraInfo(OceanBaseProtocolV20.ExtraInfoKey.FULL_TRC,
                        Arrays.copyOfRange(traceInfo.valueData, 0, totalSize));
            }
        }
    }

    private String writeLogIntoBuffer(String spanJson) throws IOException {
        String log = "{\"trace_id\":\"" + toStringUUID(traceId) + "\"," + spanJson + "}";

        if (log.length() > MAX_DRIVER_LOG_SIZE - logBufOffset) {
            throw new IOException("The length of DRIVER_LOG exceeds the buffer: need " + log.length()
                    + " bytes, but remain " + (MAX_DRIVER_LOG_SIZE - logBufOffset) + " bytes.");
        }

        System.arraycopy(log.getBytes(StandardCharsets.UTF_8), 0, logBuf, logBufOffset, log.length());
        logBufOffset += log.length();
        return log;
    }

    private void writeShowTraceIntoBuffer(String log) throws IOException {
        if (showTraceBufOffset > 1) {
            showTraceBuf[showTraceBufOffset++] = ','; // Array out of bounds has the same effect as throwing an exception after active judgment.
        }

        if (log.length() > MAX_DRIVER_LOG_SIZE - showTraceBufOffset) {
            throw new IOException("The length of SHOW_TRACE_SPAN exceeds the buffer: need " + log.length()
                    + " bytes, but remain " + (MAX_DRIVER_LOG_SIZE - showTraceBufOffset) + " bytes.");
        }

        System.arraycopy(log.getBytes(StandardCharsets.UTF_8), 0, showTraceBuf, showTraceBufOffset, log.length());
        showTraceBufOffset += log.length();
    }

    private static String toStringUUID(UUID uuid) {
        long high = uuid.getLeastSignificantBits();
        long low = uuid.getMostSignificantBits();

        byte[] bytes = new byte[16];
        Buffer buf = new Buffer(bytes);
        buf.writeLongFromHighToLow(high);
        buf.writeLongFromHighToLow(low);

        return printHexBinary(bytes);
    }

    private static String printHexBinary(byte[] data) {
        final char[] hexCode = "0123456789abcdef".toCharArray();

        StringBuilder builder = new StringBuilder(data.length * 2 + 4);
        for (int i = 0; i < data.length; i++) {
            if (i == 4 || i == 6 || i == 8 || i == 10) {
                builder.append("-");
            }

            byte b = data[i];
            builder.append(hexCode[(b >> 4) & 0xF]);
            builder.append(hexCode[(b & 0xF)]);
        }
        return builder.toString();
    }

    private static String printHexBinary2(byte[] data) {
        final char[] hexCode = "0123456789abcdef".toCharArray();

        StringBuilder builder = new StringBuilder(data.length * 2);
        for (int i = 0; i < data.length; i++) {
            byte b = data[i];
            builder.append(hexCode[(b >> 4) & 0xF]);
            builder.append(hexCode[(b & 0xF)]);
            builder.append(' ');
        }
        return builder.toString();
    }

    private boolean isInitedUUID(UUID uuid) {
        return uuid != null
               && (uuid.getLeastSignificantBits() != 0 || uuid.getMostSignificantBits() != 0);
    }

    // Returns a double value with a positive sign, greater than or equal to 0.0 and less than 1.0
    private static double getPercentage() {
        return Math.random();
    }

    // get size
    private static int getStoreInt1Size() {
        return TYPE_LEN_LENGTH + 1;
    }

    private static int getStoreInt2Size() {
        return TYPE_LEN_LENGTH + 2;
    }

    private static int getStoreInt3Size() {
        return TYPE_LEN_LENGTH + 3;
    }

    private static int getStoreInt4Size() {
        return TYPE_LEN_LENGTH + 4;
    }

    private static int getStoreInt5Size() {
        return TYPE_LEN_LENGTH + 5;
    }

    private static int getStoreInt6Size() {
        return TYPE_LEN_LENGTH + 6;
    }

    private static int getStoreInt8Size() {
        return TYPE_LEN_LENGTH + 8;
    }

    private static int getStoreDoubleSize() {
        return TYPE_LEN_LENGTH + DBL_SIZE;
    }

    private static int getStoreFloatSize() {
        return TYPE_LEN_LENGTH + FLT_SIZE;
    }

    private static int getStoreUuidSize() {
        return TYPE_LEN_LENGTH + 16;
    }

    private static int getStoreStringSize(final int strLen) {
        return TYPE_LEN_LENGTH + strLen;
    }

    // for encode
    private static void storeInt(Buffer buf, long value, short type, long valueLen)
                                                                                   throws IOException {
        buf.checkRemainder(TYPE_LEN_LENGTH + valueLen);
        buf.writeShort(type);
        buf.writeInt((int) valueLen);

        if (1 == valueLen) {
            buf.writeByte((byte) value);
        } else if (2 == valueLen) {
            buf.writeShort((short) value);
        } else if (3 == valueLen) {
            buf.writeLongInt((int) value);
        } else if (4 == valueLen) {
            buf.writeInt((int) value);
        } else if (5 == valueLen) {
            buf.writeNBytes(value, 5);
        } else if (6 == valueLen) {
            buf.writeNBytes(value, 6);
        } else if (8 == valueLen) {
            buf.writeLongFromLowToHigh(value);
        }
    }

    private static void storeInt1(Buffer buf, byte value, short type) throws IOException {
        storeInt(buf, value, type, 1);
    }

    private static void storeInt2(Buffer buf, short value, short type) throws IOException {
        storeInt(buf, value, type, 2);
    }

    private static void storeInt3(Buffer buf, int value, short type) throws IOException {
        storeInt(buf, value, type, 3);
    }

    private static void storeInt4(Buffer buf, int value, short type) throws IOException {
        storeInt(buf, value, type, 4);
    }

    private static void storeInt5(Buffer buf, long value, short type) throws IOException {
        storeInt(buf, value, type, 5);
    }

    private static void storeInt6(Buffer buf, long value, short type) throws IOException {
        storeInt(buf, value, type, 6);
    }

    private static void storeInt8(Buffer buf, long value, short type) throws IOException {
        storeInt(buf, value, type, 8);
    }

    private static void storeDouble(Buffer buf, double value, short type) throws IOException {
        buf.checkRemainder(TYPE_LEN_LENGTH + DBL_SIZE);
        buf.writeShort(type);
        buf.writeInt(DBL_SIZE);
        buf.writeLongFromLowToHigh(Double.doubleToLongBits(value));
    }

    private static void storeFloat(Buffer buf, float value, short type) throws IOException {
        buf.checkRemainder(TYPE_LEN_LENGTH + FLT_SIZE);
        buf.writeShort(type);
        buf.writeInt(FLT_SIZE);
        buf.writeInt(Float.floatToIntBits(value));
    }

    private static void storeUUID(Buffer buf, UUID uuid, short type) throws IOException {
        buf.checkRemainder(TYPE_LEN_LENGTH + 16);
        buf.writeShort(type);
        buf.writeInt(16);

        buf.writeLongFromHighToLow(uuid.getLeastSignificantBits());
        buf.writeLongFromHighToLow(uuid.getMostSignificantBits());
    }

    private static void storeString(Buffer buf, final String str, final int strLen, short type)
                                                                                               throws IOException {
        buf.checkRemainder(TYPE_LEN_LENGTH + strLen);
        buf.writeShort(type);
        buf.writeInt(strLen);
        buf.writeString(str);
    }

    private static void storeTypeAndLen(Buffer buf, short type, int valLen) throws IOException {
        buf.checkRemainder(TYPE_LEN_LENGTH);
        buf.writeShort(type);
        buf.writeInt(valLen);
    }

    // for decode
    private static byte getInt1(Buffer buf, long valLen) throws IOException {
        if (1 != valLen) {
            throw new IOException("Wrong length to decode: " + valLen);
        }
        buf.checkRemainder(valLen);
        return buf.readByte();
    }

    private static short getInt2(Buffer buf, long valLen) throws IOException {
        if (2 != valLen) {
            throw new IOException("Wrong length to decode: " + valLen);
        }
        buf.checkRemainder(valLen);
        return buf.readShort();
    }

    private static int getInt3(Buffer buf, long valLen) throws IOException {
        if (3 != valLen) {
            throw new IOException("Wrong length to decode: " + valLen);
        }
        buf.checkRemainder(valLen);
        return buf.readInt3Bytes();
    }

    private static int getInt4(Buffer buf, long valLen) throws IOException {
        if (4 != valLen) {
            throw new IOException("Wrong length to decode: " + valLen);
        }
        buf.checkRemainder(valLen);
        return buf.readInt();
    }

    private static long getInt8(Buffer buf, long valLen) throws IOException {
        if (8 != valLen) {
            throw new IOException("Wrong length to decode: " + valLen);
        }
        buf.checkRemainder(valLen);
        return buf.readLong();
    }

    private static double getDouble(Buffer buf, long valLen) throws IOException {
        if (DBL_SIZE != valLen) {
            throw new IOException("Wrong length to decode: " + valLen);
        }
        buf.checkRemainder(valLen);
        return Double.longBitsToDouble(buf.readLong());
    }

    private static float getFloat(Buffer buf, long valLen) throws IOException {
        if (FLT_SIZE != valLen) {
            throw new IOException("Wrong length to decode: " + valLen);
        }
        buf.checkRemainder(valLen);
        return Float.intBitsToFloat(buf.readInt());
    }

    private static UUID getUUID(Buffer buf, long valLen) throws IOException {
        if (16 != valLen) {
            throw new IOException("Wrong length to decode: " + valLen);
        }
        buf.checkRemainder(valLen);
        long high = buf.readLong();
        long low = buf.readLong();
        return new UUID(low, high);
    }

    private static String getString(Buffer buf, long strLen) throws IOException {
        buf.checkRemainder(strLen);
        return buf.readString((int) strLen);
    }

    static short resolveType(Buffer buf) throws IOException {
        buf.checkRemainder(TYPE_LENGTH);
        return buf.readShort();
    }

    static int resolveLength(Buffer buf) throws IOException {
        buf.checkRemainder(LEN_LENGTH);
        return buf.readInt();
    }

}
