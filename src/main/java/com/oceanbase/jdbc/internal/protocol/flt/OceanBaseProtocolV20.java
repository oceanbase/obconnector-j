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
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import com.oceanbase.jdbc.internal.com.read.Buffer;

/**
    OceanBase 2.0 Protocol Format：

    0       1         2           3          4        Byte
    +-----------------+----------------------+
    |    Magic Num    |       Version        |
    +-----------------+----------------------+
    |            Connection Id               |
    +-----------------------------+----------+
    |         Request Id          |   Seq    |
    +-----------------------------+----------+
    |            PayLoad Length              |
    +----------------------------------------+
    |                Flag                    |
    +-----------------+----------------------+
    |    Reserved     |Header Checksum(CRC16)|
    +-----------------+----------------------+
    |        ... PayLoad  Data ...           |----------+
    +----------------------------------------+          |
    |    Tailer PayLoad Checksum (CRC32)     |          |
    +----------------------------------------+          |
                                                       |
                             +--------------------------+
                             |
                             v
    +-------------------+-------------------+-------------------------------------+
    | Extra Len(4Byte)  |  Extra Info(K/V)  |  Basic Info(Standard MySQL Packet)  |
    +-------------------+-------------------+-------------------------------------+
 */
public class OceanBaseProtocolV20 {

    private static final int UINT_MAX24             = 0x00ffffff;
    private static final int TYPE_LENGTH            = 2;
    private static final int LEN_LENGTH             = 4;

    public static final int  COMPRESS_HEADER_LENGTH = 7;
    public static final int  OB20_HEADER_LENGTH     = 24;
    public static final int  TOTAL_HEADER_LENGTH    = COMPRESS_HEADER_LENGTH + OB20_HEADER_LENGTH;
    public static final int  OB20_MAGIC_NUM         = 0x20AB;
    public static final int  OB20_VERSION           = 20;
    public static final int  OB20_EXTRA_LENGTH      = 4;                                          // length of "extra length"
    public static final int  MYSQL_PACKET_HEADER    = 4;                                          // 3-packet length, 1-packet seq
    public static final int  OB20_TAIL_LENGTH       = 4;

    /******************************* OB Protocol v2.0 Flags (32 bits) *******************************/
    public static final int  OB_EXTRA_INFO_EXIST    = 1;
    public static final int  OB_IS_LAST_PACKET      = 1 << 1;
    public static final int  OB_IS_PROXY_REROUTE    = 1 << 2;
    public static final int  OB_IS_NEW_EXTRA_INFO   = 1 << 3;

    public enum ExtraInfoKey {
        OB20_DRIVER_END(1000),
        OB20_PROXY_END(2000),
        TRACE_INFO(2001),
        SESS_INFO(2002),
        FULL_TRC(2003),
        OB20_SVR_END(2004);

        private final short value;

        ExtraInfoKey(int v) {
            value = (short) v;
        }

        public short getValue() {
            return value;
        }
    }

    public static class Header {
        public int      compressLength;
        public short    compressSeqNo;
        public int      uncompressLength;
        public short    magicNum;
        public short    version;
        public long     connectionId;
        public int      requestId;
        public short    obSeqNo;
        public long     payloadLength;
        public int      flag;
        public short    reserved;
        public int      headerChecksum;
        private boolean useNewExtraInfo;

        public Header(boolean useNewExtraInfo) {
            this.useNewExtraInfo = useNewExtraInfo;
            reset();
        }

        public void reset() {
            compressLength = 0;
            compressSeqNo = 0;
            uncompressLength = 0;
            magicNum = OB20_MAGIC_NUM;
            version = OB20_VERSION;
            connectionId = 0;
            requestId = 0;
            obSeqNo = 0;
            payloadLength = 0;
            flag = 0;
            if (this.useNewExtraInfo) {
                flag |= OB_IS_NEW_EXTRA_INFO;
            }
            reserved = 0;
            headerChecksum = 0;
        }

        @Override
        public String toString() {
            return "compressLength: " + compressLength + ", uncompressLength: " + uncompressLength
                   + ", magicNum: " + magicNum + ", version: " + version + ", connectionId: "
                   + connectionId + ", requestId: " + requestId + ", obSeqNo: " + obSeqNo
                   + ", payloadLength: " + payloadLength + ", flag: " + flag + ", reserved: "
                   + reserved + ", headerChecksum: " + headerChecksum;
        }
    }

    public static class ExtraInfo {
        public long                      extraLength;
        public byte[]                    extraBytes;
        private boolean                  useNewExtraInfo;
        public Map<ObObj, ObObj>         obobjMap;
        public Map<ExtraInfoKey, byte[]> bytesMap;

        public ExtraInfo(boolean useNewExtraInfo) {
            this.useNewExtraInfo = useNewExtraInfo;
            
            if (!this.useNewExtraInfo) {
                obobjMap = new HashMap<>();
            } else {
                bytesMap = new HashMap<>();
            }
        }

        public void reset() {
            extraLength = 0;
            if (!useNewExtraInfo) {
                obobjMap.clear();
            } else {
                bytesMap.clear();
            }
            extraBytes = null;
        }

    }

    public Header                  header;
    public ExtraInfo               extraInfo;
    public long                    tailChecksum;
    public FullLinkTrace.TraceInfo traceInfo;

    public int                     curRequestId;
    public short                   curObSeqNo;

    public OceanBaseProtocolV20(boolean useNewExtraInfo) {
        header = new Header(useNewExtraInfo);
        extraInfo = new ExtraInfo(useNewExtraInfo);

        curRequestId = (new Random()).nextInt(UINT_MAX24);
    }

    public void setTraceInfo(FullLinkTrace.TraceInfo traceInfo) {
        this.traceInfo = traceInfo;
    }

    public void reset() {
        header.reset();
        extraInfo.reset();
        tailChecksum = 0;
    }

    public void updateRequestId() {
        curRequestId++;
        if (curRequestId == UINT_MAX24 + 1) {
            curRequestId = 0;
        }
    }

    public short getObSeqNo() {
        curObSeqNo = (short) ((curObSeqNo + 1) % 256);
        return curObSeqNo;
    }

    public void initObSeqNo(int seqNo) {
        curObSeqNo = (short) (seqNo - 1);
    }

    public boolean isExtraInfoExist() {
        return 1 == (header.flag & OB_EXTRA_INFO_EXIST);
    }

    public void setExtraInfoLength() {
        long len = 0;

        if (!extraInfo.useNewExtraInfo) {
            for (Map.Entry<ObObj, ObObj> entry : extraInfo.obobjMap.entrySet()) {
                len += entry.getKey().getSerializeSize();
                len += entry.getValue().getSerializeSize();
            }
        } else {
            for (Map.Entry<ExtraInfoKey, byte[]> entry : extraInfo.bytesMap.entrySet()) {
                len += TYPE_LENGTH + LEN_LENGTH + entry.getValue().length;
            }
        }

        extraInfo.extraLength = len;
    }

    public void setExtraInfo(ObObj key, ObObj value) {
        header.flag |= OceanBaseProtocolV20.OB_EXTRA_INFO_EXIST;
        extraInfo.obobjMap.put(key, value);
    }

    public void setExtraInfo(ExtraInfoKey key, byte[] valueData) {
        header.flag |= OceanBaseProtocolV20.OB_EXTRA_INFO_EXIST;
        extraInfo.bytesMap.put(key, valueData);
    }

    public byte[] getExtraInfoBytes() throws IOException {
        extraInfo.extraBytes = new byte[(int) extraInfo.extraLength];
        Buffer extra = new Buffer(extraInfo.extraBytes);

        if (!extraInfo.useNewExtraInfo) {
            for (Map.Entry<ObObj, ObObj> entry : extraInfo.obobjMap.entrySet()) {
                entry.getKey().serialize(extra);
                entry.getValue().serialize(extra);
            }
        } else {
            for (Map.Entry<ExtraInfoKey, byte[]> entry : extraInfo.bytesMap.entrySet()) {
                extra.writeShort(entry.getKey().getValue());
                extra.writeInt(entry.getValue().length);
                extra.writeBytes(entry.getValue(), 0, entry.getValue().length);
            }
        }

        return extraInfo.extraBytes;
    }

    public void analyseExtraInfoBytes() throws IOException {
        Buffer extra = new Buffer(extraInfo.extraBytes);

        if (!extraInfo.useNewExtraInfo) {
            while (extra.remaining() > 0) {
                ObObj keyObj = new ObObj();
                ObObj valueObj = new ObObj();
                keyObj.deserialize(extra);
                valueObj.deserialize(extra);

                if (new String(keyObj.value.vStr).equalsIgnoreCase(ExtraInfoKey.FULL_TRC.name())) {
                    Buffer buf = new Buffer(valueObj.value.vStr, valueObj.valueLen);
                    decodeObFullLinkTrace(buf, buf.getLimit());
                }
            }
        } else {
            while (extra.remaining() > 0) {
                short key = extra.readShort();
                int len = extra.readInt4();
                int keyEndPos = extra.getPosition() + len;

                if (key == ExtraInfoKey.FULL_TRC.getValue()) {
                    decodeObFullLinkTrace(extra, keyEndPos);
                }
            }
        }
    }

    private void decodeObFullLinkTrace(Buffer buf, final int keyEndPos) throws IOException {
        while (buf.getPosition() < keyEndPos) {
            // must resolve type first then length
            short type = FullLinkTrace.resolveType(buf);
            int valLen = FullLinkTrace.resolveLength(buf);
            int infoEndPos = buf.getPosition() + valLen;

            if (FullLinkTrace.FullLinkTraceExtraInfoType.FLT_QUERY_INFO.getValue() == type) {
                traceInfo.queryInfo.type = FullLinkTrace.FullLinkTraceExtraInfoType.FLT_QUERY_INFO;
                traceInfo.queryInfo.deserialize(buf, infoEndPos);
            } else if (FullLinkTrace.FullLinkTraceExtraInfoType.FLT_CONTROL_INFO.getValue() == type) {
                traceInfo.controlInfo.type = FullLinkTrace.FullLinkTraceExtraInfoType.FLT_CONTROL_INFO;
                traceInfo.controlInfo.deserialize(buf, infoEndPos);
            } else if (FullLinkTrace.FullLinkTraceExtraInfoType.FLT_APP_INFO.getValue() == type) {
                traceInfo.appInfo.type = FullLinkTrace.FullLinkTraceExtraInfoType.FLT_APP_INFO;
                traceInfo.appInfo.deserialize(buf, infoEndPos);
            } else if (FullLinkTrace.FullLinkTraceExtraInfoType.FLT_SPAN_INFO.getValue() == type) {
                traceInfo.spanInfo.type = FullLinkTrace.FullLinkTraceExtraInfoType.FLT_SPAN_INFO;
                traceInfo.spanInfo.deserialize(buf, infoEndPos);
            } else {
                buf.skipBytes(valLen);
            }
        }
    }

}
