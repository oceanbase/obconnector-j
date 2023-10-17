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

import com.oceanbase.jdbc.internal.com.read.Buffer;

public class ObObj {

    private enum ObObjType {
        ObNullType, // 0，空类型

        ObTinyIntType, // int8, aka mysql boolean type
        ObSmallIntType, // int16
        ObMediumIntType, // int24
        ObInt32Type, // int32
        ObIntType, // int64, aka bigint
        ObUTinyIntType, // uint8
        ObUSmallIntType, // uint16
        ObUMediumIntType, // uint24
        ObUInt32Type, // uint32
        ObUInt64Type, // uint64

        ObFloatType, // single-precision floating point
        ObDoubleType, // double-precision floating point
        ObUFloatType, // unsigned single-precision floating point
        ObUDoubleType, // unsigned double-precision floating point
        ObNumberType, // aka decimal/numeric
        ObUNumberType,
        ObDateTimeType,
        ObTimestampType,
        ObDateType,
        ObTimeType,

        ObYearType,
        ObVarcharType, // charset: utf8mb4 or binary
        ObCharType, // charset: utf8mb4 or binary
        ObHexStringType, // hexadecimal literal, e.g. X'42', 0x42, b'1001', 0b1001
        ObExtendType, // Min, Max, NOP etc.
        ObUnknownType, // For question mark(?) in prepared statement, no need to serialize. @note future new types to be defined here !!!
        ObTinyTextType,
        ObTextType,
        ObMediumTextType,
        ObLongTextType,

        ObBitType,
        ObEnumType,
        ObSetType,
        ObEnumInnerType,
        ObSetInnerType,
        ObTimestampTZType, // timestamp with time zone for oracle
        ObTimestampLTZType, // timestamp with local time zone for oracle
        ObTimestampNanoType, // timestamp nanosecond for oracle
        ObRawType, // raw type for oracle
        ObIntervalYMType, // interval year to month

        ObIntervalDSType, // interval day to second
        ObNumberFloatType, // oracle float, subtype of NUMBER
        ObNVarchar2Type, // nvarchar2
        ObNCharType, // nchar
        ObURowIDType, // UROWID
        ObLobType, // Oracle Lob
        ObMaxType; // invalid type, or count of obj type

        public static ObObjType valueOf(byte value) {
            switch (value) {
                case 0:
                    return ObObjType.ObNullType;
                case 22:
                    return ObObjType.ObVarcharType;
                default:
                    return null;
            }
        }

    }

    private enum ObCollationLevel {
        CS_LEVEL_EXPLICIT,
        CS_LEVEL_NONE,
        CS_LEVEL_IMPLICIT,
        CS_LEVEL_SYSCONST,
        CS_LEVEL_COERCIBLE,
        CS_LEVEL_NUMERIC,
        CS_LEVEL_IGNORABLE,
        CS_LEVEL_INVALID;
        // here we didn't define CS_LEVEL_INVALID as 0,
        // since 0 is a valid value for CS_LEVEL_EXPLICIT in mysql 5.6.
        // fortunately we didn't need to use it to define array like charset_arr,
        // and we didn't persist it on storage.

        public static ObCollationLevel valueOf(byte value) {
            switch (value) {
                case 2:
                    return ObCollationLevel.CS_LEVEL_IMPLICIT;
                case 7:
                    return ObCollationLevel.CS_LEVEL_INVALID;
                default:
                    return null;
            }
        }

    }

    private enum ObCollationType {
        CS_TYPE_INVALID(0),
        CS_TYPE_GBK_CHINESE_CI(28),
        CS_TYPE_UTF8MB4_GENERAL_CI(45),
        CS_TYPE_UTF8MB4_BIN(46),
        CS_TYPE_UTF16_GENERAL_CI(54),
        CS_TYPE_UTF16_BIN(55),
        CS_TYPE_BINARY(63),
        CS_TYPE_GBK_BIN(87),
        CS_TYPE_UTF16_UNICODE_CI(101),
        CS_TYPE_UTF8MB4_UNICODE_CI(224),
        CS_TYPE_GB18030_CHINESE_CI(248),
        CS_TYPE_GB18030_BIN(249),
        CS_TYPE_MAX(250);

        private final int value;

        ObCollationType(int v) {
            value = v;
        }

        public int getValue() {
            return value;
        }

        public static ObCollationType valueOf(int v) {
            switch (v) {
                case 0:
                    return ObCollationType.CS_TYPE_INVALID;
                case 28:
                    return ObCollationType.CS_TYPE_GBK_CHINESE_CI;
                case 45:
                    return ObCollationType.CS_TYPE_UTF8MB4_GENERAL_CI;
                case 46:
                    return ObCollationType.CS_TYPE_UTF8MB4_BIN;
                case 54:
                    return ObCollationType.CS_TYPE_UTF16_GENERAL_CI;
                case 55:
                    return ObCollationType.CS_TYPE_UTF16_BIN;
                case 63:
                    return ObCollationType.CS_TYPE_BINARY;
                case 87:
                    return ObCollationType.CS_TYPE_GBK_BIN;
                case 101:
                    return ObCollationType.CS_TYPE_UTF16_UNICODE_CI;
                case 224:
                    return ObCollationType.CS_TYPE_UTF8MB4_UNICODE_CI;
                case 248:
                    return ObCollationType.CS_TYPE_GB18030_CHINESE_CI;
                case 249:
                    return ObCollationType.CS_TYPE_GB18030_BIN;
                case 250:
                    return ObCollationType.CS_TYPE_MAX;
                default:
                    return null;
            }
        }

    }

    public static class ObObjMeta {
        public ObObjType        type    = ObObjType.ObNullType;
        public ObCollationLevel csLevel = ObCollationLevel.CS_LEVEL_INVALID;
        public ObCollationType  csType  = ObCollationType.CS_TYPE_INVALID;
        public byte             scale   = -1;

        private long getSerializeSize() {
            //int64_t encoded_length_uint8_t(uint8_t unused)
            return 4;
        }

        private void serialize(Buffer buf) throws IOException {
            OceanBaseSerialize.encode_int8_t(buf, (byte) type.ordinal());
            OceanBaseSerialize.encode_int8_t(buf, (byte) csLevel.ordinal());
            OceanBaseSerialize.encode_int8_t(buf, (byte) csType.getValue());
            OceanBaseSerialize.encode_int8_t(buf, scale);
        }

        private void deserialize(Buffer buf) throws IOException {
            type = ObObjType.valueOf(OceanBaseSerialize.decode_int8_t(buf));
            csLevel = ObCollationLevel.valueOf(OceanBaseSerialize.decode_int8_t(buf));
            csType = ObCollationType.valueOf(OceanBaseSerialize.decode_int8_t(buf));
            scale = OceanBaseSerialize.decode_int8_t(buf);
        }

        private void setVarcharMeta() {
            type = ObObjType.ObVarcharType;
        }

        /* private void setType(final ObObjType type) {
            if (type.equals(ObObjType.ObNullType)) {
                setCollationLevel(ObCollationLevel.CS_LEVEL_IGNORABLE);
                setCollationType(ObCollationType.CS_TYPE_BINARY);
            } else if (type.equals(ObObjType.ObUnknownType) || type.equals(ObObjType.ObExtendType)) {
                setCollationLevel(ObCollationLevel.CS_LEVEL_INVALID);
                setCollationType(ObCollationType.CS_TYPE_INVALID);
            } else if (type.equals(ObObjType.ObHexStringType)) {
                setCollationType(ObCollationType.CS_TYPE_BINARY);
            } else if (!obj_type_is_string_(type)) {
                setCollationLevel(ObCollationLevel.CS_LEVEL_NUMERIC);
                setCollationType(ObCollationType.CS_TYPE_BINARY);
            }
        } */

        private void setCollationLevel(ObCollationLevel level) {
            csLevel = level;
        }

        private void setCollationType(ObCollationType type) {
            csType = type;
        }

        private void setScale(byte scale) {
            this.scale = scale;
        }

        private ObObjType getType() {
            return type;
        }

        private boolean isValid() {
            return isValidObjType(type);
        }

        private boolean isValidObjType(final ObObjType type) {
            return type.compareTo(ObObjType.ObNullType) >= 0
                   && type.compareTo(ObObjType.ObMaxType) <= 0;
        }

    }

    // union, sizeof(ObObjValue)=8
    public static class ObObjValue {
        /* c/c++ version
        int64_t int64_;
        uint64_t uint64_;
        float float_;
        double double_;
        const char* vStr;
        uint32_t *nmb_digits_;
        int64_t datetime_;
        int32_t date_;
        int64_t time_;
        uint8_t year_;
        int64_t ext_;
        int64_t unknown_; */
        public byte[] vStr;
    }

    /********************************** **********************************/

    public ObObjMeta  meta;    // sizeof = 4
    public int        valueLen; // sizeof = 4
    public ObObjValue value;   // sizeof = 8

    public ObObj() {
        meta = new ObObjMeta();
        valueLen = 0;
        value = new ObObjValue();
    }

    public long getSerializeSize() {
        long len = 0;
        len += meta.getSerializeSize();

        if (meta.getType() == ObObjType.ObVarcharType) {
            len += getSerializeSizeOfObVarcharType();
        } else {
            throw new UnsupportedOperationException("unsupported obj type");
        }

        return len;
    }

    public void serialize(Buffer extraInfo) throws IOException {
        meta.serialize(extraInfo);

        if (meta.isValid()) {
            if (meta.getType() == ObObjType.ObVarcharType) {
                serializeOfObVarcharType(extraInfo);
            } else {
                throw new UnsupportedOperationException("unsupported obj type");
            }
        } else {
            throw new IOException("invalid ob object type");
        }
    }

    public void deserialize(Buffer extraInfo) throws IOException {
        meta.deserialize(extraInfo);

        if (meta.isValid()) {
            if (meta.getType() == ObObjType.ObVarcharType) {
                deserializeOfObVarcharType(extraInfo);
            } else {
                throw new UnsupportedOperationException("unsupported obj type");
            }
        } else {
            throw new IOException("invalid ob object type");
        }
    }

    private long getSerializeSizeOfObVarcharType() {
        return OceanBaseSerialize.encoded_length_vstr_with_len(valueLen);
    }

    private void serializeOfObVarcharType(Buffer buf) throws IOException {
        OceanBaseSerialize.encode_vstr_with_len(buf, value.vStr, valueLen);
    }

    private void deserializeOfObVarcharType(Buffer buf) throws IOException {
        final long MINIMAL_NEEDED_SIZE = 2; // at least need two bytes
        if (buf.remaining() >= MINIMAL_NEEDED_SIZE) {
            value.vStr = OceanBaseSerialize.decode_vstr_nocopy(buf);
            if (null != value.vStr) {
                valueLen = value.vStr.length;
            } else {
                throw new IOException("OB DESERIALIZE ERROR");
            }
        } else {
            throw new IOException("OB DESERIALIZE ERROR");
        }
    }

    /* public void setType(final ObObjType type) {
        if (type.compareTo(ObObjType.ObNullType) < 0 || type.compareTo(ObObjType.ObMaxType) > 0) {
            meta.setType(ObObjType.ObUnknownType);
        } else {
            meta.setType(type);
        }
    } */

    public void setVarchar(final byte[] str, int size) {
        meta.setVarcharMeta();
        meta.setCollationLevel(ObCollationLevel.CS_LEVEL_IMPLICIT);
        valueLen = size;
        value.vStr = str;
    }

}
