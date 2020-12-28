package com.alipay.oceanbase.jdbc.extend.datatype;

import com.alipay.oceanbase.jdbc.internal.ColumnType;
import com.alipay.oceanbase.jdbc.internal.com.send.parameters.DateParameter;
import com.alipay.oceanbase.jdbc.internal.io.output.AbstractPacketOutputStream;
import com.alipay.oceanbase.jdbc.internal.io.output.PacketOutputStream;
import com.alipay.oceanbase.jdbc.util.Options;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;

public class ComplexUtil {
    public static void storeDateTime(PacketOutputStream pos, java.util.Date dt, int bufferType,
                                     Options options) throws SQLException, IOException {
        storeDateTime413AndNewer(pos, dt, bufferType, options);
        //storeDateTime412AndOlder() fixme
    }

    public static void storeDateTime413AndNewer(PacketOutputStream pos, java.util.Date dt,
                                                int bufferType, Options options)
                                                                                throws SQLException,
                                                                                IOException {
        Calendar sessionCalendar = null;
        if (!options.useLegacyDatetimeCode) {
            if (bufferType == ColumnType.DATE.getType()) {
                sessionCalendar = new GregorianCalendar(TimeZone.getDefault());
            } else {
                sessionCalendar = new GregorianCalendar(pos.getTimeZone());
            }
        } else {
            sessionCalendar = new GregorianCalendar();
        }

        java.util.Date oldTime = sessionCalendar.getTime();

        try {
            sessionCalendar.setTime(dt);

            if (dt instanceof java.sql.Date) {
                sessionCalendar.set(Calendar.HOUR_OF_DAY, 0);
                sessionCalendar.set(Calendar.MINUTE, 0);
                sessionCalendar.set(Calendar.SECOND, 0);
            }

            byte length = (byte) 7;

            if (dt instanceof java.sql.Timestamp) {
                length = (byte) 11;
            }
            //            pos.writeBytes((byte) length, 1); // length
            pos.writeBytes(length, 1); // length
            int year = sessionCalendar.get(Calendar.YEAR);
            int month = sessionCalendar.get(Calendar.MONTH) + 1;
            int date = sessionCalendar.get(Calendar.DAY_OF_MONTH);

            pos.writeIntV1(year);
            pos.writeBytes((byte) month, 1);
            pos.writeBytes((byte) date, 1);

            if (dt instanceof java.sql.Date) {
                pos.writeBytes((byte) 0, 1);
                pos.writeBytes((byte) 0, 1);
                pos.writeBytes((byte) 0, 1);
            } else {
                pos.writeBytes((byte) sessionCalendar.get(Calendar.HOUR_OF_DAY), 1);
                pos.writeBytes((byte) sessionCalendar.get(Calendar.MINUTE), 1);
                pos.writeBytes((byte) sessionCalendar.get(Calendar.SECOND), 1);
            }

            if (length == 11) {
                //	MySQL expects microseconds, not nanos
                pos.writeLongV1(((java.sql.Timestamp) dt).getNanos() / 1000);
            }

        } finally {
            sessionCalendar.setTime(oldTime);
        }

    }

    public static ColumnType getMysqlType(int complexType) throws SQLException {
        switch (complexType) {
            case ComplexDataType.TYPE_DATE:
                return ColumnType.DATETIME;
            case ComplexDataType.TYPE_COLLECTION:
                return ColumnType.COMPLEX;
            case ComplexDataType.TYPE_NUMBER:
                return ColumnType.DECIMAL;
            case ComplexDataType.TYPE_VARCHAR2:
                return ColumnType.VARCHAR;
            case ComplexDataType.TYPE_OBJECT:
                return ColumnType.COMPLEX;
            case ComplexDataType.TYPE_RAW:
                return ColumnType.RAW;
            default:
                throw new SQLException("unsupported complex type");
        }
    }

    public static void storeComplexStruct(PacketOutputStream pos, ComplexData data, Options options)
                                                                                                    throws Exception {
        int nullCount = (data.getAttrCount() + 7) / 8; // 最少8位
        int nullBitsPosition = pos.getPosition();
        for (int i = 0; i < nullCount; i++) {
            pos.writeBytes((byte) 0, 1);
        }
        byte[] nullBitsBuffer = new byte[nullCount];
        for (int i = 0; i < data.getAttrCount(); ++i) {
            if (null != data.getAttrData(i)) {
                storeComplexAttrData(pos, data.getComplexType().getAttrType(i),
                    data.getAttrData(i), options);
            } else {
                nullBitsBuffer[i / 8] |= (1 << (i % 8));
            }
        }
        int endPosition = pos.getPosition();
        pos.setPosition(nullBitsPosition);
        pos.write(nullBitsBuffer);
        pos.setPosition(endPosition);

    }

    public static void storeComplexArray(PacketOutputStream pos, ComplexData data, Options options)
                                                                                                   throws Exception {
        pos.writeFieldLength(data.getAttrCount());
        int nullCount = (data.getAttrCount() + 7) / 8;
        int nullBitsPosition = pos.getPosition();
        for (int i = 0; i < nullCount; i++) {
            pos.writeBytes((byte) 0, 1);
        }
        byte[] nullBitsBuffer = new byte[nullCount];
        for (int i = 0; i < data.getAttrCount(); ++i) {
            if (null != data.getAttrData(i)) {
                storeComplexAttrData(pos, data.getComplexType().getAttrType(0),
                    data.getAttrData(i), options);
            } else {
                nullBitsBuffer[i / 8] |= (1 << (i % 8));
            }
        }
        int endPosition = pos.getPosition();
        pos.setPosition(nullBitsPosition);
        pos.write(nullBitsBuffer);
        pos.setPosition(endPosition);
    }

    public static void storeComplexAttrData(PacketOutputStream pos, ComplexDataType type,
                                            Object value, Options options) throws Exception {
        switch (type.getType()) {
            case ComplexDataType.TYPE_COLLECTION:
                storeComplexArray(pos, (ComplexData) value, options);
                return;
            case ComplexDataType.TYPE_OBJECT:
                storeComplexStruct(pos, (ComplexData) value, options);
                return;
            case ComplexDataType.TYPE_NUMBER:
                String valueStr = String.valueOf((Integer) value);
                pos.writeFieldLength(valueStr.getBytes().length);
                pos.write(valueStr.getBytes());
                return;
            case ComplexDataType.TYPE_DATE:
                storeDateTime(pos, (java.util.Date) value, ColumnType.DATETIME.getType(), options); // todo
                return;
            case ComplexDataType.TYPE_VARCHAR2:
            case ComplexDataType.TYPE_RAW:
                if (value instanceof byte[]) {
                    byte[] tmp = (byte[]) value;
                    pos.writeFieldLength(tmp.length);
                    pos.write(tmp);
                } else {
                    byte[] tmp = ((String) value).getBytes();
                    pos.writeFieldLength(tmp.length);
                    pos.write(tmp);
                }
                return;
            default:
                throw new SQLException("unsupported complex data type");
        }
    }
}
