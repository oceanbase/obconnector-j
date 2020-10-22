package com.alipay.oceanbase.jdbc.internal.com.send.parameters;

import com.alipay.oceanbase.jdbc.extend.datatype.ArrayImpl;
import com.alipay.oceanbase.jdbc.extend.datatype.ComplexDataType;
import com.alipay.oceanbase.jdbc.extend.datatype.ComplexUtil;
import com.alipay.oceanbase.jdbc.internal.ColumnType;
import com.alipay.oceanbase.jdbc.internal.io.output.AbstractPacketOutputStream;
import com.alipay.oceanbase.jdbc.internal.io.output.PacketOutputStream;
import com.alipay.oceanbase.jdbc.util.Options;

import java.io.IOException;
import java.sql.SQLException;

public class OBArrayParameter implements Cloneable, ParameterHolder {
    ArrayImpl arrayValue;
    Options   options;

    public OBArrayParameter(ArrayImpl arrayImpl, Options options) {
        this.arrayValue = arrayImpl;
        this.options = options;
    }

    /**
     * see complex data type protocol
     * https://yuque.antfin-inc.com/docs/share/61b0c815-c64b-40d4-a121-aa808e8bea1f
     */
    public void storeArrayTypeInfo(PacketOutputStream packet) throws IOException, SQLException {
        ArrayImpl array = (ArrayImpl) arrayValue;
        packet.writeFieldLength(0);
        packet.writeFieldLength(0);
        packet.writeFieldLength(array.getComplexType().getVersion());
        int elementType = ComplexUtil.getMysqlType(array.getBaseType()).getType();
        packet.writeBytes((byte) elementType, 1);
        if (elementType >= ColumnType.COMPLEX.getType()
            && elementType <= ColumnType.STRUCT.getType()) {
            // element type is also a complex type
            //            packet.writeLenBytes(StringUtils.getBytes(array.getComplexType().getAttrType(0)
            //                    .getSchemaName()));
            //            packet.writeLenBytes(StringUtils.getBytes(array.getComplexType().getAttrType(0)
            //                    .getTypeName()));
            byte[] tmp = array.getComplexType().getAttrType(0).getSchemaName().getBytes();
            packet.writeFieldLength(tmp.length);
            packet.write(tmp);
            tmp = array.getComplexType().getAttrType(0).getTypeName().getBytes();
            packet.writeFieldLength(tmp.length);
            packet.write(tmp);
            packet.writeFieldLength(tmp.length);

        }
    }

    @Override
    public void writeTo(PacketOutputStream os) throws IOException {

    }

    @Override
    public void writeBinary(PacketOutputStream pos) throws IOException {
        try {
            pos.writeFieldLength(arrayValue.getAttrCount());
            int nullCount = (arrayValue.getAttrCount() + 7) / 8;
            int nullBitsPosition = pos.getPosition();
            for (int i = 0; i < nullCount; i++) {
                pos.writeBytes((byte) 0, 1);
            }
            byte[] nullBitsBuffer = new byte[nullCount];
            for (int i = 0; i < arrayValue.getAttrCount(); ++i) {
                if (null != arrayValue.getAttrData(i)) {
                    ComplexUtil.storeComplexAttrData(pos, arrayValue.getComplexType()
                        .getAttrType(0), arrayValue.getAttrData(i), options);
                } else {
                    nullBitsBuffer[i / 8] |= (1 << (i % 8));
                }
            }
            int endPosition = pos.getPosition();
            pos.setPosition(nullBitsPosition);
            pos.write(nullBitsBuffer); // null bits
            pos.setPosition(endPosition);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public int getApproximateTextProtocolLength() throws IOException {
        return 0;
    }

    @Override
    public boolean isNullData() {
        return false;
    }

    @Override
    public ColumnType getColumnType() {
        return ColumnType.COMPLEX; // todo
    }

    @Override
    public boolean isLongData() {
        return false;
    }
}
