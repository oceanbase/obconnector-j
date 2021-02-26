package com.alipay.oceanbase.jdbc.internal.com.send.parameters;

import com.alipay.oceanbase.jdbc.ObStruct;
import com.alipay.oceanbase.jdbc.extend.datatype.ComplexUtil;
import com.alipay.oceanbase.jdbc.internal.ColumnType;
import com.alipay.oceanbase.jdbc.internal.io.output.PacketOutputStream;
import com.alipay.oceanbase.jdbc.util.Options;

import java.io.IOException;

public class OBStructParameter implements Cloneable, ParameterHolder {
    ObStruct structValue;
    Options  options;

    /**
     * see complex data type protocol
     * https://yuque.antfin-inc.com/docs/share/61b0c815-c64b-40d4-a121-aa808e8bea1f
     */
    public void storeStructTypeInfo(PacketOutputStream packet) throws IOException {

        ObStruct struct = structValue;
        byte[] tmp = struct.getComplexType().getSchemaName().getBytes();
        packet.writeFieldLength(tmp.length);
        packet.write(tmp);
        tmp = struct.getComplexType().getTypeName().getBytes();
        packet.writeFieldLength(tmp.length);
        packet.write(tmp);
        packet.writeFieldLength(struct.getComplexType().getVersion());
    }

    public OBStructParameter(ObStruct structImpl, Options options) {
        this.structValue = structImpl;
        this.options = options;
    }

    @Override
    public void writeTo(PacketOutputStream os) throws IOException {

    }

    @Override
    public void writeBinary(PacketOutputStream pos) throws IOException {
        try {
            int nullCount = (structValue.getAttrCount() + 7) / 8;
            int nullBitsPosition = pos.getPosition();
            for (int i = 0; i < nullCount; i++) {
                pos.writeBytes((byte) 0, 1);
            }
            byte[] nullBitsBuffer = new byte[nullCount];
            for (int i = 0; i < structValue.getAttrCount(); ++i) {
                if (null != structValue.getAttrData(i)) {
                    ComplexUtil.storeComplexAttrData(pos,
                        structValue.getComplexType().getAttrType(i), structValue.getAttrData(i),
                        options);
                } else {
                    nullBitsBuffer[i / 8] |= (1 << (i % 8));
                }
            }
            int endPosition = pos.getPosition();
            pos.setPosition(nullBitsPosition);
            pos.write(nullBitsBuffer); // null bits
            pos.setPosition(endPosition);
        } catch (Exception e) {
            throw new IOException("storeComplexAttrData exception");
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
        return ColumnType.COMPLEX;
    }

    @Override
    public boolean isLongData() {
        return false;
    }
}
