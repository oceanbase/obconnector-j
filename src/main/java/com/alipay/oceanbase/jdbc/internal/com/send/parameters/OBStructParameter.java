package com.alipay.oceanbase.jdbc.internal.com.send.parameters;

import com.alipay.oceanbase.jdbc.extend.datatype.StructImpl;
import com.alipay.oceanbase.jdbc.internal.ColumnType;
import com.alipay.oceanbase.jdbc.internal.io.output.PacketOutputStream;

import java.io.IOException;
import java.sql.SQLException;

public class OBStructParameter implements Cloneable, ParameterHolder {
    StructImpl structValue;

    /**
     * see complex data type protocol
     * https://yuque.antfin-inc.com/docs/share/61b0c815-c64b-40d4-a121-aa808e8bea1f
     */
    public void storeStructTypeInfo(PacketOutputStream packet) throws IOException {

        StructImpl struct = (StructImpl) structValue;
        //        packet.writeLenBytes(StringUtils.getBytes(struct.getComplexType().getSchemaName()));
        //        packet.writeLenBytes(StringUtils.getBytes(struct.getComplexType().getTypeName()));
        byte[] tmp = struct.getComplexType().getAttrType(0).getSchemaName().getBytes();
        packet.write(tmp);
        tmp = struct.getComplexType().getAttrType(0).getTypeName().getBytes();
        packet.write(tmp);
        packet.writeFieldLength(struct.getComplexType().getVersion());
    }

    public OBStructParameter(StructImpl structImpl) {
        this.structValue = structImpl;
    }

    @Override
    public void writeTo(PacketOutputStream os) throws IOException {

    }

    @Override
    public void writeBinary(PacketOutputStream pos) throws IOException {

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
