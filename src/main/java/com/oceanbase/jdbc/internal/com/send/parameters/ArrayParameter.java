package com.oceanbase.jdbc.internal.com.send.parameters;

import com.oceanbase.jdbc.ObArray;
import com.oceanbase.jdbc.extend.datatype.MysqlComplexDataType;
import com.oceanbase.jdbc.internal.ColumnType;
import com.oceanbase.jdbc.internal.io.output.PacketOutputStream;
import com.oceanbase.jdbc.internal.util.StringUtils;

import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.SQLException;

public class ArrayParameter implements Cloneable, ParameterHolder {

    private final String  fieldString;

    private final ObArray arrayValue;

    private final Charset charset;

    public ArrayParameter(ObArray array, String characterEncoding) throws SQLException {
        this.arrayValue = array;
        this.charset = StringUtils.getCharset(characterEncoding);
        this.fieldString = getFieldString();
    }

    @Override
    public void writeTo(PacketOutputStream pos) throws IOException, SQLException {
        pos.write(fieldString);
    }

    @Override
    public void writeBinary(PacketOutputStream pos) throws IOException, SQLException {
        byte[] bytes = fieldString.getBytes(charset);
        pos.writeFieldLength(bytes.length);
        pos.write(bytes);
    }

    @Override
    public int getApproximateTextProtocolLength() throws IOException {
        return fieldString.length();
    }

    @Override
    public boolean isNullData() {
        return false;
    }

    @Override
    public ColumnType getColumnType() {
        return ColumnType.STRING;
    }

    @Override
    public boolean isLongData() {
        return false;
    }

    private String getFieldString() throws SQLException {
        if (!arrayValue.isMysqlType()) {
            throw new SQLException("Unsupported database type.");
        }

        StringBuilder builder = new StringBuilder("[");
        Object[] data = arrayValue.getAttrData();
        int baseType = arrayValue.getBaseType();
        for (int i = 0; i < data.length; i++) {
            if (baseType == MysqlComplexDataType.TYPE_VARCHAR_MYSQL) {
                builder.append("\"").append(data[i]).append("\"");
            } else {
                builder.append(data[i]);
            }
            if (i != data.length - 1) {
                builder.append(",");
            }
        }

        builder.append("]");
        return builder.toString();
    }

}
