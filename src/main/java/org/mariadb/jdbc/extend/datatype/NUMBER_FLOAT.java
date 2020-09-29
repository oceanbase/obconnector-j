package org.mariadb.jdbc.extend.datatype;

import org.mariadb.jdbc.internal.ColumnType;
import org.mariadb.jdbc.internal.io.output.PacketOutputStream;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.SQLException;

/**
 * @Author : wb-xsy
 * @Description
 * @Date : 2020/9/24 17:38
 */
public class NUMBER_FLOAT extends Datum {

    private final float value ;
    private BigDecimal bigDecimal =null ;

    public NUMBER_FLOAT(float value) {
        this.value = value;
    }

    public NUMBER_FLOAT(float value, byte[] bytes) {
        super(bytes);
        this.value = value;
        if (bytes != null) {
            bigDecimal = new BigDecimal(new String(bytes));
        }
    }

    public void writeTo(final PacketOutputStream pos) throws IOException {
        pos.write(String.valueOf(value).getBytes());
    }

    public int getApproximateTextProtocolLength() {
        return String.valueOf(value).getBytes().length;
    }

    /**
     * Write data to socket in binary format.
     *
     * @param pos socket output stream
     * @throws IOException if socket error occur
     */
    public void writeBinary(final PacketOutputStream pos) throws IOException {
        pos.writeLong(Double.doubleToLongBits(value));
    }

    public ColumnType getColumnType() {
        return ColumnType.DOUBLE;
    }

    @Override
    public String toString() {
        return Double.toString(value);
    }

    public boolean isNullData() {
        return false;
    }

    public boolean isLongData() {
        return false;
    }

    @Override
    public boolean isConvertibleTo(Class var1) {
        String var2 = var1.getName() ;
        return var2.compareTo("java.lang.Float") == 0 || var2.compareTo("java.lang.Double") == 0 ;
    }

    @Override
    public Object toJdbc() throws SQLException {
        return this.bigDecimal;
    }

    @Override
    public Object makeJdbcArray(int var1) {
        BigDecimal[] var2 = new BigDecimal[var1];
        return var2;
    }
}
