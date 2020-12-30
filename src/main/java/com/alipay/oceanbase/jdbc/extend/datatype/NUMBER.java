package com.alipay.oceanbase.jdbc.extend.datatype;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.SQLException;

public class NUMBER extends Datum {
    private BigDecimal bigDecimal;
    static final long  serialVersionUID = -1656085588913430059L;

    public NUMBER() {
    }

    public NUMBER(byte[] bytes) {
        super(bytes);
        if (bytes != null) {
            bigDecimal = new BigDecimal(new String(bytes));
        }
    }

    @Override
    public boolean isConvertibleTo(Class var1) {
        String var2 = var1.getName();
        return var2.compareTo("java.lang.Integer") == 0 || var2.compareTo("java.lang.Long") == 0
               || var2.compareTo("java.lang.Float") == 0 || var2.compareTo("java.lang.Double") == 0
               || var2.compareTo("java.math.BigInteger") == 0
               || var2.compareTo("java.math.BigDecimal") == 0
               || var2.compareTo("java.lang.String") == 0
               || var2.compareTo("java.lang.Boolean") == 0 || var2.compareTo("java.lang.Byte") == 0
               || var2.compareTo("java.lang.Short") == 0;
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

    public double doubleValue() {
        if (this.bigDecimal != null) {
            return this.bigDecimal.doubleValue();
        }
        return 0.0;
    }

    public float floatValue() {
        if (this.bigDecimal != null) {
            return this.bigDecimal.floatValue();
        }
        return 0.0f;
    }

    public long longValue() {
        if (this.bigDecimal != null) {
            return this.bigDecimal.longValue();
        }
        return 0;
    }

    public int intValue() {
        if (this.bigDecimal != null) {
            return this.bigDecimal.intValue();
        }
        return 0;
    }

    public short shortValue() {
        if (this.bigDecimal != null) {
            return this.bigDecimal.shortValue();
        }
        return 0;
    }

    public byte byteValue() {
        if (this.bigDecimal != null) {
            return this.bigDecimal.byteValue();
        }
        return 0;
    }

    public BigInteger bigIntegerValue() {
        if (this.bigDecimal != null) {
            return this.bigDecimal.toBigInteger();
        }
        return null;
    }

    public BigDecimal bigDecimalValue() throws SQLException {
        return this.bigDecimal;
    }

    public String stringValue() {
        if (this.bigDecimal != null) {
            return this.bigDecimal.toString();
        }
        return null;
    }

    public boolean booleanValue() {
        if (this.bigDecimal != null) {
            return this.bigDecimal.intValue() == 0;
        }
        return false;
    }

    @Override
    public String toString() {
        if (this.bigDecimal != null)
            return this.bigDecimal.toString();
        return null;
    }
}
