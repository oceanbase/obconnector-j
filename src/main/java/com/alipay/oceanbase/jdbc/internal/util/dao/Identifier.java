package com.alipay.oceanbase.jdbc.internal.util.dao;

/*
Identifier, i.e table, or column name. Put into ` quotes in SHOW CREATE TABLE. Can be "multi-part", i.e `schema`.`table`
*/
public class Identifier {

    public String schema;
    public String name;

    /**
     * Identifier string value.
     *
     * @return the datas.
     */
    public String toString() {
        if (schema != null) {
            return schema + "." + name;
        }
        return name;
    }
}
