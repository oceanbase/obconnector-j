package com.oceanbase.jdbc.extend.datatype;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class MysqlComplexDataType extends ComplexDataType{

    public static final int   TYPE_TINYINT_MYSQL        = 101;
    public static final int   TYPE_SMALLINT_MYSQL       = 102;
    public static final int   TYPE_INT_MYSQL            = 103;
    public static final int   TYPE_BIGINT_MYSQL         = 104;
    public static final int   TYPE_FLOAT_MYSQL          = 105;
    public static final int   TYPE_DOUBLE_MYSQL         = 106;
    public static final int   TYPE_VARCHAR_MYSQL        = 107;
    public static final int   TYPE_MAX_MYSQL            = 999;

    private static final Map<String, Integer> TYPE_MAPPING = new HashMap<>();
    
    static {
        TYPE_MAPPING.put("tinyint", TYPE_TINYINT_MYSQL);
        TYPE_MAPPING.put("smallint", TYPE_SMALLINT_MYSQL);
        TYPE_MAPPING.put("int", TYPE_INT_MYSQL);
        TYPE_MAPPING.put("bigint", TYPE_BIGINT_MYSQL);
        TYPE_MAPPING.put("float", TYPE_FLOAT_MYSQL);
        TYPE_MAPPING.put("double", TYPE_DOUBLE_MYSQL);
        TYPE_MAPPING.put("varchar", TYPE_VARCHAR_MYSQL);
    }

    public MysqlComplexDataType(String typeName, String schemaName, int type) {
        super(typeName, schemaName, type);
    }

    public static int getObComplexType(String attrType) throws SQLException {
        if (attrType == null) {
            return TYPE_MAX_MYSQL;
        }
        
        Integer type = TYPE_MAPPING.get(attrType.toLowerCase());
        return type != null ? type : TYPE_MAX_MYSQL;
    }
}
