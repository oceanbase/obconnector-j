package com.alipay.oceanbase.jdbc;

import java.sql.Struct;

public interface ObStruct extends ObComplexData, Struct {
    // Ensure the interface is consistent Even if it is an empty interface
}
