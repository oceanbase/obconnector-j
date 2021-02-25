package com.alipay.oceanbase.jdbc.jdbc2.optional;

import com.alipay.oceanbase.jdbc.OceanBaseXid;

/**
 * Compatible with Mysql Connector J interface MysqlXid
 */
public class MysqlXid extends OceanBaseXid {

    public MysqlXid(byte[] globalTransactionId, byte[] branchQualifier, int formatId) {
        super(globalTransactionId, branchQualifier, formatId);
    }
}