package com.alipay.oceanbase.jdbc;

/**
 * Compatible with Mysql Connector J interface MysqlXid
 */
public class MysqlXid extends OceanBaseXid {

    public MysqlXid(byte[] globalTransactionId, byte[] branchQualifier, int formatId) {
        super(globalTransactionId, branchQualifier, formatId);
    }
}