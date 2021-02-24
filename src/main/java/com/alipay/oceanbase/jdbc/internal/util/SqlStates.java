package com.alipay.oceanbase.jdbc.internal.util;

public enum SqlStates {
    WARNING("01"), NO_DATA("02"), CONNECTION_EXCEPTION("08"), FEATURE_NOT_SUPPORTED("0A"), CARDINALITY_VIOLATION(
                                                                                                                 "21"), DATA_EXCEPTION(
                                                                                                                                       "22"), CONSTRAINT_VIOLATION(
                                                                                                                                                                   "23"), INVALID_CURSOR_STATE(
                                                                                                                                                                                               "24"), INVALID_TRANSACTION_STATE(
                                                                                                                                                                                                                                "25"), INVALID_AUTHORIZATION(
                                                                                                                                                                                                                                                             "28"), SQL_FUNCTION_EXCEPTION(
                                                                                                                                                                                                                                                                                           "2F"), TRANSACTION_ROLLBACK(
                                                                                                                                                                                                                                                                                                                       "40"), SYNTAX_ERROR_ACCESS_RULE(
                                                                                                                                                                                                                                                                                                                                                       "42"), INVALID_CATALOG(
                                                                                                                                                                                                                                                                                                                                                                              "3D"), INTERRUPTED_EXCEPTION(
                                                                                                                                                                                                                                                                                                                                                                                                           "70"), UNDEFINED_SQLSTATE(
                                                                                                                                                                                                                                                                                                                                                                                                                                     "HY"), TIMEOUT_EXCEPTION(
                                                                                                                                                                                                                                                                                                                                                                                                                                                              "JZ"), SQL_STATE_ILLEGAL_ARGUMENT(
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                "S1009"), DISTRIBUTED_TRANSACTION_ERROR(
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        "XA");

    private final String sqlStateGroup;

    SqlStates(final String stateGroup) {
        this.sqlStateGroup = stateGroup;
    }

    public String getSqlState() {
        return sqlStateGroup;
    }
}
