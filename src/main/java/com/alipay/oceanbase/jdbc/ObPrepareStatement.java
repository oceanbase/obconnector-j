package com.alipay.oceanbase.jdbc;

import com.alipay.oceanbase.jdbc.extend.datatype.INTERVALDS;
import com.alipay.oceanbase.jdbc.extend.datatype.INTERVALYM;
import com.alipay.oceanbase.jdbc.extend.datatype.TIMESTAMPLTZ;
import com.alipay.oceanbase.jdbc.extend.datatype.TIMESTAMPTZ;

import java.sql.PreparedStatement;
import java.sql.Timestamp;

public interface ObPrepareStatement extends PreparedStatement {
    void setINTERVALDS(int parameterIndex, INTERVALDS intervalds) throws java.sql.SQLException;

    void setINTERVALYM(int parameterIndex, INTERVALYM intervalym) throws java.sql.SQLException;

    void setTIMESTAMP(int parameterIndex, Timestamp timestamp) throws java.sql.SQLException;

    void setTIMESTAMPLTZ(int parameterIndex, TIMESTAMPLTZ timestampltz)
                                                                       throws java.sql.SQLException;

    void setTIMESTAMPTZ(int parameterIndex, TIMESTAMPTZ timestamptz) throws java.sql.SQLException;
}
