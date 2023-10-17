/**
 *  OceanBase Client for Java
 *
 *  Copyright (c) 2012-2014 Monty Program Ab.
 *  Copyright (c) 2015-2020 MariaDB Corporation Ab.
 *  Copyright (c) 2021 OceanBase.
 *
 *  This library is free software; you can redistribute it and/or modify it under
 *  the terms of the GNU Lesser General Public License as published by the Free
 *  Software Foundation; either version 2.1 of the License, or (at your option)
 *  any later version.
 *
 *  This library is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 *  FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 *  for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public License along
 *  with this library; if not, write to Monty Program Ab info@montyprogram.com.
 *
 *  This particular MariaDB Client for Java file is work
 *  derived from a Drizzle-JDBC. Drizzle-JDBC file which is covered by subject to
 *  the following copyright and notice provisions:
 *
 *  Copyright (c) 2009-2011, Marcus Eriksson
 *
 *  Redistribution and use in source and binary forms, with or without modification,
 *  are permitted provided that the following conditions are met:
 *  Redistributions of source code must retain the above copyright notice, this list
 *  of conditions and the following disclaimer.
 *
 *  Redistributions in binary form must reproduce the above copyright notice, this
 *  list of conditions and the following disclaimer in the documentation and/or
 *  other materials provided with the distribution.
 *
 *  Neither the name of the driver nor the names of its contributors may not be
 *  used to endorse or promote products derived from this software without specific
 *  prior written permission.
 *
 *  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS  AND CONTRIBUTORS "AS IS"
 *  AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 *  WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 *  IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 *  INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 *  NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 *  PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 *  WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 *  ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
 *  OF SUCH DAMAGE.
 */
package com.oceanbase.jdbc;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Statement;
import java.util.*;
import java.util.regex.Matcher;

import com.oceanbase.jdbc.internal.ColumnType;

public class OceanBaseCallableParameterMetaData extends CallableParameterMetaData {

    protected Map<String, CallParameter> mapNameToParameter;

    /**
     * Retrieve Callable metaData.
     *
     * @param con connection
     * @param database database name
     * @param name procedure/function name
     * @param isFunction is it a function
     */
    public OceanBaseCallableParameterMetaData(OceanBaseConnection con, String database,
                                              String name, boolean isFunction) {
        super(con, database, name, isFunction);
    }

    /**
     * Search metaData if not already loaded.
     *
     * @throws SQLException if error append during loading metaData
     */
    public void readMetadataFromDbIfRequired(String query, String arguments, Boolean isObFunction)
                                                                                                  throws SQLException {
        if (valid) {
            return;
        }
        this.query = query;
        queryMetaInfos();
        resetParams(arguments, isObFunction);
        valid = true;
    }

    private void queryMetaInfos() throws SQLException {
        this.mapNameToParameter = new HashMap<>();

        if (name == null || name.trim().length() <= 0) {
            return;
        }
        ResultSet rs = null;
        try (Statement stmt = con.getMetadataSafeStatement()) {
            String query_sql = "SELECT DISTINCT(ARGUMENT_NAME), IN_OUT, DATA_TYPE, DATA_PRECISION, DATA_SCALE, POSITION FROM ALL_ARGUMENTS WHERE"
                    + " (OVERLOAD is NULL OR OVERLOAD = 1) and POSITION != 0 AND object_name = '";
            StringBuilder paramMetaSql = new StringBuilder(query_sql);

            paramMetaSql.append(name);

            if (obOraclePackageName != null && obOraclePackageName.trim().length() > 0) {
                paramMetaSql.append("' and package_name = '").append(this.obOraclePackageName);
            }

            if (obOracleSchema != null && obOracleSchema.trim().length() > 0) {
                String tmp;
                if (obOracleSchema.startsWith("\"") && obOracleSchema.endsWith("\"")) {
                    tmp  = obOracleSchema.replace("\"", "");
                } else {
                    tmp = this.obOracleSchema.toUpperCase();
                }
                paramMetaSql.append("' and owner =  '").append(tmp);
                paramMetaSql.append("' order by POSITION");
            } else{
                if(this.obOraclePackageName != null) {
                    if (!this.obOraclePackageName.equals("DBMS_LOB")) {
                        paramMetaSql.append("' and owner = SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA')");
                    } else {
                        paramMetaSql.append("' and owner = 'SYS'");
                    }
                } else {
                    paramMetaSql.append("' and owner = SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA')");
                }
                paramMetaSql.append(" order by POSITION");
            }

            rs = stmt.executeQuery(paramMetaSql.toString());
            addParametersFromDBOD(rs);
        } catch (SQLSyntaxErrorException sqlSyntaxErrorException) {
            throw new SQLException(
                    "Access to metaData informations not granted for current user. Consider grant select access to mysql.proc "
                            + " or avoid using parameter by name",
                    sqlSyntaxErrorException);
        }finally {
            if (rs != null){
                rs.close();
            }
        }
    }

    private void parseFunctionReturnParam(String functionReturn) throws SQLException {
        if (functionReturn == null || functionReturn.length() == 0) {
            throw new SQLException(name + "is not a function returning value");
        }
        Matcher matcher = RETURN_PATTERN.matcher(functionReturn);
        if (!matcher.matches()) {
            throw new SQLException("can not parse return value definition :" + functionReturn);
        }
        CallParameter callParameter = params.get(0);

        callParameter.setOutput(true);
        callParameter.setSigned(matcher.group(1) == null);

        String typeName = matcher.group(2).trim();
        callParameter.setTypeName(typeName);
        int sqlType = ColumnType.convertDbTypeToSqlType(typeName);
        callParameter.setSqlType(sqlType);
        callParameter.setClassName(ColumnType.convertSqlTypeToClass(sqlType).getName());

        String columnSize = matcher.group(3);
        if (columnSize != null) {
            columnSize = columnSize.trim().replace("(", "").replace(")", "").replace(" ", "");
            if (columnSize.contains(",")) {
                columnSize = columnSize.substring(0, columnSize.indexOf(","));
            }
            callParameter.setPrecision(Integer.parseInt(columnSize));
        }
    }

    private void parseParamList(boolean isFunction, String paramList) throws SQLException {
        params = new ArrayList<>();
        if (isFunction) {
            // output parameter
            params.add(new CallParameter());
        }

        Matcher matcher2 = PARAMETER_PATTERN.matcher(paramList);
        while (matcher2.find()) {
            CallParameter callParameter = new CallParameter();

            String direction = matcher2.group(1);
            if (direction != null) {
                direction = direction.trim();
            }
            if (direction == null || direction.equalsIgnoreCase("IN")) {
                callParameter.setInput(true);
            } else if (direction.equalsIgnoreCase("OUT")) {
                callParameter.setOutput(true);
            } else if (direction.equalsIgnoreCase("INOUT")) {
                callParameter.setInput(true);
                callParameter.setOutput(true);
            } else {
                throw new SQLException(
                        "unknown parameter direction " + direction + "for " + callParameter.getName());
            }

            callParameter.setName(matcher2.group(2).trim());
            callParameter.setSigned(matcher2.group(3) == null);

            String typeName = matcher2.group(4).trim().toUpperCase(Locale.ROOT);
            callParameter.setTypeName(typeName);
            int sqlType = ColumnType.convertDbTypeToSqlType(typeName);
            callParameter.setSqlType(sqlType);
            callParameter.setClassName(ColumnType.convertSqlTypeToClass(sqlType).getName());

            String columnSize = matcher2.group(5);
            if (columnSize != null) {
                columnSize = columnSize.trim().replace("(", "").replace(")", "").replace(" ", "");
                if (columnSize.contains(",")) {
                    columnSize = columnSize.substring(0, columnSize.indexOf(","));
                }
                callParameter.setPrecision(Integer.parseInt(columnSize));
            }

            params.add(callParameter);
        }
    }

    /**
     * Read procedure metadata from mysql.proc table(column param_list).
     *
     * @throws SQLException if data doesn't correspond.
     */
    private void readMetadata() throws SQLException {
        if (valid) {
            return;
        }

        String[] metaInfos = null;//queryMetaInfos(isFunction);
        String paramList = metaInfos[0];
        String functionReturn = metaInfos[1];

        parseParamList(isFunction, paramList);

        // parse type of the return value (for functions)
        if (isFunction) {
            parseFunctionReturnParam(functionReturn);
        }
    }

    private void addParametersFromDBOD(java.sql.ResultSet paramTypesRs) throws SQLException {
        this.params = new ArrayList<CallParameter>();
        if (isFunction) {
            CallParameter callParameter = new CallParameter();
            callParameter.setOutput(true);
            callParameter.setInput(false);
            callParameter.setName("functionreturn");
            params.add(callParameter);
        }

        int i = 0;
        while (paramTypesRs.next()) {
            String inOut = paramTypesRs.getString("IN_OUT");
            int inOutModifier = DatabaseMetaData.procedureColumnUnknown;
            boolean isOutParameter = false;
            boolean isInParameter = false;

            if (this.getParameterCount() == 0 && this.isFunction) {
                isOutParameter = true;
            } else if (null == inOut || inOut.equalsIgnoreCase("IN")) {
                isInParameter = true;
                inOutModifier = DatabaseMetaData.procedureColumnIn;
            } else if (inOut.equalsIgnoreCase("INOUT")) {
                isOutParameter = true;
                isInParameter = true;
                inOutModifier = DatabaseMetaData.procedureColumnInOut;
            } else if (inOut.equalsIgnoreCase("OUT")) {
                isOutParameter = true;
                inOutModifier = DatabaseMetaData.procedureColumnOut;
            } else {
                isInParameter = true;
                inOutModifier = DatabaseMetaData.procedureColumnIn;
            }

            String paramName = paramTypesRs.getString("ARGUMENT_NAME");
            String typeName = paramTypesRs.getString("DATA_TYPE");
            //TODO need add currect jdbcType
            int jdbcType = ColumnType.convertDbTypeToSqlType(typeName);//ColumnType.convertSqlTypeToClass(1);
            int precision = paramTypesRs.getInt("DATA_PRECISION");
            int scale = paramTypesRs.getInt("DATA_SCALE");

            CallParameter paramInfoToAdd = new CallParameter(paramName, i++, isInParameter,
                isOutParameter, jdbcType, typeName, precision, scale, jdbcType, inOutModifier);

            this.params.add(paramInfoToAdd);
            this.mapNameToParameter.put(paramName, paramInfoToAdd);
        }
    }
}
