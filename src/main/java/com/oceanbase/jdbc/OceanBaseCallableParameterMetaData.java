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
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class OceanBaseCallableParameterMetaData extends CallableParameterMetaData {

    private static final Pattern PARAMETER_PATTERN = Pattern.compile(
            "\\s*(IN\\s+|OUT\\s+|INOUT\\s+)?([\\w\\d]+)\\s+(UNSIGNED\\s+)?(\\w+)\\s*(\\([\\d,]+\\))?\\s*",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern RETURN_PATTERN = Pattern.compile(
            "\\s*(UNSIGNED\\s+)?(\\w+)\\s*(\\([\\d,]+\\))?\\s*(CHARSET\\s+)?(\\w+)?\\s*", Pattern.CASE_INSENSITIVE);
    /*
     * private final OceanBaseConnection con; private final String name; private List<CallParameter> params; private
     * String database; private boolean valid; private boolean isFunction;
     */
    private Map<String, CallParameter> mapNameToParamter;

    public String getProName() {
        return name;
    }

    public String getDatabase() {
        return database;
    }

    /**
     * Retrieve Callable metaData.
     *
     * @param con
     *            connection
     * @param database
     *            database name
     * @param name
     *            procedure/function name
     * @param isFunction
     *            is it a function
     */
    public OceanBaseCallableParameterMetaData(OceanBaseConnection con, String database, String name,
            boolean isFunction) {
        super(con, database, name, isFunction);
        /*
         * this.params = null; this.con = con; if (database != null) { this.database = database.replace("`", ""); } else
         * { this.database = null; } this.name = name.replace("`", ""); this.isFunction = isFunction;
         */
    }

    /**
     * Search metaData if not already loaded.
     *
     * @throws SQLException
     *             if error append during loading metaData
     */
    @Override
    public void readMetadataFromDbIfRequired() throws SQLException {
        if (valid) {
            return;
        }
        // readMetadata();
        queryMetaInfos(false);
        valid = true;
    }

    private int mapMariaDbTypeToJdbc(String str) {
        switch (str.toUpperCase(Locale.ROOT)) {
        case "BIT":
            return Types.BIT;
        case "TINYINT":
            return Types.TINYINT;
        case "SMALLINT":
            return Types.SMALLINT;
        case "MEDIUMINT":
            return Types.INTEGER;
        case "INT":
            return Types.INTEGER;
        case "INTEGER":
            return Types.INTEGER;
        case "LONG":
            return Types.INTEGER;
        case "BIGINT":
            return Types.BIGINT;
        case "INT24":
            return Types.INTEGER;
        case "REAL":
            return Types.DOUBLE;
        case "FLOAT":
            return Types.FLOAT;
        case "DECIMAL":
            return Types.DECIMAL;
        case "NUMERIC":
            return Types.NUMERIC;
        case "DOUBLE":
            return Types.DOUBLE;
        case "CHAR":
            return Types.CHAR;
        case "VARCHAR":
            return Types.VARCHAR;
        case "DATE":
            return Types.DATE;
        case "TIME":
            return Types.TIME;
        case "YEAR":
            return Types.SMALLINT;
        case "TIMESTAMP":
            return Types.TIMESTAMP;
        case "DATETIME":
            return Types.TIMESTAMP;
        case "TINYBLOB":
            return Types.BINARY;
        case "BLOB":
            return Types.LONGVARBINARY;
        case "MEDIUMBLOB":
            return Types.LONGVARBINARY;
        case "LONGBLOB":
            return Types.LONGVARBINARY;
        case "TINYTEXT":
            return Types.VARCHAR;
        case "TEXT":
            return Types.LONGVARCHAR;
        case "MEDIUMTEXT":
            return Types.LONGVARCHAR;
        case "LONGTEXT":
            return Types.LONGVARCHAR;
        case "ENUM":
            return Types.VARCHAR;
        case "SET":
            return Types.VARCHAR;
        case "GEOMETRY":
            return Types.LONGVARBINARY;
        case "VARBINARY":
            return Types.VARBINARY;
        default:
            return Types.OTHER;
        }
    }

    private void queryMetaInfos(boolean isFunction) throws SQLException {

        this.mapNameToParamter = new HashMap<>();
        try (Statement stmt = con.createStatement()) {
            String query_sql = "SELECT DISTINCT(ARGUMENT_NAME), IN_OUT, DATA_TYPE, DATA_PRECISION, DATA_SCALE, POSITION FROM ALL_ARGUMENTS WHERE"
                    + " (OVERLOAD is NULL OR OVERLOAD = 1) and POSITION != 0 AND object_name = '";
            StringBuilder paramMetaSql = new StringBuilder(query_sql);

            paramMetaSql.append(name);

            if (obOraclePackageName != null && obOraclePackageName.trim().length() > 0) {
                paramMetaSql.append("' and package_name = '").append(this.obOraclePackageName);
            }

            if (obOracleSchema != null && obOracleSchema.trim().length() > 0) {
                paramMetaSql.append("' and owner =  '").append(this.obOracleSchema.toUpperCase());
                paramMetaSql.append("' order by POSITION");
            } else {
                if (this.obOraclePackageName != null) {
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

            ResultSet rs = stmt.executeQuery(paramMetaSql.toString());
            addParametersFromDBOD(rs);

        } catch (SQLSyntaxErrorException sqlSyntaxErrorException) {
            throw new SQLException(
                    "Access to metaData informations not granted for current user. Consider grant select access to mysql.proc "
                            + " or avoid using parameter by name",
                    sqlSyntaxErrorException);
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
        callParameter.setTypeName(matcher.group(2).trim());
        callParameter.setSqlType(mapMariaDbTypeToJdbc(callParameter.getTypeName()));
        String scale = matcher.group(3);
        if (scale != null) {
            scale = scale.replace("(", "").replace(")", "").replace(" ", "");
            callParameter.setScale(Integer.valueOf(scale));
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

            callParameter.setName(matcher2.group(2).trim());
            callParameter.setSigned(matcher2.group(3) == null);
            callParameter.setTypeName(matcher2.group(4).trim().toUpperCase(Locale.ROOT));

            if (direction == null || direction.equalsIgnoreCase("IN")) {
                callParameter.setInput(true);
            } else if (direction.equalsIgnoreCase("OUT")) {
                callParameter.setOutput(true);
            } else if (direction.equalsIgnoreCase("INOUT")) {
                callParameter.setInput(true);
                callParameter.setOutput(true);
            } else {
                throw new SQLException("unknown parameter direction " + direction + "for " + callParameter.getName());
            }

            callParameter.setSqlType(mapMariaDbTypeToJdbc(callParameter.getTypeName()));

            String scale = matcher2.group(5);
            if (scale != null) {
                scale = scale.trim().replace("(", "").replace(")", "").replace(" ", "");
                if (scale.contains(",")) {
                    scale = scale.substring(0, scale.indexOf(","));
                }
                callParameter.setScale(Integer.valueOf(scale));
            }
            params.add(callParameter);
        }
    }

    /**
     * Read procedure metadata from mysql.proc table(column param_list).
     *
     * @throws SQLException
     *             if data doesn't correspond.
     */
    private void readMetadata() throws SQLException {
        if (valid) {
            return;
        }

        String[] metaInfos = null;// queryMetaInfos(isFunction);
        String paramList = metaInfos[0];
        String functionReturn = metaInfos[1];

        parseParamList(isFunction, paramList);

        // parse type of the return value (for functions)
        if (isFunction) {
            parseFunctionReturnParam(functionReturn);
        }
    }

    public int getParameterCount() {
        return params.size();
    }

    public CallParameter getParamByName(String name) {
        return mapNameToParamter.containsKey(name) ? mapNameToParamter.get(name) : null; // not supported and no used.
    }

    /*
     * private CallParameter getParam(int index) throws SQLException { if (index < 1 || index > params.size()) { throw
     * new SQLException("invalid parameter index " + index); } readMetadataFromDbIfRequired(); return params.get(index -
     * 1); }
     */
    public CallParameter getParam(int index) throws SQLException {
        if (index < 1 || index > params.size()) {
            throw new SQLException("invalid parameter index " + index);
        }
        readMetadataFromDbIfRequired();
        return params.get(index - 1);
    }

    public int isNullable(int param) throws SQLException {
        return getParam(param).getCanBeNull();
    }

    public boolean isSigned(int param) throws SQLException {
        return getParam(param).isSigned();
    }

    public int getPrecision(int param) throws SQLException {
        return getParam(param).getPrecision();
    }

    public int getScale(int param) throws SQLException {
        return getParam(param).getScale();
    }

    public int getParameterType(int param) throws SQLException {
        return getParam(param).getSqlType();
    }

    public String getParameterTypeName(int param) throws SQLException {
        return getParam(param).getTypeName();
    }

    public String getParameterClassName(int param) throws SQLException {
        return getParam(param).getClassName();
    }

    /**
     * Get mode info.
     *
     * <ul>
     * <li>0 : unknown
     * <li>1 : IN
     * <li>2 : INOUT
     * <li>4 : OUT
     * </ul>
     *
     * @param param
     *            parameter index
     * @return mode information
     * @throws SQLException
     *             if index is wrong
     */
    public int getParameterMode(int param) throws SQLException {
        CallParameter callParameter = getParam(param);
        if (callParameter.isInput() && callParameter.isOutput()) {
            return parameterModeInOut;
        }
        if (callParameter.isInput()) {
            return parameterModeIn;
        }
        if (callParameter.isOutput()) {
            return parameterModeOut;
        }
        return parameterModeUnknown;
    }

    public String getName(int param) throws SQLException {
        return getParam(param).getName();
    }

    public <T> T unwrap(Class<T> iface) {
        return null;
    }

    public boolean isWrapperFor(Class<?> iface) {
        return false;
    }

    private void addParametersFromDBOD(java.sql.ResultSet paramTypesRs) throws SQLException {
        int i = 0;
        this.params = new ArrayList<CallParameter>();
        if (isFunction) {
            CallParameter callParameter = new CallParameter();
            callParameter.setOutput(true);
            callParameter.setInput(false);
            callParameter.setName("functionreturn");
            params.add(callParameter);
        }
        while (paramTypesRs.next()) {
            String paramName = paramTypesRs.getString("ARGUMENT_NAME");
            String inOut = paramTypesRs.getString("IN_OUT");
            int inOutModifier = DatabaseMetaData.procedureColumnUnknown;
            boolean isOutParameter = false;
            boolean isInParameter = false;
            if (this.getParameterCount() == 0 && this.isFunction) {
                isOutParameter = true;
                isInParameter = false;
            } else if (null == inOut || inOut.equalsIgnoreCase("IN")) {
                isOutParameter = false;
                isInParameter = true;
                inOutModifier = DatabaseMetaData.procedureColumnIn;
            } else if (inOut.equalsIgnoreCase("INOUT")) {
                isOutParameter = true;
                isInParameter = true;
                inOutModifier = DatabaseMetaData.procedureColumnInOut;
            } else if (inOut.equalsIgnoreCase("OUT")) {
                isOutParameter = true;
                isInParameter = false;
                inOutModifier = DatabaseMetaData.procedureColumnOut;
            } else {
                isInParameter = true;
                isOutParameter = false;
                inOutModifier = DatabaseMetaData.procedureColumnIn;
            }

            String typeName = paramTypesRs.getString("DATA_TYPE");
            int precision = paramTypesRs.getInt("DATA_PRECISION");
            int scale = paramTypesRs.getInt("DATA_SCALE");
            // int jdbcType = MysqlDefs.mysqlToJavaType(typeName);
            // ColumnType.getColumnTypeName(typeName);
            // TODO need add currect jdbcType
            int jdbcType = mapMariaDbTypeToJdbc(typeName);// ColumnType.classFromJavaType(1);
            short nullability = DatabaseMetaData.procedureNullable;

            CallParameter paramInfoToAdd = new CallParameter(paramName, i++, isInParameter, isOutParameter, jdbcType,
                    typeName, precision, scale, nullability, inOutModifier);

            // this.parameterList.add(paramInfoToAdd);
            this.params.add(paramInfoToAdd);
            this.mapNameToParamter.put(paramName, paramInfoToAdd);
            // this.parameterMap.put(paramName, paramInfoToAdd);
            // ++this.numParameters;
        }
    }
}
