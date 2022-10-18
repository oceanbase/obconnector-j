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

import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Types;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.oceanbase.jdbc.internal.com.read.resultset.ColumnDefinition;
import com.oceanbase.jdbc.internal.util.ParsedCallParameters;
import com.oceanbase.jdbc.internal.util.Utils;
import com.oceanbase.jdbc.internal.util.dao.ServerPrepareResult;

public class CallableParameterMetaData implements ParameterMetaData {

    protected static final Pattern      PARAMETER_PATTERN = Pattern
                                                              .compile(
                                                                  "\\s*(IN\\s+|OUT\\s+|INOUT\\s+)?(\\`[\\w\\d]+\\`)\\s+(UNSIGNED\\s+)?(\\w+)\\s*(\\([\\d,]+\\))?\\s*",
                                                                  Pattern.CASE_INSENSITIVE);
    protected static final Pattern      RETURN_PATTERN    = Pattern
                                                              .compile(
                                                                  "\\s*(UNSIGNED\\s+)?(\\w+)\\s*(\\([\\d,]+\\))?\\s*(CHARSET\\s+)?(\\w+)?\\s*",
                                                                  Pattern.CASE_INSENSITIVE);
    protected final OceanBaseConnection con;
    protected final String              name;
    protected List<CallParameter>       params;
    protected String                    obOraclePackageName;
    protected String                    obOracleSchema;
    protected String                    database;
    protected boolean                   valid;
    protected boolean                   isFunction;
    protected String                    query;

    public String getProName() {
        return name;
    }

    public String getDatabase() {
        return database;
    }

    /**
     * Retrieve Callable metaData.
     *
     * @param con connection
     * @param database database name
     * @param name procedure/function name
     * @param isFunction is it a function
     */
    public CallableParameterMetaData(OceanBaseConnection con, String database, String name,
                                     boolean isFunction) {
        this.params = null;
        this.con = con;
        if (database != null) {
            String tmp = database.replace("`", "");
            if (this.con.getProtocol().isOracleMode()) {
                if (tmp.equals(this.con.getProtocol().getDatabase())) {
                    this.database = tmp;
                    this.obOracleSchema = tmp;
                } else {
                    if (tmp.contains(".")) { // schema.package
                        //is support <user>.<package>.<stored procedure> pattern with Oracle Mode.
                        String[] databaseAndPackage = tmp.split("\\.");
                        if (databaseAndPackage.length == 2) {
                            if (databaseAndPackage[1].startsWith("\"")
                                && databaseAndPackage[1].endsWith("\"")) {
                                this.obOraclePackageName = databaseAndPackage[1].replace("\"", "");
                            } else {
                                this.obOraclePackageName = databaseAndPackage[1]
                                    .toUpperCase(Locale.ROOT);
                            }
                            if (databaseAndPackage[0].startsWith("\"")
                                && databaseAndPackage[0].endsWith("\"")) {
                                this.obOracleSchema = databaseAndPackage[0].replace("\"", "");
                            } else {
                                this.obOracleSchema = databaseAndPackage[0]
                                    .toUpperCase(Locale.ROOT);
                            }
                            this.database = databaseAndPackage[0]; // ?
                        }
                    } else {
                        // shcema or poackage
                        if (tmp.startsWith("\"") && tmp.startsWith("\"")) {
                            if (tmp.replace("\"", "").equals(this.con.getProtocol().getDatabase())) {
                                // is schema
                                this.obOracleSchema = tmp.replace("\"", "");
                                this.obOraclePackageName = null;
                            } else {
                                // is package
                                this.obOracleSchema = null;
                                this.obOraclePackageName = tmp.replace("\"", "");
                            }
                        } else if (tmp.equals(this.con.getProtocol().getDatabase())
                                   || tmp.toUpperCase(Locale.ROOT).equals(
                                       this.con.getProtocol().getDatabase())) {
                            // schema.proc
                            this.obOraclePackageName = null;
                            this.obOracleSchema = tmp.toUpperCase(Locale.ROOT);
                        } else {
                            // package.proc
                            this.obOraclePackageName = tmp.toUpperCase(Locale.ROOT);
                            this.obOracleSchema = null;
                        }
                        this.database = this.con.getProtocol().getDatabase();

                    }
                }
            } else {
                this.database = database; // for mysql mode
            }
        } else {
            this.database = null;
        }
        if (name.startsWith("\"") && name.endsWith("\"")) {
            this.name = name.replace("`", "").replace("\"", "");
        } else {
            if (this.con.getProtocol().isOracleMode()) {
                this.name = name.replace("`", "").toUpperCase(Locale.ROOT);
            } else {
                this.name = name;
            }
        }
        this.isFunction = isFunction;
    }

    /**
     * Search metaData if not already loaded.
     *
     * @throws SQLException if error append during loading metaData
     */
    public void readMetadataFromDbIfRequired() throws SQLException {
        if (valid) {
            return;
        }
        readMetadata();
        valid = true;
    }

    public void generateMetadataFromPrepareResultSet(ServerPrepareResult serverPrepareResult) throws SQLException {
        if (valid) {
            return;
        }
        // Initialize params
        params = new ArrayList<>();
        ColumnDefinition[] parameters = serverPrepareResult.getParameters();
        for (int i = 0; i < parameters.length ; i++) {
            CallParameter callParameter = new CallParameter();
            params.add(callParameter);
        }
        valid = false;
    }

    public void readMetadataFromDbIfRequired(String query, String arguments, Boolean isObFunction)
                                                                                                  throws SQLException {
        if (valid) {
            return;
        }
        this.query = query;
        readMetadata();
        resetParams(arguments, isObFunction);
        valid = true;
    }

    void resetParams(String arguments, boolean isObFunction) {
        List<ParsedCallParameters> paramList = new ArrayList<ParsedCallParameters>() ;
        if (arguments != null) {
            arguments = Utils.trimSQLString(arguments, false, true, false);  //arguments  remove comments
            paramList = Utils.argumentsSplit(arguments, ",", "'\"",
                "'\"");
        }
        if (isObFunction) {
            paramList.add(0, new ParsedCallParameters(true, "?"));
        }

        int parameterCount = params.size();
        int[] placeholderToParameterIndexMap = new int[parameterCount];
        for (int i = 0; i < parameterCount; i++) {
            placeholderToParameterIndexMap[i] = -1; // not param
        }
        int placeholderCount = 0;
        for (int i = 0; i < paramList.size(); i++) {
            if (paramList.get(i).isParam()) { // ? or name binding param
                placeholderToParameterIndexMap[placeholderCount++] = i;
            }
        }

        List<CallParameter> currentParams = new ArrayList<>(parameterCount);
        for (int index = 0; index < parameterCount; index++) {
            if (placeholderToParameterIndexMap != null) {
                int localIndex = placeholderToParameterIndexMap[index];
                if (localIndex != -1) {
                    CallParameter parameter = params.get(localIndex);
                    currentParams.add(parameter);
                }
            } else {
                CallParameter parameter = params.get(index + 1);
                if (parameter.getIndex() != -1) {
                    currentParams.add(parameter);
                }
            }
        }
        this.params = currentParams;
    }

    protected int mapMariaDbTypeToJdbc(String str) {
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

    private String[] queryMetaInfos(boolean isFunction) throws SQLException {
    String paramList;
    String functionReturn;
    try (PreparedStatement preparedStatement =
        con.prepareStatement(
            "select param_list, returns, db, type from mysql.proc where name=? and db="
                + (database != null ? "?" : "DATABASE()"))) {

      preparedStatement.setString(1, name);
      if (database != null) {
        preparedStatement.setString(2, database);
      }

      try (ResultSet rs = preparedStatement.executeQuery()) {
        if (!rs.next()) {
          throw new SQLException(
              (isFunction ? "function" : "procedure") + " `" + name + "` does not exist");
        }
        paramList = rs.getString(1);
        functionReturn = rs.getString(2);
        database = rs.getString(3);
        this.isFunction = "FUNCTION".equals(rs.getString(4));
        return new String[] {paramList, functionReturn};
      }

    } catch (SQLSyntaxErrorException sqlSyntaxErrorException) {
      throw new SQLException(
          "Access to metaData informations not granted for current user. Consider grant select access to mysql.proc "
              + " or avoid using parameter by name",
          sqlSyntaxErrorException);
    }
  }

    private CallParameter parseFunctionReturnParam(String functionReturn) throws SQLException {
        if (functionReturn == null || functionReturn.length() == 0) {
            throw new SQLException(name + "is not a function returning value");
        }
        Matcher matcher = RETURN_PATTERN.matcher(functionReturn);
        if (!matcher.matches()) {
            throw new SQLException("can not parse return value definition :" + functionReturn);
        }
        CallParameter callParameter = new CallParameter();
        callParameter.setOutput(true);
        callParameter.setSigned(matcher.group(1) == null);
        callParameter.setTypeName(matcher.group(2).trim());
        callParameter.setSqlType(mapMariaDbTypeToJdbc(callParameter.getTypeName()));
        String scale = matcher.group(3);
        if (scale != null) {
            scale = scale.replace("(", "").replace(")", "").replace(" ", "");
            callParameter.setScale(Integer.valueOf(scale));
        }
        return callParameter;
    }

    private void parseParamList(boolean isFunction, String paramList) throws SQLException {
        params = new ArrayList<>();
        int index = 1;
        if(isFunction) {
            int returnIndex = paramList.indexOf("RETURNS");
            if(returnIndex != -1) {
                int bodyStartInedx = paramList.toUpperCase(Locale.ROOT).indexOf("BEGIN");
                String returnString = paramList.substring(returnIndex+"RETURNS".length(), bodyStartInedx);
                paramList = paramList.substring(0, returnIndex - 1);
                CallParameter parameterRetrurn = parseFunctionReturnParam(returnString);
                parameterRetrurn.setIndex(index++);
                params.add(parameterRetrurn);
            }
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
                throw new SQLException(
                        "unknown parameter direction " + direction + "for " + callParameter.getName());
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
            callParameter.setIndex(index++);
        }
    }

    /**
     * Read procedure metadata from mysql.proc table(column param_list).
     *
     * @throws SQLException if data doesn't correspond.
     */
    private void readMetadata() throws SQLException {
        if (name == null || name.equals("")) {
            return;
        }

        String procedureDDl;
        ResultSet resultSet;
        if (isFunction) {
            if (database != null) {
                resultSet = con.createStatement().executeQuery(
                    "SHOW CREATE FUNCTION " + database + "." + name);
            } else {
                resultSet = con.createStatement().executeQuery("SHOW CREATE FUNCTION " + name);
            }
            resultSet.next();
            procedureDDl = resultSet.getString("Create Function");
        } else {
            if (database != null) {
                resultSet = con.createStatement().executeQuery(
                    "SHOW CREATE PROCEDURE " + database + "." + name);
            } else {
                resultSet = con.createStatement().executeQuery("SHOW CREATE PROCEDURE " + name);
            }
            resultSet.next();
            procedureDDl = resultSet.getString("Create Procedure");
        }

        String paramList = procedureDDl.substring(procedureDDl.indexOf("(") - 1);
        parseParamList(isFunction, paramList);
    }

    public int getParameterCount() {
        return params.size();
    }

    public CallParameter getParamByName(String name) {
        return null; // not supported and no used.
    }

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
     *   <li>0 : unknown
     *   <li>1 : IN
     *   <li>2 : INOUT
     *   <li>4 : OUT
     * </ul>
     *
     * @param param parameter index
     * @return mode information
     * @throws SQLException if index is wrong
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
}
