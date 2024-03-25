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

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.oceanbase.jdbc.internal.ColumnType;
import com.oceanbase.jdbc.internal.com.read.resultset.ColumnDefinition;
import com.oceanbase.jdbc.internal.util.ParsedCallParameters;
import com.oceanbase.jdbc.internal.util.Utils;
import com.oceanbase.jdbc.internal.util.dao.ServerPrepareResult;

public class CallableParameterMetaData implements ParameterMetaData {

    protected static final Pattern      PARAMETER_PATTERN                                   = Pattern
                                                                                                .compile(
                                                                                                    "\\s*" // any empty character
                                                                                                            + "(IN\\s+|OUT\\s+|INOUT\\s+)+" // IN ｜ OUT ｜ INOUT parameter types  must be matched at least once
                                                                                                            + "(\\`[\\w\\d\\S]+\\`)\\s+" // param name
                                                                                                            + "(UNSIGNED\\s+)?" // keyWords 'UNSIGNED'
                                                                                                            + "(\\w+)\\s*" // data type matching, such as varchar
                                                                                                            + "(\\([\\d,]+\\))?" // one or more numbers separated by commas within parentheses, such as (10,2)
                                                                                                            + "\\s*(?=,?|\\))", // the separator comma between parameter lists, or the closing parenthesis of the parameter list
                                                                                                    Pattern.CASE_INSENSITIVE);

    protected static final Pattern      PARAMETER_PATTERN_MYSQL_MODE_NOT_INFORMATION_SCHEMA = Pattern
                                                                                                .compile(
                                                                                                    "\\s*" //zero or any blank charact
                                                                                                            + "(IN\\s+|OUT\\s+|INOUT\\s+)?" // 'IN' || 'OUT' || 'INOUT' parameter modifiers
                                                                                                            + "(\\`[\\w\\d\\S]+\\`)\\s+" //a String enclosed in reverse quotation marks ('XXXX')
                                                                                                            + "(UNSIGNED\\s+)?" //keyWords 'UNSIGNED'
                                                                                                            + "(\\w+)\\s*" //one or more word characters, usually the name of a data type such as INT or VARCHAR
                                                                                                            + "(\\([\\d,]+\\))?" //one or more numbers separated by commas within parentheses, such as (10,2)
                                                                                                            + "\\s*(?=,?|\\))", //The matching position is followed by zero or one comma ',' or a right parenthesis ')'
                                                                                                    Pattern.CASE_INSENSITIVE);

    // the return characteristic such like ：NO SQL | READS SQL DATA | MODIFIES SQL DATA |DETERMINISTIC
    protected static final Pattern      RETURN_PATTERN                                      = Pattern
                                                                                                .compile(
                                                                                                    "\\s*(UNSIGNED\\s+)?(\\w+)\\s*(\\([\\d,]+\\))?\\s*(CHARSET\\s+)?(\\w+|\\w+\\s+|\\w+\\s+\\w+|\\w+\\s+\\w+\\s+\\w+)?\\s*",
                                                                                                    Pattern.CASE_INSENSITIVE);
    protected final OceanBaseConnection con;
    protected boolean                   isOracleMode;
    protected final String              name;
    // a set of parameters in the form of placeholders
    protected List<CallParameter>       placeholderParams;
    protected List<CallParameter>       allParams;
    /**
     * placeholderToParameterIndexMap[i] = j
     * The assignment placeholderToParameterIndexMap[i] = j
     * indicates that the (i+1)th placeholder in the SQL string corresponds to the (j+1)th parameter in the SQL command. For instance,
     * in the SQL call call test_pro('a',?,10,?), placeholderToParameterIndexMap[0] = 1 means that the first placeholder ? is associated with the second parameter in the SQL call.
     */
    protected int[]                     placeholderToParameterIndexMap;
    protected String                    obOraclePackageName;
    protected String                    obOracleSchema;
    protected String                    database;
    protected boolean                   valid;
    protected boolean                   isFunction;
    protected String                    query;
    protected SQLException              outputParamSetParamValueException;

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
        this.con = con;
        this.isOracleMode = con.getProtocol().isOracleMode();
        if (database != null) {
            String tmp = database.replace("`", "");
            if (this.isOracleMode) {
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
            if (this.isOracleMode) {
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
        allParams = new ArrayList<>();
        ColumnDefinition[] parameters = serverPrepareResult.getParameters();
        for (int i = 0; i < parameters.length ; i++) {
            CallParameter callParameter = new CallParameter();
            allParams.add(callParameter);
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

    void resetParams(String arguments, boolean isObFunction) throws SQLException {
        List<ParsedCallParameters> paramList = new ArrayList<ParsedCallParameters>();
        if (arguments != null) {
            arguments = Utils.trimSQLString(arguments, false, true, false); //arguments  remove comments
            paramList = Utils.argumentsSplit(arguments, ",", "'\"", "'\"");
        }
        //check special parameters like "call test_p(a => ?)"
        boolean haveSpecialParameters = false;
        Pattern specialParametersPattern = Pattern.compile("\\w+\\s*=>\\s*\\?");
        for (int i = 0; i < paramList.size(); i++) {
            String parsedCallParameterName = paramList.get(i).getName();
            if (specialParametersPattern.matcher(parsedCallParameterName).find()){
                haveSpecialParameters = true;
                paramList.set(i, new ParsedCallParameters(true, parsedCallParameterName.split("=")[0].trim()));
            }
        }
        if (isObFunction) {
            paramList.add(0, new ParsedCallParameters(true, "?"));
        }

        // expected: allParams.size() >= paramList.size()
        placeholderToParameterIndexMap = new int[paramList.size()];
        int placeholderCount = 0;
        for (int i = 0; i < paramList.size(); i++) {
            if (paramList.get(i).isParam()) { // ? or name binding param
                placeholderToParameterIndexMap[placeholderCount++] = i;
            } else {
                placeholderToParameterIndexMap[i] = -1; // not param
            }
        }

        List<CallParameter> currentParams = new ArrayList<>(placeholderCount);

        for (int index = 0; index < allParams.size(); index++) {
            CallParameter parameter = allParams.get(index);

            boolean found = false;
            int len = placeholderToParameterIndexMap.length;
            for (int placeholderIndex = 0; placeholderIndex < len; placeholderIndex++) {
                if (placeholderToParameterIndexMap[placeholderIndex] == index) {
                    found = true;
                    currentParams.add(parameter);
                    break;
                } else if (placeholderToParameterIndexMap[placeholderIndex] > index || placeholderToParameterIndexMap[placeholderIndex] == -1) {
                    // if placeholderToParameterIndexMap[placeholderIndex] > index, then placeholderToParameterIndexMap[placeholderIndex + 1] > index
                    // if placeholderToParameterIndexMap[placeholderIndex] == -1, then placeholderToParameterIndexMap[placeholderIndex + 1] == -1
                    break;
                }
            }
            if (!found && parameter.isOutput() && outputParamSetParamValueException == null) {
                outputParamSetParamValueException =  new SQLException("Parameter " + parameter.getName() + " is not registered as an output parameter.");
            }
        }
        for (int i = allParams.size(); i < paramList.size(); i++) {
            currentParams.add(new CallParameter());
        }
        this.placeholderParams = currentParams;

        //reset param index when param have '=>?'
        //基于用户sql参数顺序，调整placeholderParams中对应元素的相对顺序
        if (haveSpecialParameters) {
            placeholderParams = new ArrayList<>(currentParams.size());

            for (int paramListIndex = 0; paramListIndex < paramList.size(); paramListIndex++) {
                ParsedCallParameters parsedCallParameter = paramList.get(paramListIndex);
                String parsedCallParameterName = parsedCallParameter.getName();

                boolean found = false;
                for (int currentParamIndex = 0; currentParamIndex < currentParams.size(); currentParamIndex++) {
                    CallParameter parameter = currentParams.get(currentParamIndex);
                    String parameterName = parameter.getName();
                    //compatible observer 3.2.X paramName is ('paramName')
                    if (parameterName.startsWith("`") && parameterName.endsWith("`") && !parsedCallParameterName.startsWith("`")) {
                        parsedCallParameterName = "`" + parsedCallParameterName + "`";
                    }

                    if (parsedCallParameterName.equalsIgnoreCase(parameterName)) {
                        placeholderParams.add(parameter);
                        found = true;
                        break;
                    }
                }
                if (!found && parsedCallParameter.isParam()) {
                    throw new SQLException("param matching error");
                }
            }
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
        return callParameter;
    }

    private void parseParamList(boolean isFunction, String paramList) throws SQLException {
        allParams = new ArrayList<>();
        int index = 0;
        if(isFunction) {
            int returnIndex = paramList.indexOf("RETURNS");
            if(returnIndex != -1) {
                int bodyStartInedx = paramList.toUpperCase(Locale.ROOT).indexOf("BEGIN");
                String returnString = paramList.substring(returnIndex+"RETURNS".length(), bodyStartInedx);
                paramList = paramList.substring(0, returnIndex - 1);
                CallParameter parameterRetrurn = parseFunctionReturnParam(returnString);
                parameterRetrurn.setIndex(index++);
                allParams.add(parameterRetrurn);
            }
        }

        Matcher matcher2;
        if (isFunction && !isOracleMode && !con.getProtocol().haveInformationSchemaParameters()) {
            matcher2 = PARAMETER_PATTERN_MYSQL_MODE_NOT_INFORMATION_SCHEMA.matcher(paramList);
        } else {
            matcher2 = PARAMETER_PATTERN.matcher(paramList);
        }
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

            // getPrecision: specified column size
            // getScale: number of digits to right of the decimal point
            String columnSize = matcher2.group(5);
            if (columnSize != null) {
                columnSize = columnSize.trim().replace("(", "").replace(")", "").replace(" ", "");
                if (columnSize.contains(",")) {
                    int delimiter = columnSize.indexOf(",");
                    if (delimiter != -1) {
                        callParameter.setScale(Integer.parseInt(columnSize.substring(delimiter + 1)));
                    }
                    columnSize = columnSize.substring(0, delimiter);
                }
                callParameter.setPrecision(Integer.parseInt(columnSize));
            }

            allParams.add(callParameter);
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
        if (!isOracleMode && con.getProtocol().haveInformationSchemaParameters()) {
            readMetadataUsingInformationSchema();
            return;
        }

        String procedureDDl;
        ResultSet resultSet = null;
        Statement stmt = con.getMetadataSafeStatement();
        try {
            if (isFunction) {
                if (database != null) {
                    resultSet = stmt.executeQuery("SHOW CREATE FUNCTION " + database + "." + name);
                } else {
                    resultSet = stmt.executeQuery("SHOW CREATE FUNCTION " + name);
                }
                resultSet.next();
                procedureDDl = resultSet.getString("Create Function");
            } else {
                if (database != null) {
                    resultSet = stmt.executeQuery("SHOW CREATE PROCEDURE " + database + "." + name);
                } else {
                    resultSet = stmt.executeQuery("SHOW CREATE PROCEDURE " + name);
                }
                resultSet.next();
                procedureDDl = resultSet.getString("Create Procedure");
            }
        } finally {
            if (resultSet != null) {
                resultSet.close();
            }
        }

        String paramList = procedureDDl.substring(procedureDDl.indexOf("(") - 1);
        parseParamList(isFunction, paramList);
    }

    private void readMetadataUsingInformationSchema() throws SQLException {
        PreparedStatement cps;
        if (database != null && !database.equals("")) {
            cps = con.clientPrepareStatement("SELECT * from information_schema.PARAMETERS WHERE ROUTINE_TYPE = ? AND SPECIFIC_NAME = ? AND SPECIFIC_SCHEMA = ? ORDER BY ORDINAL_POSITION",
                    Statement.NO_GENERATED_KEYS);
            cps.setString(3, this.database.replace("`", "").toUpperCase(Locale.ROOT));
        } else {
            cps = con.clientPrepareStatement("SELECT * from information_schema.PARAMETERS WHERE ROUTINE_TYPE = ? AND SPECIFIC_NAME = ? ORDER BY ORDINAL_POSITION",
                    Statement.NO_GENERATED_KEYS);
        }
        cps.setString(1, isFunction ? "FUNCTION" : "PROCEDURE");
        cps.setString(2, this.name.replace("`", "").toUpperCase(Locale.ROOT));
        ResultSet rs = cps.executeQuery();

        allParams = new ArrayList<>();
        if (rs.next()) {
            String specificSchema = rs.getString("SPECIFIC_SCHEMA");
            int count = 0;
            do {
                CallParameter callParameter = new CallParameter();
                callParameter.setName(rs.getString("PARAMETER_NAME"));
                callParameter.setSigned(!rs.getString("DTD_IDENTIFIER").contains(" unsigned"));

                String direction = rs.getString("PARAMETER_MODE");
                if (direction != null) {
                    if (direction.equalsIgnoreCase("IN")) {
                        callParameter.setInput(true);
                    } else if (direction.equalsIgnoreCase("OUT")) {
                        callParameter.setOutput(true);
                    } else if (direction.equalsIgnoreCase("INOUT")) {
                        callParameter.setInput(true);
                        callParameter.setOutput(true);
                    }
                }

                String typeName = rs.getString("DATA_TYPE").toUpperCase(Locale.ROOT);
                callParameter.setTypeName(typeName);
                int sqlType = ColumnType.convertDbTypeToSqlType(typeName);
                callParameter.setSqlType(sqlType);
                callParameter.setClassName(ColumnType.convertSqlTypeToClass(sqlType).getName());

                int characterMaxLength = rs.getInt("CHARACTER_MAXIMUM_LENGTH");
                int numericPrecision = rs.getInt("NUMERIC_PRECISION");
                callParameter.setPrecision(numericPrecision > 0 ? numericPrecision : characterMaxLength);
                callParameter.setScale(rs.getInt("NUMERIC_SCALE"));

                callParameter.setIndex(count++);
                allParams.add(callParameter);
            } while (rs.next() && rs.getString("SPECIFIC_SCHEMA").equals(specificSchema));
        }
        cps.close();
        rs.close();
    }

    /**
     * When it is an Oracle function, return the number of placeholder type parameters in SQL
     * @return placeholderParams.size
     */
    public int getParameterCount() {
        return placeholderParams != null ? placeholderParams.size() : 0;
    }

    public CallParameter getParam(int index) throws SQLException {
        if (index < 1 || index > placeholderParams.size()) {
            throw new SQLException("invalid parameter index " + index);
        }
        readMetadataFromDbIfRequired();
        return allParams.get(placeholderParams.get(index - 1).getIndex());
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

    public boolean hasEmptyPlaceholderParams() {
        return this.placeholderParams == null || this.placeholderParams.isEmpty();
    }

    public CallParameter getPlaceholderParam(int index) {
        if (hasEmptyPlaceholderParams()) {
            return null;
        }
        return placeholderParams.get(index);
    }
}
