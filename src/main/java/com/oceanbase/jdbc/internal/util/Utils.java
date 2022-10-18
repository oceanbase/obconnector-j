/**
 * OceanBase Client for Java
 * <p>
 * Copyright (c) 2012-2014 Monty Program Ab.
 * Copyright (c) 2015-2020 MariaDB Corporation Ab.
 * Copyright (c) 2021 OceanBase.
 * <p>
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 * <p>
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 * <p>
 * You should have received a copy of the GNU Lesser General Public License along
 * with this library; if not, write to Monty Program Ab info@montyprogram.com.
 * <p>
 * This particular MariaDB Client for Java file is work
 * derived from a Drizzle-JDBC. Drizzle-JDBC file which is covered by subject to
 * the following copyright and notice provisions:
 * <p>
 * Copyright (c) 2009-2011, Marcus Eriksson
 * <p>
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 * Redistributions of source code must retain the above copyright notice, this list
 * of conditions and the following disclaimer.
 * <p>
 * Redistributions in binary form must reproduce the above copyright notice, this
 * list of conditions and the following disclaimer in the documentation and/or
 * other materials provided with the distribution.
 * <p>
 * Neither the name of the driver nor the names of its contributors may not be
 * used to endorse or promote products derived from this software without specific
 * prior written permission.
 * <p>
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS  AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 * NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
 * OF SUCH DAMAGE.
 */
package com.oceanbase.jdbc.internal.util;

import java.io.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;

import javax.net.SocketFactory;

import com.oceanbase.jdbc.OceanBaseStatement;
import com.oceanbase.jdbc.TnsDaemon;
import com.oceanbase.jdbc.UrlParser;
import com.oceanbase.jdbc.internal.com.send.parameters.ParameterHolder;
import com.oceanbase.jdbc.internal.failover.FailoverProxy;
import com.oceanbase.jdbc.internal.failover.impl.AuroraListener;
import com.oceanbase.jdbc.internal.failover.impl.MastersFailoverListener;
import com.oceanbase.jdbc.internal.failover.impl.MastersSlavesListener;
import com.oceanbase.jdbc.internal.failover.utils.ConfigParser;
import com.oceanbase.jdbc.internal.io.LruTraceCache;
import com.oceanbase.jdbc.internal.io.socket.SocketHandlerFunction;
import com.oceanbase.jdbc.internal.io.socket.SocketUtility;
import com.oceanbase.jdbc.internal.logging.ProtocolLoggingProxy;
import com.oceanbase.jdbc.internal.protocol.AuroraProtocol;
import com.oceanbase.jdbc.internal.protocol.MasterProtocol;
import com.oceanbase.jdbc.internal.protocol.MastersSlavesProtocol;
import com.oceanbase.jdbc.internal.protocol.Protocol;
import com.oceanbase.jdbc.internal.util.pool.GlobalStateInfo;
import com.oceanbase.jdbc.util.ConfigurableSocketFactory;
import com.oceanbase.jdbc.util.Options;

@SuppressWarnings("Annotator")
public class Utils {

  private static final char[] hexArray = "0123456789ABCDEF".toCharArray();
  private static final Pattern IP_V4 =
      Pattern.compile(
          "^(([1-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\\.){1}"
              + "(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\\.){2}"
              + "([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])$");
  private static final Pattern IP_V6 = Pattern.compile("^[0-9a-fA-F]{1,4}(:[0-9a-fA-F]{1,4}){7}$");
  private static final Pattern IP_V6_COMPRESSED =
      Pattern.compile(
          "^(([0-9A-Fa-f]{1,4}(:[0-9A-Fa-f]{1,4}){0,5})?)"
              + "::(([0-9A-Fa-f]{1,4}(:[0-9A-Fa-f]{1,4}){0,5})?)$");
  private static final SocketHandlerFunction socketHandler;
  public static TnsDaemon tnsDaemon;

  static {
    SocketHandlerFunction init;
    try {
      init = SocketUtility.getSocketHandler();
    } catch (Throwable t) {
      SocketHandlerFunction defaultSocketHandler =
          (options, host) -> Utils.standardSocket(options, host);
      init = defaultSocketHandler;
    }
    socketHandler = init;
    String tnsDaemonFlag = System.getProperty("oceanbase.tns_admin_deamon");
    String tnsDaemonFlagEnv = System.getenv("OCEANBASE_TNS_ADMIN_DEAMON");
    if(tnsDaemonFlag == null) {
        tnsDaemonFlag = tnsDaemonFlagEnv;
    }
    if(tnsDaemonFlag == null || (tnsDaemonFlag !=null  && tnsDaemonFlag.equalsIgnoreCase("FALSE"))) {
      tnsDaemon = null;
    } else {
        // Initialize the config file first of all
        try {
            ConfigParser.TnsFileInfo tnsFileInfo = ConfigParser.getTnsFilePath();
            String filePath = tnsFileInfo.path + "/" + tnsFileInfo.name;
            File file = new File(filePath);
            Reader reader = new InputStreamReader(new FileInputStream(file));
            ConfigParser.getLoadBalanceInfosFromReader(reader);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        tnsDaemon = new TnsDaemon();
        tnsDaemon.start();
    }
  }

  /**
   * Use standard socket implementation.
   *
   * @param options url options
   * @param host    host to connect
   * @return socket
   * @throws IOException in case of error establishing socket.
   */
  public static Socket standardSocket(Options options, String host) throws IOException {
    SocketFactory socketFactory;
    String socketFactoryName = options.socketFactory;
    if (socketFactoryName != null) {
      try {
        @SuppressWarnings("unchecked")
        Class<? extends SocketFactory> socketFactoryClass =
            (Class<? extends SocketFactory>) Class.forName(socketFactoryName);
        if (socketFactoryClass != null) {
          Constructor<? extends SocketFactory> constructor = socketFactoryClass.getConstructor();
          socketFactory = constructor.newInstance();
          if (socketFactoryClass.isInstance(ConfigurableSocketFactory.class)) {
            ((ConfigurableSocketFactory) socketFactory).setConfiguration(options, host);
          }
          return socketFactory.createSocket();
        }
      } catch (Exception exp) {
        throw new IOException(
            "Socket factory failed to initialized with option \"socketFactory\" set to \""
                + options.socketFactory
                + "\"",
            exp);
      }
    }
    socketFactory = SocketFactory.getDefault();
    return socketFactory.createSocket();
  }

  public static Socket socksSocket(Options options) {
    String socksProxyHost = options.socksProxyHost;
    int socksProxyPort = options.socksProxyPort;
    return new Socket(new java.net.Proxy(java.net.Proxy.Type.SOCKS, new InetSocketAddress(socksProxyHost,
        socksProxyPort)));
  }

  /**
   * Escape String.
   *
   * @param value value to escape
   * @param noBackslashEscapes must backslash be escaped
   * @return escaped string.
   */
  public static String escapeString(String value, boolean noBackslashEscapes) {
    if (!value.contains("'")) {
      if (noBackslashEscapes) {
        return value;
      }
      if (!value.contains("\\")) {
        return value;
      }
    }
    String escaped = value.replace("'", "''");
    if (noBackslashEscapes) {
      return escaped;
    }
    return escaped.replace("\\", "\\\\");
  }

  /**
   * Encrypts a password.
   *
   * <p>protocol for authentication is like this: 1. Server sends a random array of bytes (the seed)
   * 2. client makes a sha1 digest of the password 3. client hashes the output of 2 4. client
   * digests the seed 5. client updates the digest with the output from 3 6. an xor of the output of
   * 5 and 2 is sent to server 7. server does the same thing and verifies that the scrambled
   * passwords match
   *
   * @param password the password to encrypt
   * @param seed the seed to use
   * @param passwordCharacterEncoding password character encoding
   * @return a scrambled password
   * @throws NoSuchAlgorithmException if SHA1 is not available on the platform we are using
   * @throws UnsupportedEncodingException if passwordCharacterEncoding is not a valid charset name
   */
  public static byte[] encryptPassword(
      final String password, final byte[] seed, String passwordCharacterEncoding)
      throws NoSuchAlgorithmException, UnsupportedEncodingException {

    if (password == null || password.isEmpty()) {
      return new byte[0];
    }

    final MessageDigest messageDigest = MessageDigest.getInstance("SHA-1");
    byte[] bytePwd;
    if (passwordCharacterEncoding != null && !passwordCharacterEncoding.isEmpty()) {
      bytePwd = password.getBytes(passwordCharacterEncoding);
    } else {
      bytePwd = password.getBytes();
    }

    final byte[] stage1 = messageDigest.digest(bytePwd);
    messageDigest.reset();

    final byte[] stage2 = messageDigest.digest(stage1);
    messageDigest.reset();

    messageDigest.update(seed);
    messageDigest.update(stage2);

    final byte[] digest = messageDigest.digest();
    final byte[] returnBytes = new byte[digest.length];
    for (int i = 0; i < digest.length; i++) {
      returnBytes[i] = (byte) (stage1[i] ^ digest[i]);
    }
    return returnBytes;
  }

  /**
   * Copies the original byte array content to a new byte array. The resulting byte array is always
   * "length" size. If length is smaller than the original byte array, the resulting byte array is
   * truncated. If length is bigger than the original byte array, the resulting byte array is filled
   * with zero bytes.
   *
   * @param orig the original byte array
   * @param length how big the resulting byte array will be
   * @return the copied byte array
   */
  public static byte[] copyWithLength(byte[] orig, int length) {
    // No need to initialize with zero bytes, because the bytes are already initialized with that
    byte[] result = new byte[length];
    int howMuchToCopy = length < orig.length ? length : orig.length;
    System.arraycopy(orig, 0, result, 0, howMuchToCopy);
    return result;
  }

  /**
   * Copies from original byte array to a new byte array. The resulting byte array is always
   * "to-from" size.
   *
   * @param orig the original byte array
   * @param from index of first byte in original byte array which will be copied
   * @param to index of last byte in original byte array which will be copied. This can be outside
   *     of the original byte array
   * @return resulting array
   */
  public static byte[] copyRange(byte[] orig, int from, int to) {
    int length = to - from;
    byte[] result = new byte[length];
    int howMuchToCopy = orig.length - from < length ? orig.length - from : length;
    System.arraycopy(orig, from, result, 0, howMuchToCopy);
    return result;
  }

  /**
   * Helper function to replace function parameters in escaped string. 3 functions are handles :
   *
   * <ul>
   *   <li>CONVERT(value, type): replacing SQL_XXX types to convertible type, i.e SQL_BIGINT to
   *       INTEGER
   *   <li>TIMESTAMPDIFF(type, ...): replacing type SQL_TSI_XXX in type with XXX, i.e SQL_TSI_HOUR
   *       with HOUR
   *   <li>TIMESTAMPADD(type, ...): replacing type SQL_TSI_XXX in type with XXX, i.e SQL_TSI_HOUR
   *       with HOUR
   * </ul>
   *
   * <p>caution: this use MariaDB server conversion: 'SELECT CONVERT('2147483648', INTEGER)' will
   * return a BIGINT. MySQL will throw a syntax error.
   *
   * @param functionString input string
   * @param protocol protocol
   * @return unescaped string
   */
  private static String replaceFunctionParameter(String functionString, Protocol protocol) {

    char[] input = functionString.toCharArray();
    StringBuilder sb = new StringBuilder();
    int index;
    for (index = 0; index < input.length; index++) {
      if (input[index] != ' ') {
        break;
      }
    }

    for (;
         ((input[index] >= 'a' && input[index] <= 'z')
             || (input[index] >= 'A' && input[index] <= 'Z'))
             && index < input.length;
         index++) {
      sb.append(input[index]);
    }
    String func = sb.toString().toLowerCase(Locale.ROOT);
    switch (func) {
      case "convert":
        // Handle "convert(value, type)" case
        // extract last parameter, after the last ','
        int lastCommaIndex = functionString.lastIndexOf(',');
        int firstParentheses = functionString.indexOf('(');
        String value = functionString.substring(firstParentheses + 1, lastCommaIndex);
        for (index = lastCommaIndex + 1; index < input.length; index++) {
          if (!Character.isWhitespace(input[index])) {
            break;
          }
        }

        int endParam = index + 1;
        for (; endParam < input.length; endParam++) {
          if ((input[endParam] < 'a' || input[endParam] > 'z')
              && (input[endParam] < 'A' || input[endParam] > 'Z')
              && input[endParam] != '_') {
            break;
          }
        }
        String typeParam = new String(input, index, endParam - index).toUpperCase(Locale.ROOT);
        if (typeParam.startsWith("SQL_")) {
          typeParam = typeParam.substring(4);
        }

        switch (typeParam) {
          case "BOOLEAN":
            return "1=" + value;

          case "BIGINT":
          case "SMALLINT":
          case "TINYINT":
            typeParam = "SIGNED INTEGER";
            break;

          case "BIT":
            typeParam = "UNSIGNED INTEGER";
            break;

          case "BLOB":
          case "VARBINARY":
          case "LONGVARBINARY":
          case "ROWID":
            typeParam = "BINARY";
            break;

          case "NCHAR":
          case "CLOB":
          case "NCLOB":
          case "DATALINK":
          case "VARCHAR":
          case "NVARCHAR":
          case "LONGVARCHAR":
          case "LONGNVARCHAR":
          case "SQLXML":
          case "LONGNCHAR":
            typeParam = "CHAR";
            break;

          case "DOUBLE":
          case "FLOAT":
            if (protocol.isServerMariaDb() || protocol.versionGreaterOrEqual(8, 0, 17)) {
              typeParam = "DOUBLE";
              break;
            }
            return "0.0+" + value;

          case "REAL":
          case "NUMERIC":
            typeParam = "DECIMAL";
            break;

          case "TIMESTAMP":
            typeParam = "DATETIME";
            break;

          default:
            break;
        }
        return new String(input, 0, index)
            + typeParam
            + new String(input, endParam, input.length - endParam);

      case "timestampdiff":
      case "timestampadd":
        // Skip to first parameter
        for (; index < input.length; index++) {
          if (!Character.isWhitespace(input[index]) && input[index] != '(') {
            break;
          }
        }
        if (index < input.length - 8) {
          String paramPrefix = new String(input, index, 8);
          if ("SQL_TSI_".equals(paramPrefix)) {
            return new String(input, 0, index)
                + new String(input, index + 8, input.length - (index + 8));
          }
        }
        return functionString;

      default:
        return functionString;
    }
  }

  // TODO need to check resolveEscapes for Oracle Mode
  private static String resolveEscapes(String escaped, Protocol protocol) throws SQLException {
    if (escaped.charAt(0) != '{' || escaped.charAt(escaped.length() - 1) != '}') {
      throw new SQLException("unexpected escaped string");
    }
    int endIndex = escaped.length() - 1;
    String escapedLower = escaped.toLowerCase(Locale.ROOT);
    if (escaped.startsWith("{fn ")) {
      String resolvedParams = replaceFunctionParameter(escaped.substring(4, endIndex), protocol);
      return nativeSql(resolvedParams, protocol);
    } else if (escapedLower.startsWith("{oj ")) {
      // Outer join
      // the server supports "oj" in any case, even "oJ"
      return nativeSql(escaped.substring(4, endIndex), protocol);
    } else if (escaped.startsWith("{d ")) {
      // date literal
      return escaped.substring(3, endIndex);
    } else if (escaped.startsWith("{t ")) {
      // time literal
      return escaped.substring(3, endIndex);
    } else if (escaped.startsWith("{ts ")) {
      // timestamp literal
      return escaped.substring(4, endIndex);
    } else if (escaped.startsWith("{d'")) {
      // date literal, no space
      return escaped.substring(2, endIndex);
    } else if (escaped.startsWith("{t'")) {
      // time literal
      return escaped.substring(2, endIndex);
    } else if (escaped.startsWith("{ts'")) {
      // timestamp literal
      return escaped.substring(3, endIndex);
    } else if (escaped.startsWith("{call ") || escaped.startsWith("{CALL ")) {
      // We support uppercase "{CALL" only because Connector/J supports it. It is not in the JDBC
      // spec.

      return nativeSql(escaped.substring(1, endIndex), protocol);
    } else if (escaped.startsWith("{escape ")) {
      return escaped.substring(1, endIndex);
    } else if (escaped.startsWith("{?")) {
      // likely ?=call(...)
      return nativeSql(escaped.substring(1, endIndex), protocol);
    } else if (escaped.startsWith("{ ") || escaped.startsWith("{\n")) {
      // Spaces and newlines before keyword, this is not JDBC compliant, however some it works in
      // some drivers,
      // so we support it, too
      for (int i = 2; i < escaped.length(); i++) {
        if (!Character.isWhitespace(escaped.charAt(i))) {
          return resolveEscapes("{" + escaped.substring(i), protocol);
        }
      }
    } else if (escaped.startsWith("{\r\n")) {
      // Spaces and newlines before keyword, this is not JDBC compliant, however some it works in
      // some drivers,
      // so we support it, too
      for (int i = 3; i < escaped.length(); i++) {
        if (!Character.isWhitespace(escaped.charAt(i))) {
          return resolveEscapes("{" + escaped.substring(i), protocol);
        }
      }
    }
    throw new SQLException("unknown escape sequence " + escaped);
  }

  /**
   * Escape sql String.
   *
   * @param sql initial sql
   * @param protocol protocol
   * @return escaped sql string
   * @throws SQLException if escape sequence is incorrect.
   */
  @SuppressWarnings("ConstantConditions")
  public static String nativeSql(String sql, Protocol protocol) throws SQLException {
    if (!sql.contains("{")) {
      return sql;
    }

    StringBuilder escapeSequenceBuf = new StringBuilder();
    StringBuilder sqlBuffer = new StringBuilder();

    char[] charArray = sql.toCharArray();
    char lastChar = 0;
    boolean inQuote = false;
    char quoteChar = 0;
    boolean inComment = false;
    boolean isSlashSlashComment = false;
    int inEscapeSeq = 0;
    boolean isOracleMode = protocol.isOracleMode();

    for (int i = 0; i < charArray.length; i++) {
      char car = charArray[i];
      if ((!isOracleMode) && lastChar == '\\' && !protocol.noBackslashEscapes()) {
        sqlBuffer.append(car);
        // avoid considering escaped backslash as a futur escape character
        lastChar = ' ';
        continue;
      }

      switch (car) {
        case '\'':
        case '"':
        case '`':
          if (!inComment) {
            if (inQuote) {
              if (quoteChar == car) {
                inQuote = false;
              }
            } else {
              inQuote = true;
              quoteChar = car;
            }
          }
          break;

        case '*':
          if (!inQuote && !inComment && lastChar == '/') {
            inComment = true;
            isSlashSlashComment = false;
          }
          break;
        case '/':
        case '-':
          if (!inQuote) {
            if (inComment) {
              if (lastChar == '*' && !isSlashSlashComment) {
                inComment = false;
              } else if (lastChar == car && isSlashSlashComment) {
                inComment = false;
              }
            } else {
              if (lastChar == car) {
                inComment = true;
                isSlashSlashComment = true;
              } else if (lastChar == '*') {
                inComment = true;
                isSlashSlashComment = false;
              }
            }
          }
          break;
        case '\n':
          if (inComment && isSlashSlashComment) {
            // slash-slash and dash-dash comments ends with the end of line
            inComment = false;
          }
          break;
        case '{':
          if (!inQuote && !inComment) {
            inEscapeSeq++;
          }
          break;

        case '}':
          if (!inQuote && !inComment) {
            inEscapeSeq--;
            if (inEscapeSeq == 0) {
              escapeSequenceBuf.append(car);
              sqlBuffer.append(resolveEscapes(escapeSequenceBuf.toString(), protocol));
              escapeSequenceBuf.setLength(0);
              lastChar = car;
              continue;
            }
          }
          break;

        default:
          break;
      }
      lastChar = car;
      if (inEscapeSeq > 0) {
        escapeSequenceBuf.append(car);
      } else {
        sqlBuffer.append(car);
      }
    }
    if (inEscapeSeq > 0) {
      throw new SQLException(
          "Invalid escape sequence , missing closing '}' character in '" + sqlBuffer);
    }
    return sqlBuffer.toString();
  }

  public static String getSQLWithoutCommentMysql(String sql) throws SQLException {
    StringBuilder escapeSequenceBuf = new StringBuilder();
    StringBuilder sqlBuffer = new StringBuilder();

    char[] charArray = sql.toCharArray();
    char lastChar = 0;
    boolean inQuote = false;
    char quoteChar = 0;
    boolean inComment = false;
    boolean isSlashSlashComment = false; // slash and double dash
    boolean isDoubleDashComment = false;
    boolean inDoubleDashComment = false;
    boolean add = false;
    boolean skip = false;
    boolean afterDoubleDash = false;
    for (int i = 0; i < charArray.length; i++) {
      char car = charArray[i];
      if (afterDoubleDash && isDoubleDashComment && inComment) {
        if (car == 127 || car < 33) {
          inDoubleDashComment = true;
          afterDoubleDash = false;
        } else {
          isDoubleDashComment = false;
          afterDoubleDash = false;
          inComment = false;
          inDoubleDashComment = false;
        }
      } else {
        switch (car) {
          case '\'':
          case '"':
          case '`':
            if (!inComment) {
              if (inQuote) {
                if (quoteChar == car) {
                  inQuote = false;
                  add = true;
                }
              } else {
                inQuote = true;
                quoteChar = car;
              }
            }
            break;

          case '*':
            if (!inQuote && !inComment && lastChar == '/') {
              inComment = true;
              isSlashSlashComment = false;
              isDoubleDashComment = false;
            }
            break;
          case '/':
            if (!inQuote) {
              if (inComment) {
                if (lastChar == '*' && !isSlashSlashComment && !isDoubleDashComment) {
                  inComment = false;
                  skip = true;
                }
//                            } else if (lastChar == car && isSlashSlashComment) {
//                                inComment = false;
//                                skip = true;
//                            }
              } else {
                inComment = true;
              }
            }
            break;
          case '-':
            if (!inQuote) {
              if (inComment && !isDoubleDashComment) {
                isDoubleDashComment = false;
              } else if (inComment && isDoubleDashComment && lastChar == '-') {
                afterDoubleDash = true;
                inComment = true;
                isDoubleDashComment = true;
              } else if (!inComment) {
                isDoubleDashComment = true;
                inComment = true;
              }
            }
            break;
          case '#':
            if (!inQuote) {
              if (inComment) {
                isSlashSlashComment = false;
              } else {
                inComment = true;
                isSlashSlashComment = true;
              }
            }
            break;
          case '\n':
            if ((inComment && isSlashSlashComment) || (inComment && isDoubleDashComment && inDoubleDashComment)) {
              // slash-slash and dash-dash comments ends with the end of line
              inComment = false;
              skip = true;
              isDoubleDashComment = false;
              isSlashSlashComment = false;
              inDoubleDashComment = false;
              afterDoubleDash = false;
            } else {
              car = ' ';
            }
            break;
//          case '}':
//          case '{':
//            if (!inComment && !inQuote) {
//              car = ' ';
//            }
//            break;
          default:
            if (!inQuote && !inComment && !isSlashSlashComment && !isDoubleDashComment) {
              add = true;
            }
            break;
        }
      }
      lastChar = car;
      if (i == charArray.length - 1) {
        add = true;
      }
      escapeSequenceBuf.append(car);
      if (skip) {
        escapeSequenceBuf.setLength(0);
        skip = false;
      }
      if (add) {
        sqlBuffer.append(escapeSequenceBuf);
        add = false;
        escapeSequenceBuf.setLength(0);
      }
    }
    return sqlBuffer.toString().trim();
  }

  enum LexState {
    Normal, /* inside  query */
    String, /* inside string */
    SlashStarComment, /* inside slash-star comment */
    Escape, /* found backslash */
    EOLComment, /* # comment, or // comment, or -- comment */
    Backtick /* found backtick */,
    NameBinding /* in name binding such as :name */
  }

  public static String trimSQLString(String queryString, boolean noBackslashEscapes, boolean isOracleMode) {
    return trimSQLString(queryString, noBackslashEscapes, isOracleMode, false);
  }

  /**
   *  Adjust the string to have no comments and the name binding  parameters to ? format
   * @param queryString  original sql string
   * @param noBackslashEscapes  must backslash be escaped
   * @param isOracleMode  oracle mode
   * @param skipComment  don not remove the comment
   * @return
   */
  public static String trimSQLString(String queryString, boolean noBackslashEscapes, boolean isOracleMode, boolean skipComment) {
    return trimSQLStringInternal(queryString, noBackslashEscapes, isOracleMode, skipComment)[0];
  }

  public static String[] trimSQLStringInternal(String queryString, boolean noBackslashEscapes, boolean isOracleMode, boolean skipComment) {
    char[] query = queryString.toCharArray();
    int queryLength = query.length;
    int parameterCount = 0;
    StringBuilder trimedSqlString = new StringBuilder();
    StringBuilder paramSb = new StringBuilder();
    boolean multipleQueriesPrepare = true;

    LexState state = LexState.Normal;
    char lastChar = '\0';
    boolean includeCurChar = false;
    boolean singleQuotes = false;
    int selectEndPos = -1;
    int whereEndPos = -1;
    int lastParamPos = 0;
    int commentStart = 0;
    boolean slashEnd = false;
    boolean semicolonEnd = false;
    boolean nameBindingEnd = false;

    for (int i = 0; i < queryLength; i++) {
      if (i == queryLength - 1 && state == LexState.NameBinding && isOracleMode) {
        nameBindingEnd = true;
        includeCurChar = true;
      }

      char car = query[i];
      if (state == LexState.Escape && !(car == '\'' && singleQuotes || car == '"' && !singleQuotes)) {
        state = LexState.String;
        lastChar = car;
        continue;
      }

      switch (car) {
        case '*':
          if (state == LexState.Normal && lastChar == '/') {
            state = LexState.SlashStarComment;
            commentStart = i - 1;
          }
          break;

        case '/':
          if (state == LexState.SlashStarComment && lastChar == '*') {
            state = LexState.Normal;
            slashEnd = true;
            if (skipComment) {
              trimedSqlString.append(queryString.substring(lastParamPos, i + 1));
            } else if (commentStart != 0) {
              trimedSqlString.append(queryString.substring(lastParamPos, commentStart));
            }
            lastParamPos = i + 1;
          } else if (state == LexState.Normal && lastChar == '/') {
            if (slashEnd) {
              slashEnd = false;
              break;
            }
            state = LexState.EOLComment;
            lastParamPos = i + 1;
            commentStart = i - 1;
          }
          break;

        case '#':
          if (state == LexState.Normal && !isOracleMode) {
            state = LexState.EOLComment;
            commentStart = i;
          }
          break;

        case '-':
          if (state == LexState.Normal && lastChar == '-') {
            state = LexState.EOLComment;
            multipleQueriesPrepare = false;
            commentStart = i - 1;
          }
          break;

        case '\n':
          if (state == LexState.EOLComment) {
            multipleQueriesPrepare = true;
            state = LexState.Normal;
            if (skipComment) {
              trimedSqlString.append(queryString.substring(lastParamPos, i + 1));
            } else if (commentStart != 0) {
              trimedSqlString.append(queryString.substring(lastParamPos, commentStart));
            }
            lastParamPos = i + 1;
          } else if (state == LexState.NameBinding && isOracleMode) {
            nameBindingEnd = true;
            includeCurChar = false;
          }
          break;

        case '"':
          if (state == LexState.Normal) {
            state = LexState.String;
            singleQuotes = false;
          } else if (state == LexState.String && !singleQuotes) {
            state = LexState.Normal;
          } else if (state == LexState.Escape && !singleQuotes) {
            state = LexState.String;
          }
          break;

        case '\'':
          if (state == LexState.Normal) {
            state = LexState.String;
            singleQuotes = true;
          } else if (state == LexState.String && singleQuotes) {
            state = LexState.Normal;
          } else if (state == LexState.Escape && singleQuotes) {
            state = LexState.String;
          }
          break;

        case '\\':
          if (noBackslashEscapes) {
            break;
          }
          if (state == LexState.String && !isOracleMode) {
            state = LexState.Escape;
          }
          break;

        case ';':
          if (state == LexState.Normal) {
            semicolonEnd = true;
            multipleQueriesPrepare = false;
          } else if (state == LexState.NameBinding && isOracleMode) {
            nameBindingEnd = true;
            includeCurChar = false;
          }
          break;

        case '?':
          if (state == LexState.Normal) {
            trimedSqlString.append(queryString.substring(lastParamPos, i));
            trimedSqlString.append("?");
            lastParamPos = i + 1;
            parameterCount++;
          }
          break;

        case '`':
          if (state == LexState.Backtick) {
            state = LexState.Normal;
          } else if (state == LexState.Normal) {
            state = LexState.Backtick;
          }
          break;

        case ':':
          if (state == LexState.Normal && isOracleMode) {
            state = LexState.NameBinding;
            if (semicolonEnd && ((int) car >= 40)) { // do as normal
              semicolonEnd = false;
              multipleQueriesPrepare = true;
            }
          }
          break;

        case '=':
          if (isOracleMode) {
            if (state == LexState.NameBinding) {
              state = LexState.Normal; // := is not name binding
              paramSb.setLength(0);
            } else if (state == LexState.Normal && semicolonEnd && ((int) car >= 40)) { // do as normal
              semicolonEnd = false;
              multipleQueriesPrepare = true;
            }
          }
          break;

        case ' ':
        case ',':
        case ')':
        case '}':
          if (state == LexState.NameBinding && isOracleMode) {
            nameBindingEnd = true;
            includeCurChar = false;
          }
          break;

        default:
          // multiple queries
          if (state == LexState.Normal) { 
            if (semicolonEnd && ((int) car >= 40)) {
              semicolonEnd = false;
              multipleQueriesPrepare = true;
            }
            if (selectEndPos == -1 && (car == 't' || car == 'T') && i >= 5 && queryString.substring(i - 5, i + 1).equalsIgnoreCase("select")) {
              selectEndPos = i;
            } else if (whereEndPos == -1 && (car == 'e' || car == 'E') && i >= 4 && queryString.substring(i - 4, i + 1).equalsIgnoreCase("where")) {
                whereEndPos = i;
            }
          }
          break;
      }

      if (state == LexState.NameBinding && isOracleMode) {
        if (!nameBindingEnd) {
          if ((int) car >= 40) {
            paramSb.append(car);
          }
        } else {
          if (!includeCurChar) {
            String tmp = paramSb.toString();
            if(tmp != null) {
              int index = tmp.indexOf('.');
              if(index != -1) {
                  trimedSqlString.append(queryString.substring(lastParamPos, i));
              } else {
                trimedSqlString.append(queryString.substring(lastParamPos, i - paramSb.length()));
                trimedSqlString.append("?");
                parameterCount++;
              }
            }
            lastParamPos = i;
          } else {
            paramSb.append(car);
            String tmp = paramSb.toString();
            if(tmp != null) {
              int index = tmp.indexOf('.');
              if(index != -1) {
                  trimedSqlString.append(queryString.substring(lastParamPos, i + 1));
              } else {
                trimedSqlString.append(queryString.substring(lastParamPos, i - paramSb.length() + 1));
                trimedSqlString.append("?");
                parameterCount++;
              }
            }
            lastParamPos = i + 1;
            includeCurChar = false;
          }
          paramSb.setLength(0);
          nameBindingEnd = false;
          state = LexState.Normal;
        }
      }
      lastChar = car;
    }

    if (lastParamPos == 0) {
      trimedSqlString.append(queryString);
    } else {
      trimedSqlString.append(queryString.substring(lastParamPos, queryLength));
    }

    return new String[]{trimedSqlString.toString(), String.valueOf(parameterCount), String.valueOf(selectEndPos), String.valueOf(whereEndPos)};
  }

  private static int nextCharIndex(int startPos, int stopPos, String searchedString,
                                     String leftMarks, String rightMarks) {
    if (searchedString == null) {
      return -1;
    }
    int searchStringLength = searchedString.length();

    if (startPos >= searchStringLength) {
      return -1;
    }

    char charVal0 = Character.MIN_VALUE;
    char charVal1 = searchedString.charAt(startPos);
    char charVal2 = startPos + 1 < searchStringLength ? searchedString.charAt(startPos + 1)
            : Character.MIN_VALUE;

    for (int i = startPos; i <= stopPos; i++) {
      charVal0 = charVal1;
      charVal1 = charVal2;
      charVal2 = i + 2 < searchStringLength ? searchedString.charAt(i + 2) : Character.MIN_VALUE;

      int markerIndex = -1;

      if ((markerIndex = leftMarks.indexOf(charVal0)) != -1) {
        int nestedMarkersCount = 0;
        char openingMarker = charVal0;
        char closingMarker = rightMarks.charAt(markerIndex);
        while (++i <= stopPos
                && ((charVal0 = searchedString.charAt(i)) != closingMarker || nestedMarkersCount != 0)) {
          if (charVal0 == openingMarker) {
            nestedMarkersCount++;
          } else if (charVal0 == closingMarker) {
            nestedMarkersCount--;
          } else if ( charVal0 == '\\') {
            i++;
          }
        }
        charVal1 = i + 1 < searchStringLength ? searchedString.charAt(i + 1) : Character.MIN_VALUE;
        charVal2 = i + 2 < searchStringLength ? searchedString.charAt(i + 2) : Character.MIN_VALUE;
      } else {
        return i;
      }
    }
    return -1;
  }
  
  public static int nextDelimiterPos( String stringToSearched,int startingPosition, String stringToSearch,
                                      String leftMarks, String rightMarks) {
    if (stringToSearched == null || stringToSearch == null) {
      return -1;
    }
    int stringToSearchedLen = stringToSearched.length();
    int stringToSearchLen   = stringToSearch.length();
    int stopSearchingAt = stringToSearchedLen - stringToSearchLen;
    if (startingPosition > stopSearchingAt || stringToSearchLen == 0) {
      return -1;
    }
    char firstUc = Character.toUpperCase(stringToSearch.charAt(0));
    char firstLc = Character.toLowerCase(stringToSearch.charAt(0));
    for (int i = startingPosition; i <= stopSearchingAt; i++) {
      i = nextCharIndex(i, stopSearchingAt, stringToSearched, leftMarks, rightMarks);
      if (i == -1) {
        return -1;
      }
      char c = stringToSearched.charAt(i);
      if((c == firstLc || c == firstUc) && stringToSearched.toUpperCase(Locale.ROOT).substring(i).startsWith(stringToSearch)) {
        return i;
      }
    }
    return -1;
  }
  
  public static  List<ParsedCallParameters> argumentsSplit(String arguments, String delimiter, String markers,
                                                           String markerCloses) {
    if (arguments == null) {
      return null;
    }
    arguments = arguments.substring(arguments.indexOf("(") + 1,arguments.lastIndexOf(")"));
    List<ParsedCallParameters> retList = new ArrayList<>();
    boolean trim = true;
    if (delimiter == null) {
      throw new IllegalArgumentException();
    }
    int delimPos = 0;
    int currentPos = 0;
    while((delimPos = nextDelimiterPos(arguments,currentPos, delimiter, markers,
            markerCloses)) != -1) {
      String token = arguments.substring(currentPos,delimPos);
      if (trim) {
        token = token.trim();
      }
      if(token.startsWith(":") || token.startsWith("?")) {
        retList.add(new ParsedCallParameters(true,token));
      } else {
        retList.add(new ParsedCallParameters(false,token));
      }
      currentPos = delimPos + 1;
    }
    if (currentPos < arguments.length()) {
      String token = arguments.substring(currentPos);

      if (trim) {
        token = token.trim();
      }
      if(token.startsWith(":") || token.startsWith("?")) {
        retList.add(new ParsedCallParameters(true,token));
      } else {
        retList.add(new ParsedCallParameters(false,token));
      }
    }
    return retList;
  }

  public static int getStatementType(String queryString) {
    char[] query = queryString.toCharArray();
    int index = 0;
    while (index < query.length && (query[index] == ' ' || query[index] == '\r'  || query[index] == '\n' || query[index] == '\t')) {
      index++;
    }

    String str = queryString.substring(index);
    if (str.substring(0, 4).equalsIgnoreCase("with")) {
      return OceanBaseStatement.STMT_SELECT;
    } else if (str.substring(0, 6).equalsIgnoreCase("select")) {
      return OceanBaseStatement.STMT_SELECT;
    } else if (str.substring(0, 6).equalsIgnoreCase("update")) {
      return OceanBaseStatement.STMT_UPDATE;
    } else if (str.substring(0, 6).equalsIgnoreCase("delete")) {
      return OceanBaseStatement.STMT_DELETE;
    } else if (str.substring(0, 6).equalsIgnoreCase("insert")) {
      return OceanBaseStatement.STMT_INSERT;
    } else if (str.substring(0, 6).equalsIgnoreCase("create")) {
      return OceanBaseStatement.STMT_CREATE;
    } else if (str.substring(0, 4).equalsIgnoreCase("drop")) {
      return OceanBaseStatement.STMT_DROP;
    } else if (str.substring(0, 5).equalsIgnoreCase("alter")) {
      return OceanBaseStatement.STMT_ALTER;
    } else if (str.substring(0, 5).equalsIgnoreCase("begin")) {
      return OceanBaseStatement.STMT_BEGIN;
    } else if (str.substring(0, 7).equalsIgnoreCase("declare")) {
      return OceanBaseStatement.STMT_DECLARE;
    } else if (str.substring(0, 4).equalsIgnoreCase("call")) {
      return OceanBaseStatement.STMT_CALL;
    } else {
      return OceanBaseStatement.STMT_UNKNOWN;
    }
  }

  /**
   * Retrieve protocol corresponding to the failover options. if no failover option, protocol will
   * not be proxied. if a failover option is precised, protocol will be proxied so that any
   * connection error will be handle directly.
   *
   * @param urlParser urlParser corresponding to connection url string.
   * @param globalInfo global variable information
   * @return protocol
   * @throws SQLException if any error occur during connection
   */
  public static Protocol retrieveProxy(final UrlParser urlParser, final GlobalStateInfo globalInfo)
      throws SQLException {
    final ReentrantLock lock = new ReentrantLock();
    boolean tnsFlag = true;
    if(tnsFlag) {

    }
    final LruTraceCache traceCache =
        urlParser.getOptions().enablePacketDebug ? new LruTraceCache() : null;
    Protocol protocol;
    switch (urlParser.getHaMode()) {
      case AURORA:
        return getProxyLoggingIfNeeded(
            urlParser,
            (Protocol)
                Proxy.newProxyInstance(
                    AuroraProtocol.class.getClassLoader(),
                    new Class[]{Protocol.class},
                    new FailoverProxy(
                        new AuroraListener(urlParser, globalInfo), lock, traceCache)));
      case REPLICATION:
        return getProxyLoggingIfNeeded(
            urlParser,
            (Protocol)
                Proxy.newProxyInstance(
                    MastersSlavesProtocol.class.getClassLoader(),
                    new Class[]{Protocol.class},
                    new FailoverProxy(
                        new MastersSlavesListener(urlParser, globalInfo), lock, traceCache)));
      case LOADBALANCE:
      case SEQUENTIAL:
        return getProxyLoggingIfNeeded(
            urlParser,
            (Protocol)
                Proxy.newProxyInstance(
                    MasterProtocol.class.getClassLoader(),
                    new Class[]{Protocol.class},
                    new FailoverProxy(
                        new MastersFailoverListener(urlParser, globalInfo), lock, traceCache)));
      default:
        protocol =
            getProxyLoggingIfNeeded(
                urlParser, new MasterProtocol(urlParser, globalInfo, lock, traceCache));
        protocol.connectWithoutProxy();
        return protocol;
    }
  }

  private static Protocol getProxyLoggingIfNeeded(UrlParser urlParser, Protocol protocol) {
    if (urlParser.getOptions().profileSql
        || urlParser.getOptions().slowQueryThresholdNanos != null) {
      return (Protocol)
          Proxy.newProxyInstance(
              MasterProtocol.class.getClassLoader(),
              new Class[]{Protocol.class},
              new ProtocolLoggingProxy(protocol, urlParser.getOptions()));
    }
    return protocol;
  }

  /**
   * Get timezone from Id. This differ from java implementation : by default, if timezone Id is
   * unknown, java return GMT timezone. GMT will be return only if explicitly asked.
   *
   * @param id timezone id
   * @return timezone.
   * @throws SQLException if no timezone is found for this Id
   */
  public static TimeZone getTimeZone(String id) throws SQLException {
    TimeZone tz = TimeZone.getTimeZone(id);

    // Validate the timezone ID. JDK maps invalid timezones to GMT
    if ("GMT".equals(tz.getID()) && !"GMT".equals(id)) {
      throw new SQLException("invalid timezone id '" + id + "'");
    }
    return tz;
  }

  /**
   * Create socket accordingly to options.
   *
   * @param options Url options
   * @param host hostName ( mandatory only for named pipe)
   * @return a nex socket
   * @throws IOException if connection error occur
   */
  public static Socket createSocket(Options options, String host) throws IOException {
    return socketHandler.apply(options, host);
  }

  /**
   * Hexdump.
   *
   * @param bytes byte arrays
   * @return String
   */
  public static String hexdump(byte[]... bytes) {
    return hexdump(Integer.MAX_VALUE, 0, Integer.MAX_VALUE, bytes);
  }

  /**
   * Hexdump. Multiple byte arrays will be combined
   *
   * <p>String output example :
   *
   * <pre>{@code
   * +--------------------------------------------------+
   * |  0  1  2  3  4  5  6  7   8  9  a  b  c  d  e  f |
   * +--------------------------------------------------+------------------+
   * | 11 00 00 02 00 00 00 02  40 00 00 00 08 01 06 05 | ........@....... |
   * | 74 65 73 74 6A                                   | testj            |
   * +--------------------------------------------------+------------------+
   * }</pre>
   *
   * @param maxQuerySizeToLog max log size
   * @param offset offset of last byte array
   * @param length length of last byte array
   * @param byteArr byte arrays. if many, only the last may have offset and size limitation others
   *     will be displayed completely.
   * @return String
   */
  public static String hexdump(int maxQuerySizeToLog, int offset, int length, byte[]... byteArr) {
    switch (byteArr.length) {
      case 0:
        return "";

      case 1:
        byte[] bytes = byteArr[0];
        if (bytes.length <= offset) {
          return "";
        }
        int dataLength = Math.min(maxQuerySizeToLog, Math.min(bytes.length - offset, length));

        StringBuilder outputBuilder = new StringBuilder(dataLength * 5);
        outputBuilder.append("\n");
        writeHex(bytes, offset, dataLength, outputBuilder);
        return outputBuilder.toString();

      default:
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try {
          outputStream.write(byteArr[0]);
          outputStream.write(byteArr[1], offset, Math.min(length, byteArr[1].length));
          for (int i = 2; i < byteArr.length; i++) {
            outputStream.write(byteArr[i]);
          }
        } catch (IOException ioe) {
          // eat
        }

        byte[] concat = outputStream.toByteArray();
        if (concat.length <= offset) {
          return "";
        }

        int stlength = Math.min(maxQuerySizeToLog, outputStream.size());

        StringBuilder out = new StringBuilder(stlength * 3 + 80);
        out.append("\n");
        writeHex(outputStream.toByteArray(), 0, outputStream.size(), out);
        return out.toString();
    }
  }

  /**
   * Write bytes/hexadecimal value of a byte array to a StringBuilder.
   *
   * <p>String output example :
   *
   * <pre>{@code
   * +--------------------------------------------------+
   * |  0  1  2  3  4  5  6  7   8  9  a  b  c  d  e  f |
   * +--------------------------------------------------+------------------+
   * | 5F 00 00 00 03 73 65 74  20 61 75 74 6F 63 6F 6D | _....set autocom |
   * | 6D 69 74 3D 31 2C 20 73  65 73 73 69 6F 6E 5F 74 | mit=1, session_t |
   * | 72 61 63 6B 5F 73 63 68  65 6D 61 3D 31 2C 20 73 | rack_schema=1, s |
   * | 71 6C 5F 6D 6F 64 65 20  3D 20 63 6F 6E 63 61 74 | ql_mode = concat |
   * | 28 40 40 73 71 6C 5F 6D  6F 64 65 2C 27 2C 53 54 | (@@sql_mode,',ST |
   * | 52 49 43 54 5F 54 52 41  4E 53 5F 54 41 42 4C 45 | RICT_TRANS_TABLE |
   * | 53 27 29                                         | S')              |
   * +--------------------------------------------------+------------------+
   * }</pre>
   *
   * @param bytes byte array
   * @param offset offset
   * @param dataLength byte length to write
   * @param outputBuilder string builder
   */
  private static void writeHex(
      byte[] bytes, int offset, int dataLength, StringBuilder outputBuilder) {

    if (bytes == null || bytes.length == 0) {
      return;
    }

    char[] hexaValue = new char[16];
    hexaValue[8] = ' ';

    int pos = offset;
    int posHexa = 0;

    outputBuilder.append(
        "+--------------------------------------------------+\n"
            + "|  0  1  2  3  4  5  6  7   8  9  a  b  c  d  e  f |\n"
            + "+--------------------------------------------------+------------------+\n| ");

    while (pos < dataLength + offset) {
      int byteValue = bytes[pos] & 0xFF;
      outputBuilder
          .append(hexArray[byteValue >>> 4])
          .append(hexArray[byteValue & 0x0F])
          .append(" ");

      hexaValue[posHexa++] = (byteValue > 31 && byteValue < 127) ? (char) byteValue : '.';

      if (posHexa == 8) {
        outputBuilder.append(" ");
      }
      if (posHexa == 16) {
        outputBuilder.append("| ").append(hexaValue).append(" |\n");
        if (pos + 1 != dataLength + offset) outputBuilder.append("| ");
        posHexa = 0;
      }
      pos++;
    }

    int remaining = posHexa;
    if (remaining > 0) {
      if (remaining < 8) {
        for (; remaining < 8; remaining++) {
          outputBuilder.append("   ");
        }
        outputBuilder.append(" ");
      }

      for (; remaining < 16; remaining++) {
        outputBuilder.append("   ");
      }

      for (; posHexa < 16; posHexa++) {
        hexaValue[posHexa] = ' ';
      }

      outputBuilder.append("| ").append(hexaValue).append(" |\n");
    }
    outputBuilder.append(
        "+--------------------------------------------------+------------------+\n");
  }

  private static String getHex(final byte[] raw) {
    final StringBuilder hex = new StringBuilder(2 * raw.length);
    for (final byte b : raw) {
      hex.append(hexArray[(b & 0xF0) >> 4]).append(hexArray[(b & 0x0F)]);
    }
    return hex.toString();
  }

  public static String byteArrayToHexString(final byte[] bytes) {
    return (bytes != null) ? getHex(bytes) : "";
  }

  /**
   * Convert int value to hexadecimal String.
   *
   * @param value value to transform
   * @return Hexadecimal String value of integer.
   */
  public static String intToHexString(final int value) {
    final StringBuilder hex = new StringBuilder(8);
    int offset = 24;
    byte b;
    boolean nullEnd = false;
    while (offset >= 0) {
      b = (byte) (value >> offset);
      offset -= 8;
      if (b != 0 || nullEnd) {
        nullEnd = true;
        hex.append(hexArray[(b & 0xF0) >> 4]).append(hexArray[(b & 0x0F)]);
      }
    }
    return hex.toString();
  }

  /**
   * Parse the option "sessionVariable" to ensure having no injection. semi-column not in string
   * will be replaced by comma.
   *
   * @param sessionVariable option value
   * @return parsed String
   */
  public static String parseSessionVariables(String sessionVariable) {
    StringBuilder out = new StringBuilder();
    StringBuilder sb = new StringBuilder();
    Parse state = Parse.Normal;
    boolean iskey = true;
    boolean singleQuotes = true;
    boolean first = true;
    String key = null;

    char[] chars = sessionVariable.toCharArray();

    for (char car : chars) {

      if (state == Parse.Escape) {
        sb.append(car);
        state = singleQuotes ? Parse.Quote : Parse.String;
        continue;
      }

      switch (car) {
        case '"':
          if (state == Parse.Normal) {
            state = Parse.String;
            singleQuotes = false;
          } else if (state == Parse.String && !singleQuotes) {
            state = Parse.Normal;
          }
          break;

        case '\'':
          if (state == Parse.Normal) {
            state = Parse.String;
            singleQuotes = true;
          } else if (state == Parse.String && singleQuotes) {
            state = Parse.Normal;
          }
          break;

        case '\\':
          if (state == Parse.String) {
            state = Parse.Escape;
          }
          break;

        case ';':
        case ',':
          if (state == Parse.Normal) {
            if (!iskey) {
              if (!first) {
                out.append(",");
              }
              out.append(key);
              out.append(sb.toString());
              first = false;
            } else {
              key = sb.toString().trim();
              if (!key.isEmpty()) {
                if (!first) {
                  out.append(",");
                }
                out.append(key);
                first = false;
              }
            }
            iskey = true;
            key = null;
            sb = new StringBuilder();
            continue;
          }
          break;

        case '=':
          if (state == Parse.Normal && iskey) {
            key = sb.toString().trim();
            iskey = false;
            sb = new StringBuilder();
          }
          break;

        default:
          // nothing
      }

      sb.append(car);
    }

    if (!iskey) {
      if (!first) {
        out.append(",");
      }
      out.append(key);
      out.append(sb.toString());
    } else {
      String tmpkey = sb.toString().trim();
      if (!tmpkey.isEmpty() && !first) {
        out.append(",");
      }
      out.append(tmpkey);
    }
    return out.toString();
  }

  public static boolean isIPv4(final String ip) {
    return IP_V4.matcher(ip).matches();
  }

  public static boolean isIPv6(final String ip) {
    return IP_V6.matcher(ip).matches() || IP_V6_COMPRESSED.matcher(ip).matches();
  }

  /**
   * Traduce a String value of transaction isolation to corresponding java value.
   *
   * @param txIsolation String value
   * @return java corresponding value (Connection.TRANSACTION_READ_UNCOMMITTED,
   *     Connection.TRANSACTION_READ_COMMITTED, Connection.TRANSACTION_REPEATABLE_READ or
   *     Connection.TRANSACTION_SERIALIZABLE)
   * @throws SQLException if String value doesn't correspond
   *     to @@tx_isolation/@@transaction_isolation possible value
   */
  public static int transactionFromString(String txIsolation) throws SQLException {
    switch (txIsolation) { // tx_isolation
      case "READ-UNCOMMITTED":
        return Connection.TRANSACTION_READ_UNCOMMITTED;

      case "READ-COMMITTED":
        return Connection.TRANSACTION_READ_COMMITTED;

      case "REPEATABLE-READ":
        return Connection.TRANSACTION_REPEATABLE_READ;

      case "SERIALIZABLE":
        return Connection.TRANSACTION_SERIALIZABLE;

      default:
        throw new SQLException("unknown transaction isolation level");
    }
  }

  /**
   * Validate that file name correspond to send query.
   *
   * @param sql sql command
   * @param parameters sql parameter
   * @param fileName server file name
   * @return true if correspond
   */
  public static boolean validateFileName(
      String sql, ParameterHolder[] parameters, String fileName) {
    Pattern pattern =
        Pattern.compile(
            "^(\\s*\\/\\*([^\\*]|\\*[^\\/])*\\*\\/)*\\s*LOAD\\s+DATA\\s+((LOW_PRIORITY|CONCURRENT)\\s+)?LOCAL\\s+INFILE\\s+'"
                + fileName
                + "'",
            Pattern.CASE_INSENSITIVE);
    if (pattern.matcher(sql).find()) {
      return true;
    }

    if (parameters != null) {
      pattern =
          Pattern.compile(
              "^(\\s*\\/\\*([^\\*]|\\*[^\\/])*\\*\\/)*\\s*LOAD\\s+DATA\\s+((LOW_PRIORITY|CONCURRENT)\\s+)?LOCAL\\s+INFILE\\s+\\?",
              Pattern.CASE_INSENSITIVE);
      if (pattern.matcher(sql).find() && parameters.length > 0) {
        return parameters[0].toString().toLowerCase().equals("'" + fileName.toLowerCase() + "'");
      }
    }
    return false;
  }

  private enum Parse {
    Normal,
    String, /* inside string */
    Quote,
    Escape /* found backslash */
  }
  
  private static final char[] DIGITS_LOWER = { '0', '1', '2', '3', '4', '5',
      '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };

  private static final char[] DIGITS_UPPER = { '0', '1', '2', '3', '4', '5',
      '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F' };

  protected static char[] encodeHex(byte[] data, char[] toDigits) {
    int l = data.length;
    char[] out = new char[l << 1];
    for (int i = 0, j = 0; i < l; i++) {
      out[j++] = toDigits[(0xF0 & data[i]) >>> 4];
      out[j++] = toDigits[0x0F & data[i]];
    }
    return out;
  }

  public static String encodeHexStr(byte[] data) {
    return encodeHexStr(data, false); // ob support upper case
  }
  
  public static String encodeHexStr(byte[] data, boolean toLowerCase) {
    return encodeHexStr(data, toLowerCase ? DIGITS_LOWER : DIGITS_UPPER);
  }
  
  protected static String encodeHexStr(byte[] data, char[] toDigits) {
    return new String(encodeHex(data, toDigits));
  }

  public static String toHexString(byte[]data) {
    return encodeHexStr(data);
  }
  
  public static  boolean convertStringToBoolean(String str)  {
    if ((str != null) && (str.length() > 0)) {
      int c = Character.toLowerCase(str.charAt(0));
      return ((c == 't') || (c == 'y') || (c == '1') || str.equals("-1"));
    }
    return false;
  }
}
