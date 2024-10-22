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
package com.oceanbase.jdbc.internal.util.dao;

import java.nio.charset.Charset;
import java.security.Key;
import java.util.ArrayList;
import java.util.List;

public class ClientPrepareResult implements PrepareResult {

    private final String       sql;
    private final List<byte[]> queryParts;
    private final boolean      rewriteType;
    private final int          paramCount;
    private boolean            isQueryMultiValuesRewritable = true;
    private boolean            isQueryMultipleRewritable    = true;

    private ClientPrepareResult(String sql, List<byte[]> queryParts,
                                boolean isQueryMultiValuesRewritable,
                                boolean isQueryMultipleRewritable, boolean rewriteType,
                                boolean hasParam) {
        this.sql = sql;
        this.queryParts = queryParts;
        this.isQueryMultiValuesRewritable = isQueryMultiValuesRewritable;
        this.isQueryMultipleRewritable = isQueryMultipleRewritable;
        if (!hasParam) {
            this.paramCount = 0;
        } else {
            this.paramCount = queryParts.size() - (rewriteType ? 3 : 1);
        }
        this.rewriteType = rewriteType;
    }

    /**
     * Separate query in a String list and set flag isQueryMultipleRewritable. The resulting string
     * list is separed by ? that are not in comments. isQueryMultipleRewritable flag is set if query
     * can be rewrite in one query (all case but if using "-- comment"). example for query : "INSERT
     * INTO tableName(id, name) VALUES (?, ?)" result list will be : {"INSERT INTO tableName(id, name)
     * VALUES (", ", ", ")"}
     *
     * @param queryString query
     * @param noBackslashEscapes escape mode
     * @return ClientPrepareResult
     */
    public static ClientPrepareResult parameterParts(String queryString, boolean noBackslashEscapes, boolean isOracleMode, String encoding) {
      Charset charset = Charset.forName(encoding);
      boolean reWritablePrepare = false;
      boolean multipleQueriesPrepare = true;
      List<byte[]> partList = new ArrayList<>();
      LexState state = LexState.Normal;
      char lastChar = '\0';
      boolean endingSemicolon = false;
      int lastCommentEndStarPos = -2;
      int lastCommentBeginSlashPos = 0;

        boolean singleQuotes = false;
        int lastParameterPosition = 0;

        char[] query = queryString.toCharArray();
        int queryLength = query.length;
        boolean slashend = false;
        boolean endNameBinding = false;
        StringBuilder paramSb = new StringBuilder();
        for (int i = 0; i < queryLength; i++) {
            char car = query[i];
            if (state == LexState.Escape
                    && !((car == '\'' && singleQuotes) || (car == '"' && !singleQuotes))) {
                state = LexState.String;
                lastChar = car;
                continue;
            }
            switch (car) {
                case '*':
                    if (state == LexState.Normal && lastChar == '/' && i > lastCommentEndStarPos + 2 ) {
                        state = LexState.SlashStarComment;
                        lastCommentBeginSlashPos = i - 1;
                    }
                    break;

                case '/':
                    if (state == LexState.SlashStarComment && lastChar == '*' && i > lastCommentBeginSlashPos + 2 ) {
                        state = LexState.Normal;
                        slashend = true;
                        lastCommentEndStarPos = i - 1;
                    } else if (state == LexState.Normal && slashend && lastChar == '/') {
                        slashend = false;
                        break;
                    } else if (state == LexState.Normal && lastChar == '/') {
                        state = LexState.EOLComment;
                    }
                    break;

                case '#':
                    if ((!isOracleMode) && state == LexState.Normal) {
                        state = LexState.EOLComment;
                    }
                    break;

                case '-':
                    if (state == LexState.Normal && lastChar == '-') {
                        state = LexState.EOLComment;
                        multipleQueriesPrepare = false;
                    }
                    break;

                case '\n':
                    if (state == LexState.EOLComment) {
                        multipleQueriesPrepare = true;
                        state = LexState.Normal;
                    }
                    if (isOracleMode) {  // such as where c1 = ?
                        if (state == LexState.NabmeBinding) {
                            endNameBinding = true;
                        }
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
                    if ((!isOracleMode) && state == LexState.String) {
                        state = LexState.Escape;
                    }
                    break;
                case ';':
                    if (isOracleMode) {
                        if (state == LexState.NabmeBinding) {
                            endNameBinding = true;
                        }
                    }
                    if (state == LexState.Normal) {
                        endingSemicolon = true;
                        multipleQueriesPrepare = false;
                    }
                    break;
                case '?':
                    if (state == LexState.Normal) {
                        partList.add(queryString.substring(lastParameterPosition, i).getBytes(charset));
                        lastParameterPosition = i + 1;
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
                    if (isOracleMode) {
                        if (state == LexState.Normal) {
                            state = LexState.NabmeBinding;
                        }
                        if (state == LexState.Normal && endingSemicolon && ((byte) car >= 40)) { // do as normal
                            endingSemicolon = false;
                            multipleQueriesPrepare = true;
                        }
                    }
                    break;
                case '=':
                    if (isOracleMode) {
                        if (state == LexState.NabmeBinding) {
                            state = LexState.Normal; // := is not name binding
                        }
                        if (state == LexState.Normal && endingSemicolon && ((int) car >= 40)) { // do as normal
                            endingSemicolon = false;
                            multipleQueriesPrepare = true;
                        }
                    }
                    break;
                case ' ':
                case ',':
                case ')':
                case '}':
                    if (isOracleMode) {
                        if (state == LexState.NabmeBinding) {
                            endNameBinding = true;
                        }
                    }
                    break;
                default:
                    // multiple queries

                    if (state == LexState.Normal && endingSemicolon && ((int) car >= 40)) {
                        endingSemicolon = false;
                        multipleQueriesPrepare = true;
                    }
                    break;
            }
            if (isOracleMode && state == LexState.NabmeBinding && ((int) car >= 40) && endNameBinding != true ) {
                paramSb.append(car);
            }
            if (isOracleMode && state == LexState.NabmeBinding && (endNameBinding || i == queryLength - 1)) {
                String tmp = paramSb.toString();
                if(tmp.indexOf('.') != -1) {
                  paramSb.setLength(0);
                  state = LexState.Normal;
                } else {
                  partList.add((queryString.substring(lastParameterPosition, i - paramSb.length() )).getBytes(charset));
                  paramSb.setLength(0);
                  state = LexState.Normal;
                  if(endNameBinding) {
                    lastParameterPosition = i;
                  } else {
                    lastParameterPosition = i + 1;
                  }
                  endNameBinding = false;
                }
            }
            lastChar = car;
        }
        if (lastParameterPosition == 0) {
            partList.add(queryString.getBytes(charset));
        } else {
                partList.add(
                        queryString
                                .substring(lastParameterPosition, queryLength)
                                .getBytes(charset));

        }

    return new ClientPrepareResult(
        queryString, partList, reWritablePrepare, multipleQueriesPrepare, false,true);
  }

    /**
     * Valid that query is valid (no ending semi colon, or end-of line comment ).
     *
     * @param queryString query
     * @param noBackslashEscapes escape
     * @return valid flag
     */
    public static boolean canAggregateSemiColon(String queryString, boolean noBackslashEscapes,
                                                boolean isOracleMode) {

        LexState state = LexState.Normal;
        char lastChar = '\0';

        boolean singleQuotes = false;
        boolean endingSemicolon = false;
        char[] query = queryString.toCharArray();
        int lastCommentEndStarPos = -2;
        int lastCommentBeginSlashPos = 0;
        int i = 0;

        for (char car : query) {

            if (state == LexState.Escape
                && !((car == '\'' && singleQuotes) || (car == '"' && !singleQuotes))) {
                state = LexState.String;
                lastChar = car;
                continue;
            }

            switch (car) {
                case '*':
                    if (state == LexState.Normal && lastChar == '/' && i > lastCommentEndStarPos + 2) {
                        state = LexState.SlashStarComment;
                        lastCommentBeginSlashPos = i - 1;
                    }
                    break;

                case '/':
                    if (state == LexState.SlashStarComment && lastChar == '*' && i > lastCommentBeginSlashPos + 2) {
                        state = LexState.Normal;
                        lastCommentEndStarPos = i - 1;
                    }
                    break;

                case '#':
                    if ((!isOracleMode) && state == LexState.Normal) {
                        state = LexState.EOLComment;
                    }
                    break;

                case '-':
                    if (state == LexState.Normal && lastChar == '-') {
                        state = LexState.EOLComment;
                    }
                    break;
                case ';':
                    if (state == LexState.Normal) {
                        endingSemicolon = true;
                    }
                    break;
                case '\n':
                    if (state == LexState.EOLComment) {
                        state = LexState.Normal;
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
                    if ((!isOracleMode) && state == LexState.String) {
                        state = LexState.Escape;
                    }
                    break;
                case '`':
                    if (state == LexState.Backtick) {
                        state = LexState.Normal;
                    } else if (state == LexState.Normal) {
                        state = LexState.Backtick;
                    }
                    break;
                default:
                    // multiple queries
                    if (state == LexState.Normal && endingSemicolon && ((byte) car >= 40)) {
                        endingSemicolon = false;
                    }
                    break;
            }
            i ++ ;
            lastChar = car;
        }
        return state != LexState.EOLComment && !endingSemicolon;
    }

    /**
     * Separate query in a String list and set flag isQueryMultiValuesRewritable The parameters "?"
     * (not in comments) emplacements are to be known.
     *
     * <p>The only rewritten queries follow these notation: INSERT [LOW_PRIORITY | DELAYED |
     * HIGH_PRIORITY] [IGNORE] [INTO] tbl_name [PARTITION (partition_list)] [(col,...)] {VALUES |
     * VALUE} (...) [ ON DUPLICATE KEY UPDATE col=expr [, col=expr] ... ] With expr without parameter.
     *
     * <p>Query with LAST_INSERT_ID() will not be rewritten
     *
     * <p>INSERT ... SELECT will not be rewritten.
     *
     * <p>String list :
     *
     * <ul>
     *   <li>pre value part
     *   <li>After value and first parameter part
     *   <li>for each parameters :
     *       <ul>
     *         <li>part after parameter and before last parenthesis
     *         <li>Last query part
     *       </ul>
     * </ul>
     *
     * <p>example : INSERT INTO TABLE(col1,col2,col3,col4, col5) VALUES (9, ?, 5, ?, 8) ON DUPLICATE
     * KEY UPDATE col2=col2+10
     *
     * <ul>
     *   <li>pre value part : INSERT INTO TABLE(col1,col2,col3,col4, col5) VALUES
     *   <li>after value part : "(9 "
     *   <li>part after parameter 1: ", 5," - ", 5," - ",8)"
     *   <li>last part : ON DUPLICATE KEY UPDATE col2=col2+10
     * </ul>
     *
     * <p>With 2 series of parameters, this query will be rewritten like [INSERT INTO
     * TABLE(col1,col2,col3,col4, col5) VALUES][ (9, param0_1, 5, param0_2, 8)][, (9, param1_1, 5,
     * param1_2, 8)][ ON DUPLICATE KEY UPDATE col2=col2+10]
     *
     * @param queryString query String
     * @param noBackslashEscapes must backslash be escaped.
     * @return List of query part.
     */
    public static ClientPrepareResult rewritableParts(
      String queryString, boolean noBackslashEscapes, boolean isOracleMode,String encoding) {
      Charset charset = Charset.forName(encoding);
    boolean reWritablePrepare = true;
    boolean multipleQueriesPrepare = true;
    List<byte[]> partList = new ArrayList<>();
    LexState state = LexState.Normal;
    KeyState keyState = KeyState.Normal;
    char lastChar = '\0';

    StringBuilder sb = new StringBuilder();

    String preValuePart1 = null;
    String preValuePart2 = null;
    String postValuePart = null;

    boolean singleQuotes = false;

    int isInParenthesis = 0;
    boolean skipChar = false;
    boolean isFirstChar = true;
    boolean isInsert = false;
    boolean semicolon = false;
    boolean hasParam = false;
    boolean endNameBinding = false;
    int     onDuplicateKeyUpdateIndex = -1;
    int lastCommentEndStarPos = -2;
    int lastCommentBeginSlashPos = 0;

    char[] query = queryString.toCharArray();
    int queryLength = query.length;
    for (int i = 0; i < queryLength; i++) {

      char car = query[i];
      if (state == LexState.Escape
          && !((car == '\'' && singleQuotes) || (car == '"' && !singleQuotes))) {
        sb.append(car);
        lastChar = car;
        state = LexState.String;
        continue;
      }

      switch (car) {
        case '*':
          if (state == LexState.Normal && lastChar == '/' && i > lastCommentEndStarPos + 2) {
            state = LexState.SlashStarComment;
            lastCommentBeginSlashPos = i - 1;
          }
          break;
        case '/':
          if (state == LexState.SlashStarComment && lastChar == '*' && i > lastCommentBeginSlashPos + 2) {
            state = LexState.Normal;
            lastCommentEndStarPos = i - 1;
          }
          break;

        case '#':
          if ((!isOracleMode) && state == LexState.Normal) {
            state = LexState.EOLComment;
          }
          break;

        case '-':
          if (state == LexState.Normal && lastChar == '-') {
            state = LexState.EOLComment;
            multipleQueriesPrepare = false;
          }
          break;

        case '\n':
          if (state == LexState.EOLComment) {
            state = LexState.Normal;
          }
            if (isOracleMode) {  // such as where c1 = ?
                if (state == LexState.NabmeBinding) {
                    endNameBinding = true;
                }
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
        case ';':
          if (state == LexState.Normal) {
            semicolon = true;
            multipleQueriesPrepare = false;
          }
            if (isOracleMode) {
                if (state == LexState.NabmeBinding) {
                    endNameBinding = true;
                }
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
          if ((!isOracleMode) && state == LexState.String) {
            state = LexState.Escape;
          }
            break;
        case '?':
          if (state == LexState.Normal) {
            hasParam = true;
            if (preValuePart1 == null) {
              preValuePart1 = sb.toString();
              sb.setLength(0);
            }
            if (preValuePart2 == null) {
              preValuePart2 = sb.toString();
              sb.setLength(0);
            } else {
              if (postValuePart != null) {
                // having parameters after the last ")" of value is not rewritable
                reWritablePrepare = false;

                // add part
                sb.insert(0, postValuePart);
                postValuePart = null;
              }
              partList.add(sb.toString().getBytes(charset));
              sb.setLength(0);
            }

            skipChar = true;
          }
          break;
        case '`':
          if (state == LexState.Backtick) {
            state = LexState.Normal;
          } else if (state == LexState.Normal) {
            state = LexState.Backtick;
          }
          break;

        case 's':
        case 'S':
          if (state == LexState.Normal
              && postValuePart == null
              && queryLength > i + 7
              && (query[i + 1] == 'e' || query[i + 1] == 'E')
              && (query[i + 2] == 'l' || query[i + 2] == 'L')
              && (query[i + 3] == 'e' || query[i + 3] == 'E')
              && (query[i + 4] == 'c' || query[i + 4] == 'C')
              && (query[i + 5] == 't' || query[i + 5] == 'T')) {

            // field/table name might contain 'select'
            if (i > 0 && (query[i - 1] > ' ' && "();><=-+,".indexOf(query[i - 1]) == -1)) {
              break;
            }
            if (query[i + 6] > ' ' && "();><=-+,".indexOf(query[i + 6]) == -1) {
              break;
            }

            // SELECT queries, INSERT FROM SELECT not rewritable
            reWritablePrepare = false;
          }
          break;
        case 'v':
        case 'V':
          if (state == LexState.Normal
              && preValuePart1 == null
              && (lastChar == ')' || ((byte) lastChar <= 40))
              && queryLength > i + 6
              && (query[i + 1] == 'a' || query[i + 1] == 'A')
              && (query[i + 2] == 'l' || query[i + 2] == 'L')
              && (query[i + 3] == 'u' || query[i + 3] == 'U')
              && (query[i + 4] == 'e' || query[i + 4] == 'E')) {
              boolean flagValues = false;
              boolean flagValue = false;
              flagValues = (query[i + 5] == 's' || query[i + 5] == 'S') && ((query.length > i + 6) && (query[i + 6] == '(' || ((byte) query[i + 6] <= 40)));
              flagValue = (query[i + 5] == '(' || ((byte) query[i + 5] <= 40));
              if(( (isOracleMode && flagValues) || (!isOracleMode && (flagValues || flagValue)) ) && keyState != KeyState.Update) {
                  sb.append(car);
                  sb.append(query[i + 1]);
                  sb.append(query[i + 2]);
                  sb.append(query[i + 3]);
                  sb.append(query[i + 4]);
                  if(flagValue && !isOracleMode) {
                      i = i + 4;
                  } else {
                      sb.append(query[i + 5]);
                      i = i + 5;
                  }
                  preValuePart1 = sb.toString();
                  sb.setLength(0);
                  skipChar = true;
              }
          }
          break;
        case 'l':
        case 'L':
          if (state == LexState.Normal
              && queryLength > i + 14
              && (query[i + 1] == 'a' || query[i + 1] == 'A')
              && (query[i + 2] == 's' || query[i + 2] == 'S')
              && (query[i + 3] == 't' || query[i + 3] == 'T')
              && query[i + 4] == '_'
              && (query[i + 5] == 'i' || query[i + 5] == 'I')
              && (query[i + 6] == 'n' || query[i + 6] == 'N')
              && (query[i + 7] == 's' || query[i + 7] == 'S')
              && (query[i + 8] == 'e' || query[i + 8] == 'E')
              && (query[i + 9] == 'r' || query[i + 9] == 'R')
              && (query[i + 10] == 't' || query[i + 10] == 'T')
              && query[i + 11] == '_'
              && (query[i + 12] == 'i' || query[i + 12] == 'I')
              && (query[i + 13] == 'd' || query[i + 13] == 'D')
              && query[i + 14] == '(') {
            sb.append(car);
            reWritablePrepare = false;
            skipChar = true;
          }
          break;
        case '(':
          if (state == LexState.Normal) {
            isInParenthesis++;
          }
          break;
        case ')':
          if (state == LexState.Normal) {
            isInParenthesis--;
            if (isInParenthesis == 0 && preValuePart2 != null && postValuePart == null) {
              sb.append(car);
              postValuePart = sb.toString();
              sb.setLength(0);
              skipChar = true;
            }
          }
          break;
          case 'o':
          case 'O':
              if (state == LexState.Normal
                      && postValuePart == null
                      && queryLength > i  + 1
                      && (query[i + 1] == 'n' || query[i + 1] == 'N')) {
                  onDuplicateKeyUpdateIndex = sb.toString().length();
                  sb.append(car);
                  keyState = KeyState.No;
                  skipChar = true;
              }
              break;
          case 'd':
          case 'D':
              if (state == LexState.Normal
                      && keyState == KeyState.No
                      && postValuePart == null
                      && queryLength > i  + 8
                      && (query[i + 1] == 'u' || query[i + 1] == 'U')
                      && (query[i + 2] == 'p' || query[i + 2] == 'P')
                      && (query[i + 3] == 'l' || query[i + 3] == 'L')
                      && (query[i + 4] == 'i' || query[i + 4] == 'I')
                      && (query[i + 5] == 'c' || query[i + 5] == 'C')
                      && (query[i + 6] == 'a' || query[i + 6] == 'A')
                      && (query[i + 7] == 't' || query[i + 7] == 'T')
                      && (query[i + 8] == 'e' || query[i + 8] == 'E')) {
                  sb.append(car);
                  keyState = KeyState.Duplicate;
                  skipChar = true;
              }
              break;
          case 'k':
          case 'K':
              if (state == LexState.Normal
                      && keyState == KeyState.Duplicate
                      && postValuePart == null
                      && queryLength > i  + 2
                      && (query[i + 1] == 'e' || query[i + 1] == 'E')
                      && (query[i + 2] == 'y' || query[i + 2] == 'Y')) {
                  sb.append(car);
                  keyState = KeyState.Key;
                  skipChar = true;
              }
              break;
          case 'u':
          case 'U':
              if (state == LexState.Normal
                      && keyState == KeyState.Key
                      && postValuePart == null
                      && queryLength > i  + 5
                      && (query[i + 1] == 'p' || query[i + 1] == 'P')
                      && (query[i + 2] == 'd' || query[i + 2] == 'D')
                      && (query[i + 3] == 'a' || query[i + 3] == 'A')
                      && (query[i + 4] == 't' || query[i + 4] == 'T')
                      && (query[i + 5] == 'e' || query[i + 5] == 'E')) {
                  sb.append(car);
                  keyState = KeyState.Update;
                  skipChar = true;
              }
              break;

        default:
          if (state == LexState.Normal && isFirstChar && ((byte) car >= 40)) {
            if (car == 'I' || car == 'i') {
              isInsert = true;
            }
            isFirstChar = false;
          }
          // multiple queries
          if (state == LexState.Normal && semicolon && ((byte) car >= 40)) {
            reWritablePrepare = false;
            multipleQueriesPrepare = true;
          }
          break;
      }

      lastChar = car;
      if (skipChar) {
        skipChar = false;
      } else {
        sb.append(car);
      }
    }

    if (!hasParam) {
      // permit to have rewrite without parameter
      if (preValuePart1 == null) {
        partList.add(0, sb.toString().getBytes(charset));
        partList.add(1, new byte[0]);
      } else {
        partList.add(0, preValuePart1.getBytes(charset));
        if(keyState == KeyState.Update && onDuplicateKeyUpdateIndex !=-1) {
            partList.add(1, sb.substring(0,onDuplicateKeyUpdateIndex).getBytes(charset));
            partList.add(2, sb.substring(onDuplicateKeyUpdateIndex).getBytes(charset));
        } else {
            partList.add(1, sb.toString().getBytes(charset));
        }
      }
      sb.setLength(0);
    } else {
      partList.add(
          0,
          (preValuePart1 == null) ? new byte[0] : preValuePart1.getBytes(charset));
      partList.add(
          1,
          (preValuePart2 == null) ? new byte[0] : preValuePart2.getBytes(charset));
    }

    if (!isInsert) {
      reWritablePrepare = false;
    }

    // postValuePart is the value after the last parameter and parenthesis
    // if no param, don't add to the list.
    if (hasParam) {
      partList.add(
          (postValuePart == null) ? new byte[0] : postValuePart.getBytes(charset));
    }
    partList.add(sb.toString().getBytes(charset));

    return new ClientPrepareResult(
        queryString, partList, reWritablePrepare, multipleQueriesPrepare, true,hasParam);
  }

    public static List<String> rewritablePartsInsertSql(
            String queryString, boolean noBackslashEscapes, boolean isOracleMode,String encoding) {
        Charset charset = Charset.forName(encoding);
        boolean reWritablePrepare = true;
        boolean multipleQueriesPrepare = true;
        List<String> stringPartList = new ArrayList<>();
        LexState state = LexState.Normal;
        char lastChar = '\0';

        StringBuilder sb = new StringBuilder();

        String preValuePart1 = null;
        String preValuePart2 = null;
        String postValuePart = null;

        boolean singleQuotes = false;

        int isInParenthesis = 0;
        boolean skipChar = false;
        boolean isFirstChar = true;
        boolean isInsert = false;
        boolean semicolon = false;
        boolean hasParam = false;
        boolean endNameBinding = false;
        char[] query = queryString.toCharArray();
        int queryLength = query.length;
        KeyState keyState = KeyState.Normal;
        int onDuplicateKeyUpdateIndex = -1;
        int lastCommentEndStarPos = -2;
        int lastCommentBeginSlashPos = 0;
        for (int i = 0; i < queryLength; i++) {

            char car = query[i];
            if (state == LexState.Escape
                    && !((car == '\'' && singleQuotes) || (car == '"' && !singleQuotes))) {
                sb.append(car);
                lastChar = car;
                state = LexState.String;
                continue;
            }

            switch (car) {
                case '*':
                    if (state == LexState.Normal && lastChar == '/' && i > lastCommentEndStarPos + 2) {
                        state = LexState.SlashStarComment;
                        lastCommentBeginSlashPos = i - 1;
                    }
                    break;
                case '/':
                    if (state == LexState.SlashStarComment && lastChar == '*' && i > lastCommentBeginSlashPos + 2) {
                        state = LexState.Normal;
                        lastCommentEndStarPos = i - 1;
                    }
                    break;

                case '#':
                    if ((!isOracleMode) && state == LexState.Normal) {
                        state = LexState.EOLComment;
                    }
                    break;

                case '-':
                    if (state == LexState.Normal && lastChar == '-') {
                        state = LexState.EOLComment;
                        multipleQueriesPrepare = false;
                    }
                    break;

                case '\n':
                    if (state == LexState.EOLComment) {
                        state = LexState.Normal;
                    }
                    if (isOracleMode) {  // such as where c1 = ?
                        if (state == LexState.NabmeBinding) {
                            endNameBinding = true;
                        }
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
                case ';':
                    if (state == LexState.Normal) {
                        semicolon = true;
                        multipleQueriesPrepare = false;
                    }
                    if (isOracleMode) {
                        if (state == LexState.NabmeBinding) {
                            endNameBinding = true;
                        }
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
                    if ((!isOracleMode) && state == LexState.String) {
                        state = LexState.Escape;
                    }
                    break;
                case '?':
                    if (state == LexState.Normal) {
                        hasParam = true;
                        if (preValuePart1 == null) {
                            preValuePart1 = sb.toString();
                            sb.setLength(0);
                        }
                        if (preValuePart2 == null) {
                            preValuePart2 = sb.toString();
                            sb.setLength(0);
                        } else {
                            if (postValuePart != null) {
                                // having parameters after the last ")" of value is not rewritable
                                reWritablePrepare = false;

                                // add part
                                sb.insert(0, postValuePart);
                                postValuePart = null;
                            }
                            stringPartList.add(sb.toString());
                            sb.setLength(0);
                        }

                        skipChar = true;
                    }
                    break;
                case '`':
                    if (state == LexState.Backtick) {
                        state = LexState.Normal;
                    } else if (state == LexState.Normal) {
                        state = LexState.Backtick;
                    }
                    break;

                case 's':
                case 'S':
                    if (state == LexState.Normal
                            && postValuePart == null
                            && queryLength > i + 7
                            && (query[i + 1] == 'e' || query[i + 1] == 'E')
                            && (query[i + 2] == 'l' || query[i + 2] == 'L')
                            && (query[i + 3] == 'e' || query[i + 3] == 'E')
                            && (query[i + 4] == 'c' || query[i + 4] == 'C')
                            && (query[i + 5] == 't' || query[i + 5] == 'T')) {

                        // field/table name might contain 'select'
                        if (i > 0 && (query[i - 1] > ' ' && "();><=-+,".indexOf(query[i - 1]) == -1)) {
                            break;
                        }
                        if (query[i + 6] > ' ' && "();><=-+,".indexOf(query[i + 6]) == -1) {
                            break;
                        }

                        // SELECT queries, INSERT FROM SELECT not rewritable
                        reWritablePrepare = false;
                    }
                    break;
                case 'v':
                case 'V':
                    if (state == LexState.Normal
                            && preValuePart1 == null
                            && (lastChar == ')' || ((byte) lastChar <= 40))
                            && queryLength > i + 6
                            && (query[i + 1] == 'a' || query[i + 1] == 'A')
                            && (query[i + 2] == 'l' || query[i + 2] == 'L')
                            && (query[i + 3] == 'u' || query[i + 3] == 'U')
                            && (query[i + 4] == 'e' || query[i + 4] == 'E')){
                        boolean flagValues = false;
                        boolean flagValue = false;
                        flagValues = (query[i + 5] == 's' || query[i + 5] == 'S') && ((query.length > i + 6) && (query[i + 6] == '(' || ((byte) query[i + 6] <= 40)));
                        flagValue = (query[i + 5] == '(' || ((byte) query[i + 5] <= 40));
                        if(( (isOracleMode && flagValues) || (!isOracleMode && (flagValues || flagValue)) ) && keyState != KeyState.Update) {
                            sb.append(car);
                            sb.append(query[i + 1]);
                            sb.append(query[i + 2]);
                            sb.append(query[i + 3]);
                            sb.append(query[i + 4]);
                            if (flagValue && !isOracleMode) {
                                i = i + 4;
                            } else {
                                sb.append(query[i + 5]);
                                i = i + 5;
                            }
                            preValuePart1 = sb.toString();
                            sb.setLength(0);
                            skipChar = true;
                        }
                    }
                    break;
                case 'l':
                case 'L':
                    if (state == LexState.Normal
                            && queryLength > i + 14
                            && (query[i + 1] == 'a' || query[i + 1] == 'A')
                            && (query[i + 2] == 's' || query[i + 2] == 'S')
                            && (query[i + 3] == 't' || query[i + 3] == 'T')
                            && query[i + 4] == '_'
                            && (query[i + 5] == 'i' || query[i + 5] == 'I')
                            && (query[i + 6] == 'n' || query[i + 6] == 'N')
                            && (query[i + 7] == 's' || query[i + 7] == 'S')
                            && (query[i + 8] == 'e' || query[i + 8] == 'E')
                            && (query[i + 9] == 'r' || query[i + 9] == 'R')
                            && (query[i + 10] == 't' || query[i + 10] == 'T')
                            && query[i + 11] == '_'
                            && (query[i + 12] == 'i' || query[i + 12] == 'I')
                            && (query[i + 13] == 'd' || query[i + 13] == 'D')
                            && query[i + 14] == '(') {
                        sb.append(car);
                        reWritablePrepare = false;
                        skipChar = true;
                    }
                    break;

                case 'o':
                case 'O':
                    if (state == LexState.Normal
                            && postValuePart == null
                            && queryLength > i  + 1
                            && (query[i + 1] == 'n' || query[i + 1] == 'N')) {
                        onDuplicateKeyUpdateIndex = sb.toString().length();
                        sb.append(car);
                        keyState = KeyState.No;
                        skipChar = true;
                    }
                    break;
                case 'd':
                case 'D':
                    if (state == LexState.Normal
                            && keyState == KeyState.No
                            && postValuePart == null
                            && queryLength > i  + 8
                            && (query[i + 1] == 'u' || query[i + 1] == 'U')
                            && (query[i + 2] == 'p' || query[i + 2] == 'P')
                            && (query[i + 3] == 'l' || query[i + 3] == 'L')
                            && (query[i + 4] == 'i' || query[i + 4] == 'I')
                            && (query[i + 5] == 'c' || query[i + 5] == 'C')
                            && (query[i + 6] == 'a' || query[i + 6] == 'A')
                            && (query[i + 7] == 't' || query[i + 7] == 'T')
                            && (query[i + 8] == 'e' || query[i + 8] == 'E')) {
                        sb.append(car);
                        keyState = KeyState.Duplicate;
                        skipChar = true;
                    }
                    break;
                case 'k':
                case 'K':
                    if (state == LexState.Normal
                            && keyState == KeyState.Duplicate
                            && postValuePart == null
                            && queryLength > i  + 2
                            && (query[i + 1] == 'e' || query[i + 1] == 'E')
                            && (query[i + 2] == 'y' || query[i + 2] == 'Y')) {
                        sb.append(car);
                        keyState = KeyState.Key;
                        skipChar = true;
                    }
                    break;
                case 'u':
                case 'U':
                    if (state == LexState.Normal
                            && keyState == KeyState.Key
                            && postValuePart == null
                            && queryLength > i  + 5
                            && (query[i + 1] == 'p' || query[i + 1] == 'P')
                            && (query[i + 2] == 'd' || query[i + 2] == 'D')
                            && (query[i + 3] == 'a' || query[i + 3] == 'A')
                            && (query[i + 4] == 't' || query[i + 4] == 'T')
                            && (query[i + 5] == 'e' || query[i + 5] == 'E')) {
                        sb.append(car);
                        keyState = KeyState.Update;
                        skipChar = true;
                    }
                    break;
                case '(':
                    if (state == LexState.Normal) {
                        isInParenthesis++;
                    }
                    break;
                case ')':
                    if (state == LexState.Normal) {
                        isInParenthesis--;
                        if (isInParenthesis == 0 && preValuePart2 != null && postValuePart == null) {
                            sb.append(car);
                            postValuePart = sb.toString();
                            sb.setLength(0);
                            skipChar = true;
                        }
                    }
                    break;
                default:
                    if (state == LexState.Normal && isFirstChar && ((byte) car >= 40)) {
                        if (car == 'I' || car == 'i') {
                            isInsert = true;
                        }
                        isFirstChar = false;
                    }
                    // multiple queries
                    if (state == LexState.Normal && semicolon && ((byte) car >= 40)) {
                        reWritablePrepare = false;
                        multipleQueriesPrepare = true;
                    }
                    break;
            }

            lastChar = car;
            if (skipChar) {
                skipChar = false;
            } else {
                sb.append(car);
            }
        }

        if (!hasParam) {
            // permit to have rewrite without parameter
            if (preValuePart1 == null) {
                stringPartList.add(0,sb.toString());
                stringPartList.add(1,"?");
            } else {

                stringPartList.add(0,preValuePart1);
                if(keyState == KeyState.Update && onDuplicateKeyUpdateIndex !=-1) {
                    stringPartList.add(1, sb.substring(0,onDuplicateKeyUpdateIndex));
                    stringPartList.add(2, sb.substring(onDuplicateKeyUpdateIndex));
                } else {
                    stringPartList.add(1, sb.toString());
                }
            }
            sb.setLength(0);
        } else {
            stringPartList.add(
                    0,
                    (preValuePart1 == null) ? "" : preValuePart1);
            stringPartList.add(
                    1,
                    (preValuePart2 == null) ? "" : preValuePart2);
        }

        if (!isInsert) {
            reWritablePrepare = false;
        }

        // postValuePart is the value after the last parameter and parenthesis
        // if no param, don't add to the list.
        if (hasParam) {
            stringPartList.add(
                    (postValuePart == null) ? "" : postValuePart.toString());
        }
        stringPartList.add(sb.toString());
        if(reWritablePrepare) {
            return stringPartList;
        } else {
            return  null;
        }
    }

    public String getSql() {
        return sql;
    }

    public List<byte[]> getQueryParts() {
        return queryParts;
    }

    public boolean isQueryMultiValuesRewritable() {
        return isQueryMultiValuesRewritable;
    }

    public boolean isQueryMultipleRewritable() {
        return isQueryMultipleRewritable;
    }

    public boolean isRewriteType() {
        return rewriteType;
    }

    public int getParamCount() {
        return paramCount;
    }

    enum LexState {
        Normal, /* inside  query */
        String, /* inside string */
        SlashStarComment, /* inside slash-star comment */
        Escape, /* found backslash */
        EOLComment, /* # comment, or // comment, or -- comment */
        Backtick /* found backtick */,
        NabmeBinding /* in name binding such as :name */
    }

    enum KeyState {
        Normal, /* inside  query */
        No,
        Duplicate,
        Key,
        Update
    }
}
