package com.alipay.oceanbase.jdbc.internal.com.read.dao;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import com.alipay.oceanbase.jdbc.internal.com.read.resultset.ColumnDefinition;
import com.alipay.oceanbase.jdbc.internal.util.exceptions.ExceptionFactory;

public class ColumnLabelIndexer {

    private final ColumnDefinition[] columnInfo;
    private Map<String, Integer>     originalMap;
    private Map<String, Integer>     aliasMap;

    public ColumnLabelIndexer(ColumnDefinition[] columnDefinitions) {
        this.columnInfo = columnDefinitions;
    }

    /**
     * Get column index by name.
     *
     * @param name column name
     * @return index.
     * @throws SQLException if no column info exists, or column is unknown
     */
    public int getIndex(String name) throws SQLException {
    if (name == null) {
      throw new SQLException("Column name cannot be null");
    }
    String lowerName = name.toLowerCase(Locale.ROOT);
    // The specs in JDBC 4.0 specify that ResultSet.findColumn and
    // ResultSet.getXXX(String name) should use column alias (AS in the query). If label is not
    // found, we use
    // original table name.
    if (aliasMap == null) {
      Map<String, Integer> map = new HashMap<>();
      int counter = 0;
      for (ColumnDefinition ci : columnInfo) {
        String columnAlias = ci.getName();
        if (columnAlias != null) {
          columnAlias = columnAlias.toLowerCase(Locale.ROOT);
          map.putIfAbsent(columnAlias, counter);

          String tableName = ci.getTable();
          if (tableName != null) {
            map.putIfAbsent(tableName.toLowerCase(Locale.ROOT) + "." + columnAlias, counter);
          }
        }
        counter++;
      }
      aliasMap = map;
    }

    Integer res = aliasMap.get(lowerName);
    if (res != null) {
      return res;
    }

    if (originalMap == null) {
      Map<String, Integer> map = new HashMap<>();
      int counter = 0;
      for (ColumnDefinition ci : columnInfo) {
        String columnRealName = ci.getOriginalName();
        if (columnRealName != null) {
          columnRealName = columnRealName.toLowerCase(Locale.ROOT);
          map.putIfAbsent(columnRealName, counter);

          String tableName = ci.getOriginalTable();
          if (tableName != null) {
            map.putIfAbsent(tableName.toLowerCase(Locale.ROOT) + "." + columnRealName, counter);
          }
        }
        counter++;
      }
      originalMap = map;
    }

    res = originalMap.get(lowerName);

    if (res == null) {
      Map<String, Integer> possible = new HashMap<>();
      possible.putAll(aliasMap);
      possible.putAll(originalMap);
      throw ExceptionFactory.INSTANCE.create(
          String.format(
              "No such column: '%s'. '%s' must be in %s",
              name, lowerName, possible.keySet().toString()),
          "42S22",
          1054);
    }
    return res;
  }
}
