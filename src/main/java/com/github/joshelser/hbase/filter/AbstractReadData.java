/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.joshelser.hbase.filter;

import java.io.IOException;
import java.util.function.Function;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;

public abstract class AbstractReadData implements Runnable {

  abstract Logger getLogger();

  public static final Function<byte[],String> HEX_STRINGER = new Function<byte[],String>() {
    @Override public String apply(byte[] b) {
      return Bytes.toStringBinary(b);
    }
  };

  public static final Function<byte[],String> UTF8_STRINGER = new Function<byte[],String>() {
    @Override public String apply(byte[] b) {
      return Bytes.toString(b);
    }
  };

  void scanWithFilter(Connection conn, TableName tableName, String testName, byte[] family,
      Filter f, Function<byte[],String> stringer) throws IOException {
    try (Table t = conn.getTable(tableName)) {
      Scan scan = new Scan();
      scan.addFamily(family);
      scan.setFilter(f);
      ResultScanner scanner = t.getScanner(scan);
      long numResults = 0;
      String first_row = null;
      String last_row = null;
      String row = null;
      Result r = null;
      while ((r = scanner.next()) != null) {
        row = stringer.apply(r.getRow());
        if (first_row == null) {
          first_row = row;
        }
        last_row = row;
        numResults++;
      }
      getLogger().info("{}: Saw {} results: first_row={}, last_row={}", testName, numResults,
          first_row == null ? "null" : first_row, last_row == null ? "null" : last_row);
    }
  }
}
