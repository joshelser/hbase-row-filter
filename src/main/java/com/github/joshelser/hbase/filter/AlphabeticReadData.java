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

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 */
public class AlphabeticReadData extends AbstractReadData {
  private static final Logger LOG = LoggerFactory.getLogger(AlphabeticReadData.class);

  @Override Logger getLogger() {
    return LOG;
  }

  @Override
  public void run() {
    try {
      _run();
    } catch (Exception e) {
      LOG.error("Failed to read data", e);
      throw new RuntimeException(e);
    }
  }

  void _run() throws Exception {
    try (Connection conn = ConnectionFactory.createConnection(HBaseConfiguration.create())) {
      LOG.info("**** Test1 *******");
      test1(conn);
      LOG.info("**** Test2 *******");
      test2(conn);
      LOG.info("**** Test3 *******");
      test3(conn);
      LOG.info("**** Test4 *******");
      test4(conn);
    }
  }

  void test1(Connection conn) throws IOException {
    RowFilter filter = new RowFilter(CompareOp.EQUAL, new SubstringComparator("ab"));
    scanWithFilter(conn, AlphabeticDataLoad.TABLE_NAME, "test1", AlphabeticDataLoad.FAMILY, filter, UTF8_STRINGER);
  }

  void test2(Connection conn) throws IOException {
    RowFilter filter = new RowFilter(CompareOp.EQUAL, new SubstringComparator("abcd"));
    scanWithFilter(conn, AlphabeticDataLoad.TABLE_NAME, "test2", AlphabeticDataLoad.FAMILY, filter, UTF8_STRINGER);
  }

  void test3(Connection conn) throws IOException {
    RowFilter filter = new RowFilter(CompareOp.EQUAL, new SubstringComparator("abc"));
    scanWithFilter(conn, AlphabeticDataLoad.TABLE_NAME, "test3", AlphabeticDataLoad.FAMILY, filter, UTF8_STRINGER);
  }

  void test4(Connection conn) throws IOException {
    RowFilter filter = new RowFilter(CompareOp.EQUAL, new SubstringComparator("bcd"));
    scanWithFilter(conn, AlphabeticDataLoad.TABLE_NAME, "test4", AlphabeticDataLoad.FAMILY, filter, UTF8_STRINGER);
  }

  public static void main(String[] args) {
    new AlphabeticReadData().run();
  }
}
