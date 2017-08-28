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
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReadData extends AbstractReadData {
  private static final Logger LOG = LoggerFactory.getLogger(ReadData.class);

  @Override public Logger getLogger() {
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
      LOG.info("******** Test1 ***********");
      test1(conn);
      LOG.info("******** Test2 ***********");
      test2(conn);
    }
  }

  void test1(Connection conn) throws IOException {
    RowFilter filter = new RowFilter(CompareOp.LESS_OR_EQUAL, new BinaryComparator(new byte[] {0, 5}));
    scanWithFilter(conn, DataLoad.TABLE_NAME, "test1", DataLoad.FAMILY, filter, HEX_STRINGER);
  }

  void test2(Connection conn) throws IOException {
    RowFilter filter = new RowFilter(CompareOp.LESS_OR_EQUAL, new BinaryComparator(new byte[] {2, 2, 2}));
    scanWithFilter(conn, DataLoad.TABLE_NAME, "test2", DataLoad.FAMILY, filter, HEX_STRINGER);
  }

  public static void main(String[] args) {
    new ReadData().run();
  }
}
