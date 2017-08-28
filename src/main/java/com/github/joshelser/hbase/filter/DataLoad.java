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
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.RegionSplitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 */
public class DataLoad implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(DataLoad.class);
  public static final TableName TABLE_NAME = TableName.valueOf("filter_test");
  private static final int NUM_REGIONS = 30;
  public static final byte[] FAMILY = Bytes.toBytes("f1");
  private static final int NUM_ROWS = 1000000;
  private static final int NUM_COLS_PER_ROW = 5;

  public void run() {
    try {
      _run();
    } catch (Exception e) {
      LOG.error("Failed to load data", e);
      throw new RuntimeException(e);
    }
  }

  void _run() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    try (Connection conn = ConnectionFactory.createConnection(conf)) {
      final Admin admin = conn.getAdmin();

      // Create or empty the table
      if (admin.tableExists(TABLE_NAME)) {
        if (admin.isTableEnabled(TABLE_NAME)) {
          LOG.info("Disabling {}", TABLE_NAME);
          admin.disableTable(TABLE_NAME);
        }
        LOG.info("Truncating {}", TABLE_NAME);
        admin.truncateTable(TABLE_NAME, true);
      } else {
        createTable(admin);
      }

      LOG.info("Loading data");
      loadData(conn);
      LOG.info("Data load complete");
    }
  }

  /**
   * Create a new table with the colfam {@code FAMILY} and {@code NUM_REGIONS} split points.
   */
  void createTable(Admin admin) throws IOException {
    HTableDescriptor tDesc = new HTableDescriptor(TABLE_NAME);
    HColumnDescriptor cDesc = new HColumnDescriptor(FAMILY);
    tDesc.addFamily(cDesc);
    byte[][] splits = new RegionSplitter.UniformSplit().split(NUM_REGIONS);
    LOG.info("Creating {} with splits {}", TABLE_NAME, splitsToString(splits));
    admin.createTable(tDesc, splits);
  }

  /**
   * Stringify an array of byte-array split points.
   */
  String splitsToString(byte[][] splits) {
    StringBuilder sb = new StringBuilder();
    sb.append("[");
    for (byte[] split : splits) {
      if (sb.length() > 0) {
        sb.append(", ");
      }
      sb.append(Bytes.toString(split));
    }
    sb.append("]");
    return sb.toString();
  }

  void loadData(Connection conn) throws IOException {
    try (BufferedMutator writer = conn.getBufferedMutator(TABLE_NAME)) {
      for (int n_row = 0; n_row < NUM_ROWS; n_row++) {
        byte[] fwd_bytes = Bytes.toBytes(n_row);
        Put p = new Put(reverseBytes(fwd_bytes));
        for (int n_col = 0; n_col < NUM_COLS_PER_ROW; n_col++) {
          p.addColumn(FAMILY, Bytes.toBytes("c_" + n_col), Bytes.toBytes("v_" + n_col));
        }
        writer.mutate(p);
      }
    }
  }

  byte[] reverseBytes(byte[] input) {
    byte[] rev_bytes = new byte[input.length];
    int i = input.length;
    for (byte b : input) {
      // Decrement and then use
      rev_bytes[--i] = b;
    }
    return rev_bytes;
  }

  public static void main(String[] args) throws Exception {
    new DataLoad().run();
  }
}
