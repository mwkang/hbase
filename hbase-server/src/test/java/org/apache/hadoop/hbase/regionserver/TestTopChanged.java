/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.regionserver;

import static org.apache.hadoop.hbase.NamespaceDescriptor.DEFAULT_NAMESPACE_NAME_STR;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.LocalHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(LargeTests.class)
public class TestTopChanged {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestTopChanged.class);

  private HBaseTestingUtil testUtil;
  private LocalHBaseCluster cluster;

  @Before
  public void setUp() throws Exception {
    testUtil = new HBaseTestingUtil();
    testUtil.startMiniDFSCluster(1);
    testUtil.startMiniZKCluster(1);
    testUtil.createRootDir();

    final Configuration conf = testUtil.getConfiguration();
    cluster = new LocalHBaseCluster(conf, 1, 1);
    cluster.startup();
    cluster.getActiveMaster().waitForMetaOnline();
  }

  @After
  public void tearDown() throws Exception {
    cluster.shutdown();
    cluster.join();

    testUtil.shutdownMiniZKCluster();
    testUtil.shutdownMiniDFSCluster();
  }

  @Test
  public void testTopChanged() throws Exception {

    try (
      final Connection connection = ConnectionFactory.createConnection(cluster.getConfiguration());
      final Admin admin = connection.getAdmin()) {
      // Create a table with two column families
      final TableName tableName = TableName.valueOf(DEFAULT_NAMESPACE_NAME_STR, "test");
      final TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tableName)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of("cf1"))
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of("cf2")).build();
      admin.createTable(tableDescriptor);

      try (final Table table = connection.getTable(tableName)) {
        table.put(createPuts("row1"));

        final List<Delete> deletes = new ArrayList<>();
        deletes.add(
          new Delete(Bytes.toBytes("row1")).addColumns(Bytes.toBytes("cf1"), Bytes.toBytes("cq1")));
        deletes.add(
          new Delete(Bytes.toBytes("row1")).addColumns(Bytes.toBytes("cf1"), Bytes.toBytes("cq2")));
        deletes.add(
          new Delete(Bytes.toBytes("row1")).addColumns(Bytes.toBytes("cf1"), Bytes.toBytes("cq3")));
        table.delete(deletes);

        deletes.clear();
        deletes.add(
          new Delete(Bytes.toBytes("row1")).addColumns(Bytes.toBytes("cf2"), Bytes.toBytes("cq1")));
        deletes.add(
          new Delete(Bytes.toBytes("row1")).addColumns(Bytes.toBytes("cf2"), Bytes.toBytes("cq2")));
        deletes.add(
          new Delete(Bytes.toBytes("row1")).addColumns(Bytes.toBytes("cf2"), Bytes.toBytes("cq3")));
        table.delete(deletes);

        table.put(createPuts("row2"));
      }

      try (final Table table = connection.getTable(tableName)) {
        final Scan scan = new Scan();
        scan.setAttribute("TEST_TOP_CHANGED", Bytes.toBytes("true"));

        try (final ResultScanner scanner = table.getScanner(scan)) {
          for (Result result : scanner) {
            final String cf = result.getNoVersionMap().keySet().stream().map(Bytes::toString)
              .collect(Collectors.joining(", "));

            System.out
              .println("RESULT: rowkey = " + Bytes.toString(result.getRow()) + ", cf = " + cf);
          }
        }
      }
    }
  }

  private List<Put> createPuts(String rowKey) {
    final List<Put> puts = new ArrayList<>();

    // cf1
    Put put = new Put(Bytes.toBytes(rowKey));
    put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("cq1"), Bytes.toBytes("value " + rowKey));
    puts.add(put);

    put = new Put(Bytes.toBytes(rowKey));
    put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("cq2"), Bytes.toBytes("value " + rowKey));
    puts.add(put);

    put = new Put(Bytes.toBytes(rowKey));
    put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("cq3"), Bytes.toBytes("value " + rowKey));
    puts.add(put);

    // cf2
    put = new Put(Bytes.toBytes(rowKey));
    put.addColumn(Bytes.toBytes("cf2"), Bytes.toBytes("cq1"), Bytes.toBytes("value " + rowKey));
    puts.add(put);

    put = new Put(Bytes.toBytes(rowKey));
    put.addColumn(Bytes.toBytes("cf2"), Bytes.toBytes("cq2"), Bytes.toBytes("value " + rowKey));
    puts.add(put);

    put = new Put(Bytes.toBytes(rowKey));
    put.addColumn(Bytes.toBytes("cf2"), Bytes.toBytes("cq3"), Bytes.toBytes("value " + rowKey));
    puts.add(put);

    return puts;
  }
}
