package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;

import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(LargeTests.class)
public class TestRowsFilteredMetrics {
  private final static HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();

  private static TableName TABLE_NAME = TableName.valueOf("testRowsFilteredMetricsTable");
  private static byte[] FAMILY = Bytes.toBytes("testFamily");

  private static Table table;
  private static Admin admin;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(1);
    table = TEST_UTIL.createTable(TABLE_NAME, FAMILY);
    admin = TEST_UTIL.getAdmin();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testRowsFilteredMetrics() throws Exception {
    // put
    Put put = new Put(Bytes.toBytes("row1"));
    put.addColumn(FAMILY, Bytes.toBytes("qual"), Bytes.toBytes("value"));
    table.put(put);

    admin.flush(TABLE_NAME);
    // scan
    scan();

    // delete
    Delete delete = new Delete(Bytes.toBytes("row1"));
    table.delete(delete);

    admin.flush(TABLE_NAME);
    scan();
    scan();
  }

  private static void scan() throws IOException {
    final Scan scan = new Scan();
    scan.setScanMetricsEnabled(true);
    final ResultScanner resultScanner = table.getScanner(scan);

    resultScanner.forEach(result -> {
    });

    final ScanMetrics metrics = resultScanner.getScanMetrics();
    System.out.printf(
        "countOfRowsScanned: %s, countOfRowsFiltered: %s, countOfBlockBytesScanned: %s\n",
        metrics.countOfRowsScanned, metrics.countOfRowsFiltered, metrics.countOfBlockBytesScanned);
  }
}
