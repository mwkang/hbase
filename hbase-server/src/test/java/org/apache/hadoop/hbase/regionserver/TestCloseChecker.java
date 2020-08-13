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

import static org.apache.hadoop.hbase.regionserver.CloseChecker.SIZE_LIMIT_KEY;
import static org.apache.hadoop.hbase.regionserver.CloseChecker.TIME_LIMIT_KEY;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class) public class TestCloseChecker {

  @ClassRule public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestCloseChecker.class);

  @Test public void testIsClosed() throws Exception {
    final RegionInfo mockRegionInfo = mock(RegionInfo.class);
    when(mockRegionInfo.getRegionNameAsString()).thenReturn("testRegion");

    final Store enableWrite = mock(Store.class);
    when(enableWrite.areWritesEnabled()).thenReturn(true);
    when(enableWrite.getRegionInfo()).thenReturn(mockRegionInfo);

    final Store disableWrite = mock(Store.class);
    when(disableWrite.areWritesEnabled()).thenReturn(false);
    when(disableWrite.getRegionInfo()).thenReturn(mockRegionInfo);

    Configuration conf = new Configuration();

    conf.setInt(SIZE_LIMIT_KEY, 10);
    conf.setLong(TIME_LIMIT_KEY, 10);

    CloseChecker checker = new CloseChecker(conf, disableWrite);
    checker.throwExceptionIfClosed();
    checker.throwExceptionIfClosed();
    checker.throwExceptionIfClosed();
    checker.throwExceptionIfClosed();
    checker.throwExceptionIfClosed();
    checker.throwExceptionIfClosed();
    checker.sleep(5);

    //
    //    assertFalse(closeChecker.throwExceptionIfClosed(enableWrite, currentTime));
    //    assertFalse(closeChecker.throwExceptionIfClosed(enableWrite, 10L));
    //
    //    closeChecker = new CloseChecker(conf, currentTime);
    //    assertFalse(closeChecker.throwExceptionIfClosed(enableWrite, currentTime + 11));
    //    assertFalse(closeChecker.throwExceptionIfClosed(enableWrite, 11L));
    //
    //    closeChecker = new CloseChecker(conf, currentTime);
    //    assertTrue(closeChecker.throwExceptionIfClosed(disableWrite, currentTime + 11));
    //    assertTrue(closeChecker.throwExceptionIfClosed(disableWrite, 11L));
    //
    //    for (int i = 0; i < 10; i++) {
    //      int plusTime = 5 * i;
    //      assertFalse(closeChecker.throwExceptionIfClosed(enableWrite, currentTime + plusTime));
    //      assertFalse(closeChecker.throwExceptionIfClosed(enableWrite, 5L));
    //    }
    //
    //    closeChecker = new CloseChecker(conf, currentTime);
    //    assertFalse(closeChecker.throwExceptionIfClosed(disableWrite, currentTime + 6));
    //    assertFalse(closeChecker.throwExceptionIfClosed(disableWrite, 6));
    //    assertTrue(closeChecker.throwExceptionIfClosed(disableWrite, currentTime + 12));
    //    assertTrue(closeChecker.throwExceptionIfClosed(disableWrite, 6));
  }
}
