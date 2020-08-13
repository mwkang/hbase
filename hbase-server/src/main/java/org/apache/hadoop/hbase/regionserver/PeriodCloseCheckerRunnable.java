/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.regionserver;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Periodically task to see if the system is stopped.
 */
@InterfaceAudience.Private
class PeriodCloseCheckerRunnable implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(PeriodCloseCheckerRunnable.class);
  private static AtomicInteger atomicInteger = new AtomicInteger(0);
  private final Store store;

  PeriodCloseCheckerRunnable(Store store) {
    this.store = store;
  }

  @Override
  public void run() {
    if (atomicInteger.getAndIncrement() == 5) {
      final String message = String.format("Aborting compact or flush of store %s in region %s because writes is unable.", store.toString(), store.getRegionInfo().getRegionNameAsString());
      LOG.error(message);
      throw new UncheckedRegionStoppedException(message);
    }
    LOG.error("good");

//    if (store.areWritesEnabled()) {
//      return;
//    }
//
//    final String message = String.format("Aborting compact or flush of store %s in region %s because writes is unable.", store.toString(), store.getRegionInfo().getRegionNameAsString());
//    LOG.error(message);
//    throw new UncheckedRegionStoppedException(message);
  }
}
