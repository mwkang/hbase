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

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Check periodically to see if a system stop is requested
 */
@InterfaceAudience.Private
public class CloseChecker implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(CloseChecker.class);

  public static final String SIZE_LIMIT_KEY = "hbase.hstore.close.check.interval";
  public static final String TIME_LIMIT_KEY = "hbase.hstore.close.check.time.interval";

  /**
   * The store is that want to check if it is Stop.
   */
  private final Store store;
  /**
   * size check limit
   */
  private final int closeCheckSizeLimit;
  /**
   * Service to check whether it stops according to the time schedule.
   */
  private final ScheduledExecutorService timeCheckerService;
  /**
   * Field to check if it is a stop while sleeping for a while.
   */
  private final ScheduledFuture<?> sleepService;
  /**
   * Field used in size check.
   */
  private long bytesWrittenProgressForCloseCheck;

  public CloseChecker(Configuration conf, Store store) {
    this.store = store;
    this.closeCheckSizeLimit = conf.getInt(SIZE_LIMIT_KEY, 10 * 1000 * 1000 /* 10 MB */);
    this.timeCheckerService =
      Executors.newScheduledThreadPool(1, Threads.newDaemonThreadFactory("Time-Checker-Service"));
    //Handler 추가해서 Exception 받아야 함.. 지금 확인이 안됨.
    this.sleepService = newSleepService(conf, store, timeCheckerService);
    this.bytesWrittenProgressForCloseCheck = 0;
  }

  private ScheduledFuture<?> newSleepService(Configuration conf, Store store,
    ScheduledExecutorService timeCheckerService) {
    final long period = conf.getLong(TIME_LIMIT_KEY, 10 * 1000L /* 10 s */);
    return timeCheckerService
      .scheduleWithFixedDelay(new PeriodCloseCheckerRunnable(store), period, period,
        TimeUnit.MILLISECONDS);
  }

  /**
   * Check periodically to see if a system stop is requested every written bytes reach size limit.
   *
   * @throws RegionStoppedException When the system is stopped.
   */
  public void throwExceptionIfClosed(long bytesWritten) throws RegionStoppedException {
    if (closeCheckSizeLimit <= 0) {
      return;
    }

    bytesWrittenProgressForCloseCheck += bytesWritten;
    if (bytesWrittenProgressForCloseCheck <= closeCheckSizeLimit) {
      return;
    }

    bytesWrittenProgressForCloseCheck = 0;
    if (store.areWritesEnabled()) {
      return;
    }

    throw new RegionStoppedException(
      "Aborting compact or flush of store " + store + " in region " + store.getRegionInfo()
        .getRegionNameAsString() + " because writes is unable.");
  }

  /**
   * Check periodically to see if a system stop is requested every time.
   *
   * @throws RegionStoppedException When the system is stopped.
   */
  public void throwExceptionIfClosed() throws RegionStoppedException, InterruptedException {
    try {
      sleepService.get(0, TimeUnit.MILLISECONDS);
    } catch (ExecutionException e) {
      propagateIfUncheckedRegionStopped(e);
    } catch (TimeoutException e) {
      // It is an exception caused by giving a wait time and doing a get. (Normal situation)
      LOG.error("wait timed out");
    }
  }

  /**
   * Sleep thread for a millis.
   *
   * @param millis sleep millis
   * @throws RegionStoppedException When the system is stopped.
   */
  public void sleep(long millis) throws RegionStoppedException, InterruptedException {
    try {
      sleepService.get(millis, TimeUnit.MILLISECONDS);
    } catch (ExecutionException e) {
      propagateIfUncheckedRegionStopped(e);
    } catch (TimeoutException e) {
      // It is an exception caused by giving a wait time and doing a get. (Normal situation)
      LOG.trace("wait timed out: {}", e.getMessage());
    }
  }

  private void propagateIfUncheckedRegionStopped(ExecutionException e) throws RegionStoppedException {
    final Throwable cause = e.getCause();
    if (cause instanceof UncheckedRegionStoppedException) {
      throw new RegionStoppedException((UncheckedRegionStoppedException) cause);
    }

    // An unexpected error occurred. Log it.
    LOG.error("An unexpected error occurred: {}", e.getMessage());
  }

  @Override
  public void close() throws IOException {
    timeCheckerService.shutdown();
    if (!timeCheckerService.isShutdown()) {
      final List<Runnable> runnables = timeCheckerService.shutdownNow();
      LOG.debug("Still running: {}", runnables);
    }
  }
}
