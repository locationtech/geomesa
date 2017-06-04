/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.utils

import java.util.concurrent.{PriorityBlockingQueue, ScheduledThreadPoolExecutor, TimeUnit}

import com.google.common.util.concurrent.MoreExecutors
import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.filter.filterToString
import org.locationtech.geomesa.index.geotools.GeoMesaFeatureReader

/**
 * Singleton for registering and managing running queries.
 */
object ThreadManagement extends LazyLogging {

  private val interval = 5000L // how often we check for expired readers
  private val ordering = new Ordering[ReaderAndTime]() {
    // head of queue will be ones the will timeout first
    override def compare(x: ReaderAndTime, y: ReaderAndTime) = x.killAt.compareTo(y.killAt)
  }
  private val openReaders = new PriorityBlockingQueue[ReaderAndTime](11, ordering) // size will grow unbounded

  private val reaper = new Runnable() {
    override def run() = {
      val time = System.currentTimeMillis()
      var loop = true
      var numClosed = 0
      while (loop) {
        val holder = openReaders.poll()
        if (holder == null) {
          loop = false
        } else if (holder.killAt < time) {
          if (!holder.reader.isClosed) {
            logger.warn(s"Stopping query on schema '${holder.reader.query.getTypeName}' with filter " +
                s"'${filterToString(holder.reader.query.getFilter)}' based on timeout of ${holder.timeout}ms")
            holder.reader.close()
            numClosed += 1
          }
        } else {
          if (!holder.reader.isClosed) {
            openReaders.offer(holder)
          }
          loop = false
        }
      }
      logger.trace(s"Force closed $numClosed queries with ${openReaders.size()} queries still running.")
    }
  }

  private val executor = MoreExecutors.getExitingScheduledExecutorService(new ScheduledThreadPoolExecutor(1))
  executor.scheduleWithFixedDelay(reaper, interval, interval, TimeUnit.MILLISECONDS)
  sys.addShutdownHook(executor.shutdownNow())

  /**
   * Register a query with the thread manager
   */
  def register(reader: GeoMesaFeatureReader, start: Long, timeout: Long): Unit =
    openReaders.offer(ReaderAndTime(reader, start + timeout, timeout))

  /**
   * Unregister a query with the thread manager once the query has been closed
   */
  def unregister(reader: GeoMesaFeatureReader, start: Long, timeout: Long): Unit =
    openReaders.remove(ReaderAndTime(reader, start + timeout, timeout))

  case class ReaderAndTime(reader: GeoMesaFeatureReader, killAt: Long, timeout: Long)
}
