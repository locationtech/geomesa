/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
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
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty

/**
 * Singleton for registering and managing running queries.
 */
object ThreadManagement extends Runnable with LazyLogging {

  // how often we check for expired readers
  private val interval = SystemProperty("geomesa.query.timeout.check").toDuration.map(_.toMillis).getOrElse(5000L)

  // head of queue will be ones the will timeout first
  private val openReaders = new PriorityBlockingQueue[ReaderAndTime]()

  private val executor = MoreExecutors.getExitingScheduledExecutorService(new ScheduledThreadPoolExecutor(1))
  executor.scheduleWithFixedDelay(this, interval, interval, TimeUnit.MILLISECONDS)
  sys.addShutdownHook(executor.shutdownNow())

  override def run(): Unit = {
    var loop = true
    var numClosed = 0
    while (loop) {
      val holder = openReaders.peek() // peek but don't remove, as that will trigger a re-ordering
      if (holder == null || holder.killAt > System.currentTimeMillis()) {
        loop = false
      } else {
        // note: holder should be the first obj in the priority queue backing array, so remove will be O(1)
        openReaders.remove(holder)
        // last sanity check in case the reader was closed but hasn't been removed from the queue yet
        if (!holder.reader.isClosed) {
          logger.warn(s"Stopping query on schema '${holder.reader.query.getTypeName}' with filter " +
              s"'${filterToString(holder.reader.query.getFilter)}' based on timeout of ${holder.reader.timeout.get}ms")
          holder.thread.interrupt()
          holder.reader.close()
          numClosed += 1
        }
      }
    }
    logger.trace(s"Force closed $numClosed queries with ${openReaders.size()} queries still running.")
  }

  /**
   * Register a query with the thread manager
   */
  def register(reader: GeoMesaFeatureReader, timeout: Long): Unit =
    openReaders.offer(new ReaderAndTime(reader, Thread.currentThread(), System.currentTimeMillis() + timeout))

  /**
    * Unregister a query with the thread manager once the query has been closed
    */
  def unregister(reader: GeoMesaFeatureReader): Unit = openReaders.remove(new ReaderAndTime(reader, null, 0L))

  /**
    * Holder for our queue. Implements equals based on the reader instance, to facilitate removing
    * from the queue when unregistering. Sorts naturally based on expiry time.
    *
    * @param reader reader
    * @param thread thread
    * @param killAt system time to kill at
    */
  private class ReaderAndTime(val reader: GeoMesaFeatureReader, val thread: Thread, val killAt: Long)
      extends Ordered[ReaderAndTime] {

    override def compare(that: ReaderAndTime): Int = java.lang.Long.compare(killAt, that.killAt)

    override def equals(obj: Any): Boolean = obj match {
      case r: ReaderAndTime => reader.eq(r.reader)
      case _ => false
    }

    override def hashCode(): Int = reader.hashCode()
  }
}
