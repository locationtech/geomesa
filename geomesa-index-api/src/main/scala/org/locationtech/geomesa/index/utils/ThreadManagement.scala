/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.utils

import java.io.Closeable
import java.util.concurrent.{ScheduledFuture, ScheduledThreadPoolExecutor, TimeUnit}

import com.typesafe.scalalogging.Logger
import org.locationtech.geomesa.filter.filterToString
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.concurrent.ExitingExecutor
import org.locationtech.geomesa.utils.iterators.ExceptionalIterator
import org.opengis.filter.Filter
import org.slf4j.LoggerFactory

import scala.concurrent.duration.Duration
import scala.util.control.NonFatal

/**
 * Singleton for registering and managing running queries.
 */
object ThreadManagement {

  private val logger = Logger(LoggerFactory.getLogger(ThreadManagement.getClass.getName.replace("$", "")))

  private val executor = {
    val ex = new ScheduledThreadPoolExecutor(2)
    ex.setRemoveOnCancelPolicy(true)
    ExitingExecutor(ex, force = true)
  }

  /**
   * Register a scan with the thread manager
   *
   * @param scan scan to terminate
   * @return
   */
  def register(scan: ManagedScan[_]): ScheduledFuture[_] = {
    val timeout = math.max(1, scan.timeout.absolute - System.currentTimeMillis())
    executor.schedule(new QueryKiller(scan), timeout, TimeUnit.MILLISECONDS)
  }

  /**
   * Trait for scans that are managed, i.e. tracked and terminated if they exceed a specified timeout
   *
   * @tparam T type
   */
  trait ManagedScan[T] extends CloseableIterator[T] {

    /**
     * Scan timeout
     *
     * @return
     */
    def timeout: Timeout

    /**
     * Low-level scan to be stopped
     *
     * @return
     */
    protected def underlying: LowLevelScanner[T]

    // used for log messages
    protected def typeName: String
    protected def filter: Option[Filter]

    // we can use a volatile var since we only update the value with a single thread
    @volatile
    private var terminated = timeout.absolute <= System.currentTimeMillis()

    private val iter = ExceptionalIterator(if (terminated) { Iterator.empty } else { underlying.iterator })
    private val cancel = if (terminated) { None } else { Some(ThreadManagement.register(this)) }

    // note: check iter.hasNext first so we get updated terminated flag
    override def hasNext: Boolean = iter.hasNext || terminated

    override def next(): T = {
      if (terminated) {
        val e = new RuntimeException(s"Scan terminated due to timeout of ${timeout.relative}ms")
        iter.suppressed.foreach(e.addSuppressed)
        throw e
      } else {
        iter.next()
      }
    }

    /**
     * Forcibly terminate the scan
     */
    def terminate(): Unit = {
      terminated = true
      try {
        logger.warn(
          s"Stopping scan on schema '$typeName' with filter '${filterToString(filter)}' " +
              s"based on timeout of ${timeout.relative}ms")
        underlying.close()
      } catch {
        case NonFatal(e) => logger.warn("Error cancelling scan:", e)
      }
    }

    /**
     * Was the scan terminated due to timeout
     *
     * @return
     */
    def isTerminated: Boolean = terminated

    override def close(): Unit = {
      cancel.foreach(_.cancel(false))
      // if terminated, we've already closed the iterator
      if (!terminated) {
        underlying.close()
      }
    }
  }

  /**
   * Low level scanner that can be closed to terminate a scan
   *
   * @tparam T type
   */
  trait LowLevelScanner[T] extends Closeable {
    def iterator: Iterator[T]
  }

  /**
   * Timeout holder
   *
   * @param relative relative timeout, in millis
   * @param absolute absolute timeout, in system millis since epoch
   */
  case class Timeout(relative: Long, absolute: Long)

  object Timeout {
    def apply(relative: Long): Timeout = Timeout(relative, System.currentTimeMillis() + relative)
    def apply(relative: String): Timeout = Timeout(Duration(relative).toMillis)
  }

  /**
   * Runnable to handle terminating a scan
   *
   * @param scan scan to terminate
   */
  private class QueryKiller(val scan: ManagedScan[_]) extends Runnable {
    override def run(): Unit = scan.terminate()
  }
}
