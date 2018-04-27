/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.utils

import java.io.Closeable
import java.util.concurrent.{ScheduledThreadPoolExecutor, TimeUnit}

import com.google.common.util.concurrent.MoreExecutors
import com.typesafe.scalalogging.LazyLogging

import scala.ref.WeakReference
import scala.util.control.NonFatal

/**
 * Singleton for registering and managing running queries.
 */
object ThreadManagement extends LazyLogging {

  private val executor = MoreExecutors.getExitingScheduledExecutorService(new ScheduledThreadPoolExecutor(2))
  sys.addShutdownHook(executor.shutdownNow())

  /**
   * Register a query with the thread manager
   */
  def register(query: ManagedQuery): Unit =
    executor.schedule(new QueryKiller(WeakReference(query)), query.getTimeout, TimeUnit.MILLISECONDS)

  /**
    * Trait for classes to be managed for timeouts
    */
  trait ManagedQuery extends Closeable {
    def getTimeout: Long
    def isClosed: Boolean
    def debug: String
  }

  private class QueryKiller(query: WeakReference[ManagedQuery]) extends Runnable {
    override def run(): Unit = {
      query.get.foreach { q =>
        if (!q.isClosed) {
          logger.warn(s"Stopping ${q.debug} based on timeout of ${q.getTimeout}ms")
          try { q.close() } catch {
            case NonFatal(e) => logger.warn("Error cancelling query:", e)
          }
        }
      }
    }
  }
}
