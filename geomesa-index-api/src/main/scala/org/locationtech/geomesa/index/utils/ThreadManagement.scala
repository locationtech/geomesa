/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.utils

import java.io.Closeable
import java.util.concurrent.{ScheduledFuture, ScheduledThreadPoolExecutor, TimeUnit}

import com.google.common.util.concurrent.MoreExecutors
import com.typesafe.scalalogging.LazyLogging

import scala.util.control.NonFatal

/**
 * Singleton for registering and managing running queries.
 */
object ThreadManagement extends LazyLogging {

  private val executor = {
    val ex = new ScheduledThreadPoolExecutor(2)
    ex.setRemoveOnCancelPolicy(true)
    MoreExecutors.getExitingScheduledExecutorService(ex)
  }
  sys.addShutdownHook(executor.shutdownNow())

  /**
   * Register a query with the thread manager
   */
  def register(query: ManagedQuery): ScheduledFuture[_] =
    executor.schedule(new QueryKiller(query), query.getTimeout, TimeUnit.MILLISECONDS)

  /**
    * Trait for classes to be managed for timeouts
    */
  trait ManagedQuery extends Closeable {
    def getTimeout: Long
    def isClosed: Boolean
    def debug: String
  }

  private class QueryKiller(query: ManagedQuery) extends Runnable {
    override def run(): Unit = {
      if (!query.isClosed) {
        logger.warn(s"Stopping ${query.debug} based on timeout of ${query.getTimeout}ms")
        try { query.close() } catch {
          case NonFatal(e) => logger.warn("Error cancelling query:", e)
        }
      }
    }
  }
}
