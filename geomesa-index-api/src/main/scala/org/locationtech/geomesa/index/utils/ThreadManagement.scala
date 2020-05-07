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
import java.util.function.Consumer

import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.utils.concurrent.ExitingExecutor

import scala.util.control.NonFatal

/**
 * Singleton for registering and managing running queries.
 */
object ThreadManagement extends LazyLogging {

  private val executor = {
    val ex = new ScheduledThreadPoolExecutor(2)
    ex.setRemoveOnCancelPolicy(true)
    ExitingExecutor(ex, force = true)
  }

  if (logger.underlying.isTraceEnabled) {
    executor.scheduleAtFixedRate(QueryReporter, 0l, 5l, TimeUnit.SECONDS)

  }

  /**
   * Register a query with the thread manager
   */
  def register(query: ManagedQuery): ScheduledFuture[_] = {
      executor.schedule(new QueryKiller(query), query.getTimeout, TimeUnit.MILLISECONDS)
    }

  /**
    * Trait for classes to be managed for timeouts
    */
  trait ManagedQuery {
    def getTimeout: Long
    def isClosed: Boolean
    def debug: String
    def interrupt: Unit
  }

  private lazy val QueryReporter: Runnable = new Runnable {
    override def run(): Unit = {
      val queue = executor.getQueue
      logger.trace(s"ThreadManagement has a queue of ${queue.size} queries.")

      import scala.collection.JavaConversions._
      executor.getQueue.foreach { runnable =>
        runnable match {
          case qk: QueryKiller => logQuery(qk)
          case _ => logger.trace(s"Queue has $runnable")
        }
      }

        executor.getQueue.forEach(new Consumer[Runnable] {
          override def accept(t: Runnable): Unit = t match {
            case qk: QueryKiller => logQuery(qk)
            case _ => // This reporter may be in the queue
          }
        })
    }

    private def logQuery(qk: QueryKiller): Unit = {

      logger.trace(s"Query started at ${qk.start} has been running for " +
        s"${(System.currentTimeMillis()-qk.start)/1000.0} seconds for filter ${qk.query.debug}")
    }
  }




  private class QueryKiller(val query: ManagedQuery) extends Runnable {
    val start: Long = System.currentTimeMillis()

    override def run(): Unit = {
      if (!query.isClosed) {
        logger.warn(s"Stopping ${query.debug} based on timeout of ${query.getTimeout}ms")
        try { query.stop() } catch {
          case NonFatal(e) => logger.warn("Error cancelling query:", e)
        }
      }
    }
  }
}
