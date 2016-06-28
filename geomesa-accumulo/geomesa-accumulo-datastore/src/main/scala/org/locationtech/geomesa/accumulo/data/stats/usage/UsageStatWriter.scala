/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.data.stats.usage

import java.io.Closeable
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{ScheduledThreadPoolExecutor, TimeUnit}

import com.google.common.util.concurrent.MoreExecutors
import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.client.{BatchWriter, Connector}
import org.apache.accumulo.core.data.Mutation
import org.locationtech.geomesa.accumulo.AccumuloVersion
import org.locationtech.geomesa.accumulo.util.GeoMesaBatchWriterConfig

/**
 * Manages writing of usage stats in a background thread.
 */
class UsageStatWriter(connector: Connector, table: String) extends Runnable with Closeable with LazyLogging {

  // initial schedule
  UsageStatWriter.executor.schedule(this, writeDelayMillis, TimeUnit.MILLISECONDS)

  private val writeDelayMillis = 5000

  private val batchWriterConfig = GeoMesaBatchWriterConfig().setMaxMemory(10000L).setMaxWriteThreads(5)

  private var maybeWriter: BatchWriter = null

  private val running = new AtomicBoolean(true)

  private val queue = new java.util.concurrent.ConcurrentLinkedQueue[() => Mutation]

  /**
   * Queues a stat for writing
   */
  def queueStat[T <: UsageStat](stat: T)(implicit transform: UsageStatTransform[T]): Unit =
    queue.offer(() => transform.statToMutation(stat))

  override def run() = {
    var toMutation = queue.poll()
    if (toMutation != null) {
      val writer = getWriter
      do {
        writer.addMutation(toMutation())
        toMutation = queue.poll()
      } while (toMutation != null && running.get)
      writer.flush()
    }

    if (running.get) {
      UsageStatWriter.executor.schedule(this, writeDelayMillis, TimeUnit.MILLISECONDS)
    }
  }

  override def close(): Unit = {
    running.set(false)
    synchronized {
      if (maybeWriter != null) {
        maybeWriter.close()
      }
    }
  }

  private def getWriter: BatchWriter = synchronized {
    if (maybeWriter == null) {
      AccumuloVersion.ensureTableExists(connector, table)
      maybeWriter = connector.createBatchWriter(table, batchWriterConfig)
    }
    maybeWriter
  }
}

object UsageStatWriter {
  private val executor = MoreExecutors.getExitingScheduledExecutorService(new ScheduledThreadPoolExecutor(5))
  sys.addShutdownHook(executor.shutdownNow())
}
