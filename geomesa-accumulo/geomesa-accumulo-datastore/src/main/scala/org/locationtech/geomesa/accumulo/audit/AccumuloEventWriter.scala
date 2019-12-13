/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.audit

import java.io.Closeable
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{ScheduledThreadPoolExecutor, TimeUnit}

import com.google.common.util.concurrent.MoreExecutors
import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.client.{BatchWriter, Connector}
import org.apache.accumulo.core.data.Mutation
import org.locationtech.geomesa.accumulo.AccumuloVersion
import org.locationtech.geomesa.accumulo.util.GeoMesaBatchWriterConfig
import org.locationtech.geomesa.utils.audit.AuditedEvent
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty

/**
 * Manages writing of usage stats in a background thread.
 */
class AccumuloEventWriter(connector: Connector, table: String) extends Runnable with Closeable with LazyLogging {

  private val delay = AccumuloEventWriter.WriteInterval.toDuration.get.toMillis

  logger.trace(s"Scheduling audit writer for ${delay}ms")

  private val schedule = AccumuloEventWriter.executor.scheduleWithFixedDelay(this, delay, delay, TimeUnit.MILLISECONDS)

  private val batchWriterConfig = GeoMesaBatchWriterConfig().setMaxMemory(10000L).setMaxWriteThreads(5)

  private var maybeWriter: BatchWriter = _

  private val running = new AtomicBoolean(true)

  private val queue = new java.util.concurrent.ConcurrentLinkedQueue[() => Mutation]

  /**
    * Queues a stat for writing
    */
  def queueStat[T <: AuditedEvent](event: T)(implicit transform: AccumuloEventTransform[T]): Unit =
    queue.offer(() => transform.toMutation(event))

  override def run(): Unit = {
    var toMutation = queue.poll()
    if (toMutation != null) {
      val writer = getWriter
      do {
        writer.addMutation(toMutation())
        toMutation = queue.poll()
      } while (toMutation != null && running.get)
      writer.flush()
    }
  }

  override def close(): Unit = {
    running.set(false)
    schedule.cancel(false)
    synchronized {
      if (maybeWriter != null) {
        maybeWriter.close()
      }
    }
  }

  private def getWriter: BatchWriter = synchronized {
    if (maybeWriter == null) {
      AccumuloVersion.createTableIfNeeded(connector, table)
      maybeWriter = connector.createBatchWriter(table, batchWriterConfig)
    }
    maybeWriter
  }
}

object AccumuloEventWriter {

  val WriteInterval = SystemProperty("geomesa.accumulo.audit.interval", "5 seconds")

  private val executor = MoreExecutors.getExitingScheduledExecutorService(new ScheduledThreadPoolExecutor(5))
  sys.addShutdownHook(executor.shutdownNow())
}
