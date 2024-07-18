/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.audit

import org.apache.accumulo.core.client.{AccumuloClient, BatchWriter}
import org.locationtech.geomesa.accumulo.audit.AccumuloAuditWriter.ShutdownTimeout
import org.locationtech.geomesa.accumulo.util.{GeoMesaBatchWriterConfig, TableManager}
import org.locationtech.geomesa.index.audit.AuditWriter.AuditLogger
import org.locationtech.geomesa.index.audit.AuditedEvent
import org.locationtech.geomesa.index.audit.AuditedEvent.QueryEvent
import org.locationtech.geomesa.utils.audit.AuditProvider
import org.locationtech.geomesa.utils.concurrent.ExitingExecutor
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty

import java.io.Closeable
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.{ScheduledThreadPoolExecutor, TimeUnit}
import scala.concurrent.{Future, Promise}
import scala.util.Try
import scala.util.control.NonFatal

/**
 * Audit writer that persists log entries to a table in Accumulo
 *
 * @param client accumulo client
 * @param table table to write to
 * @param auditProvider audit provider
 * @param enabled enable table writes (entries will always be written to logs)
 */
class AccumuloAuditWriter(
    client: AccumuloClient,
    val table: String,
    auditProvider: AuditProvider,
    enabled: Boolean
  ) extends AuditLogger(StoreType, auditProvider) with Runnable with Closeable {

  private var writer: BatchWriter = _

  private val queue = new java.util.concurrent.ConcurrentLinkedQueue[(Promise[Unit], QueryEvent)]

  private val running = new AtomicBoolean(enabled)
  private val writeLock = new ReentrantLock()

  private val timeout = ShutdownTimeout.toDuration.get.toMillis

  private val scheduledRun =
    AccumuloAuditWriter.WriteInterval.toDuration.collect {
      case d if enabled && d.isFinite() =>
        val millis = d.toMillis
        logger.debug(s"Scheduling audit writer for ${millis}ms")
        AccumuloAuditWriter.executor.scheduleWithFixedDelay(this, millis, millis, TimeUnit.MILLISECONDS)
    }

  override protected def write(event: AuditedEvent.QueryEvent): Future[Unit] = {
    val log = super.write(event)
    if (running.get()) {
      val promise = Promise[Unit]()
      queue.offer(promise -> event) // unbounded queue so will never fail
      promise.future
    } else {
      log
    }
  }

  override def run(): Unit = {
    if (running.get() && writeLock.tryLock()) {
      try { writeQueuedEvents() } finally {
        writeLock.unlock()
      }
    }
  }

  private def writeQueuedEvents(): Unit = {
    try {
      var promiseAndEvent = queue.poll()
      if (promiseAndEvent != null) {
        val stopTime = System.currentTimeMillis() + timeout
        if (writer == null) {
          new TableManager(client).ensureTableExists(table)
          val writeConfig = GeoMesaBatchWriterConfig().setMaxMemory(10000L).setMaxWriteThreads(5)
          writer = client.createBatchWriter(table, writeConfig)
        }
        while (promiseAndEvent != null) {
          val (promise, event) = promiseAndEvent
          promise.complete(Try(writer.addMutation(AccumuloQueryEventTransform.toMutation(event))))
          promiseAndEvent = if (running.get() || System.currentTimeMillis() < stopTime) { queue.poll() } else { null }
        }
        writer.flush()
      }
    } catch {
      case NonFatal(e) => logger.error("Error writing audit logs:", e)
    }
  }

  override def close(): Unit = {
    if (running.compareAndSet(true, false)) {
      scheduledRun.foreach(_.cancel(false))
      try {
        if (writeLock.tryLock()) {
          // not currently running, so clean up any remaining events
          writeQueuedEvents()
        } else {
          // currently running, wait for run to end
          writeLock.lock()
        }
        if (writer != null) {
          writer.close()
        }
      } finally {
        writeLock.unlock()
      }
    }
  }
}

object AccumuloAuditWriter {

  val WriteInterval: SystemProperty = SystemProperty("geomesa.accumulo.audit.interval", "5 seconds")
  val ShutdownTimeout: SystemProperty = SystemProperty("geomesa.accumulo.audit.shutdown.timeout", "5 seconds")

  private val executor = ExitingExecutor(new ScheduledThreadPoolExecutor(5), force = true)
}
