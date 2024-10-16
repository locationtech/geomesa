/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.audit

import com.typesafe.scalalogging.StrictLogging
import org.apache.accumulo.core.client.{AccumuloClient, BatchWriter}
import org.geotools.api.filter.Filter
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.accumulo.audit.AccumuloAuditWriter.ShutdownTimeout
import org.locationtech.geomesa.accumulo.data.AccumuloQueryPlan
import org.locationtech.geomesa.accumulo.util.{GeoMesaBatchWriterConfig, TableManager}
import org.locationtech.geomesa.index.api.QueryPlan
import org.locationtech.geomesa.index.audit.AuditWriter
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
  ) extends AuditWriter(StoreType, auditProvider) with Runnable with Closeable with StrictLogging {

  import scala.collection.JavaConverters._

  private var writer: BatchWriter = _

  private val queue = new java.util.concurrent.ConcurrentLinkedQueue[(Promise[Unit], QueryEvent, Seq[AccumuloQueryPlan])]

  private val running = new AtomicBoolean(true)
  private val writeLock = new ReentrantLock()

  private val timeout = ShutdownTimeout.toDuration.get.toMillis

  private val scheduledRun = {
    val millis = AccumuloAuditWriter.WriteInterval.toDuration.get.toMillis
    logger.debug(s"Scheduling audit writer for ${millis}ms")
    AccumuloAuditWriter.executor.scheduleWithFixedDelay(this, millis, millis, TimeUnit.MILLISECONDS)
  }

  override def writeQueryEvent(
      typeName: String,
      user: String,
      filter: Filter,
      hints: Hints,
      plans: Seq[QueryPlan[_]],
      startTime: Long,
      endTime: Long,
      planTime: Long,
      scanTime: Long,
      hits: Long): Future[Unit] = {
    if (running.get()) {
      val promise = Promise[Unit]()
      // note: we populate the metadata in the asynchronous thread
      val eventNoMeta = QueryEvent(storeType, typeName, user, filter, hints, startTime, endTime, planTime, scanTime, hits)
      queue.offer((promise, eventNoMeta, plans.asInstanceOf[Seq[AccumuloQueryPlan]])) // unbounded queue so will never fail
      promise.future
    } else {
      Future.failed(new RuntimeException("This AuditWriter instance has been closed"))
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
        if (enabled && writer == null) {
          new TableManager(client).ensureTableExists(table)
          val writeConfig = GeoMesaBatchWriterConfig().setMaxMemory(10000L).setMaxWriteThreads(5)
          writer = client.createBatchWriter(table, writeConfig)
        }
        while (promiseAndEvent != null) {
          val (promise, eventNoMeta, plans) = promiseAndEvent
          val write =
            Try {
              val event = eventNoMeta.copy(metadata = createMetadata(plans))
              AuditWriter.log(event)
              if (enabled) {
                writer.addMutation(AccumuloQueryEventTransform.toMutation(event))
              }
            }
          promise.complete(write)
          promiseAndEvent = if (running.get() || System.currentTimeMillis() < stopTime) { queue.poll() } else { null }
        }
        if (enabled) {
          writer.flush()
        }
      }
    } catch {
      case NonFatal(e) => logger.error("Error writing audit logs:", e)
    }
  }

  private def createMetadata(plans: Seq[AccumuloQueryPlan]): java.util.Map[String, AnyRef] = {
    // note: use tree maps for consistent ordering in json output
    val planMeta = plans.map { plan =>
      val planData = new java.util.TreeMap[String, AnyRef]()
      planData.put("index", plan.filter.index.identifier)
      val tables = new java.util.TreeMap[String, String]()
      planData.put("tables", tables)
      val locations = new java.util.TreeMap[String, java.util.Set[String]]()
      planData.put("locations", locations)
      plan.tables.foreach { table =>
        tables.put(table, client.tableOperations().tableIdMap().get(table))
        val locate = client.tableOperations().locate(table, plan.ranges.asJava)
        locate.groupByTablet().asScala.keys.foreach { tablet =>
          val location = locate.getTabletLocation(tablet)
          var tablets = locations.get(location)
          if (tablets == null) {
            tablets = new java.util.TreeSet[String]()
            locations.put(location, tablets)
          }
          tablets.add(tablet.toString)
        }
      }
      planData.put("iterators", plan.iterators.map(_.toString).sorted.asJava)
      plan.join.foreach { case (_, join) => planData.put("join", join.filter.index.identifier) }
      plan.sort.foreach { sort =>
        planData.put("sort", sort.map { case (field, descending) => s"$field:${if (descending) { "desc" } else { "asc" }}"}.asJava)
      }
      plan.maxFeatures.foreach(m => planData.put("limit", Int.box(m)))
      planData
    }
    java.util.Map.of("queryPlans", planMeta.asJava)
  }

  override def close(): Unit = {
    if (running.compareAndSet(true, false)) {
      scheduledRun.cancel(false)
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
