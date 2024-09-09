/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.audit

import com.google.gson.GsonBuilder
import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.client.{AccumuloClient, BatchWriter}
import org.apache.accumulo.core.data.{Mutation, Value}
import org.apache.hadoop.io.Text
import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.accumulo.data.AccumuloQueryPlan
import org.locationtech.geomesa.accumulo.util.{GeoMesaBatchWriterConfig, TableManager}
import org.locationtech.geomesa.index.audit.QueryEvent
import org.locationtech.geomesa.index.geoserver.ViewParams
import org.locationtech.geomesa.index.geotools.GeoMesaFeatureReader
import org.locationtech.geomesa.utils.audit.AuditedEvent
import org.locationtech.geomesa.utils.concurrent.ExitingExecutor
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes

import java.io.Closeable
import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{ScheduledThreadPoolExecutor, TimeUnit}

/**
 * Manages writing of usage stats in a background thread.
 */
class AccumuloEventWriter(connector: AccumuloClient, table: String) extends Runnable with Closeable with LazyLogging {

  import AccumuloEventWriter.gson

  import scala.collection.JavaConverters._

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
  def queueStat[T <: AuditedEvent](event: T)(implicit transform: AccumuloEventTransform[T]): Unit = {
    val plansAndHints = GeoMesaFeatureReader.EventData.get()
    val (plans, hints, start) = if (plansAndHints == null) { (null, null, -1L) } else { plansAndHints }
    event match {
      case q: QueryEvent =>
        queue.offer(() => toMutation(q, plans.asInstanceOf[Seq[AccumuloQueryPlan]], hints, start))
      case _ =>
        queue.offer(() => transform.toMutation(event))
    }
  }

  private def toMutation(event: QueryEvent, plans: Seq[AccumuloQueryPlan], hints: Hints, start: Long): Mutation = {
    val cf = AccumuloQueryEventTransform.createRandomColumnFamily
    val mutation = AccumuloQueryEventTransform.toMutation(event, cf)
    if (hints != null) {
      // overwrite old hints string with json object
      val readableHints = new java.util.TreeMap[String, String]() // use tree map for consistent ordering by key
      hints.asScala.map { case (k: Hints.Key, v) =>
        val key = ViewParams.hintToString(k)
        val value = v match {
          case null => "null"
          case sft: SimpleFeatureType => SimpleFeatureTypes.encodeType(sft)
          case s => s.toString
        }
        readableHints.put(key, value)
      }
      mutation.put(cf, AccumuloQueryEventTransform.CQ_HINTS, new Value(gson.toJson(readableHints).getBytes(StandardCharsets.UTF_8)))
    }
    if (start != -1) {
      mutation.put(cf, AccumuloEventWriter.CqStart, new Value(start.toString.getBytes(StandardCharsets.UTF_8)))
    }
    mutation.put(cf, AccumuloEventWriter.CqEnd, new Value(event.date.toString.getBytes(StandardCharsets.UTF_8)))
    if (plans != null) {
      mutation.put(cf, AccumuloEventWriter.CqMetadata, new Value(gson.toJson(createMetadata(plans)).getBytes(StandardCharsets.UTF_8)))
    }
    mutation
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
        tables.put(table, connector.tableOperations().tableIdMap().get(table))
        val locate = connector.tableOperations().locate(table, plan.ranges.asJava)
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

  override def run(): Unit = {
    var toMutation = queue.poll()
    if (toMutation != null) {
      val writer = getWriter
      while (toMutation != null && running.get) {
        writer.addMutation(toMutation())
        toMutation = queue.poll()
      }
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
      new TableManager(connector).ensureTableExists(table)
      maybeWriter = connector.createBatchWriter(table, batchWriterConfig)
    }
    maybeWriter
  }
}

object AccumuloEventWriter {

  val WriteInterval: SystemProperty = SystemProperty("geomesa.accumulo.audit.interval", "5 seconds")

  private val executor = ExitingExecutor(new ScheduledThreadPoolExecutor(5), force = true)

  private val gson = new GsonBuilder().disableHtmlEscaping().serializeNulls().create()

  private val CqMetadata = new Text("metadata")
  private val CqStart    = new Text("start")
  private val CqEnd      = new Text("end")
}
