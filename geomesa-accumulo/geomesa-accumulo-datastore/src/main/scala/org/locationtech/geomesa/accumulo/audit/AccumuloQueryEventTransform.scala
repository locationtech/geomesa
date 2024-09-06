/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.audit

import org.apache.accumulo.core.data.{Key, Mutation, Value}
import org.apache.hadoop.io.Text
import org.locationtech.geomesa.index.audit.AuditWriter
import org.locationtech.geomesa.index.audit.AuditedEvent.QueryEvent

import java.nio.charset.StandardCharsets
import java.util.Collections
import java.util.Map.Entry

/**
 * Maps query stats to accumulo
 */
object AccumuloQueryEventTransform extends AccumuloEventTransform[QueryEvent] {

  import scala.collection.JavaConverters._

  private val HintRegex = "([A-Z_]+|unknown_hint)=(.*?)(, |$)".r

  private val CqUser     = new Text("user")
  private val CqFilter   = new Text("queryFilter")
  private val CqHints    = new Text("queryHints")
  private val CqMetadata = new Text("metadata")
  private val CqStart    = new Text("start")
  private val CqEnd      = new Text("end")
  private val CqPlanTime = new Text("timePlanning")
  private val CqScanTime = new Text("timeScanning")
  private val CqTime     = new Text("timeTotal")
  private val CqHits     = new Text("hits")

  override def toMutation(event: QueryEvent): Mutation = {
    val mutation = new Mutation(AccumuloEventTransform.toRowKey(event.typeName, event.end))
    // avoids collisions if timestamp is the same, not 100% foolproof but good enough
    val cf = AccumuloEventTransform.createRandomColumnFamily
    mutation.put(cf, CqUser,     new Value(event.user.getBytes(StandardCharsets.UTF_8)))
    mutation.put(cf, CqFilter,   new Value(event.filter.getBytes(StandardCharsets.UTF_8)))
    mutation.put(cf, CqHints,    new Value(AuditWriter.Gson.toJson(event.hints).getBytes(StandardCharsets.UTF_8)))
    mutation.put(cf, CqMetadata, new Value(AuditWriter.Gson.toJson(event.metadata).getBytes(StandardCharsets.UTF_8)))
    mutation.put(cf, CqStart,    new Value(event.start.toString.getBytes(StandardCharsets.UTF_8)))
    mutation.put(cf, CqEnd,      new Value(event.end.toString.getBytes(StandardCharsets.UTF_8)))
    mutation.put(cf, CqPlanTime, new Value(s"${event.planTime}ms".getBytes(StandardCharsets.UTF_8)))
    mutation.put(cf, CqScanTime, new Value(s"${event.scanTime}ms".getBytes(StandardCharsets.UTF_8)))
    mutation.put(cf, CqTime,     new Value(s"${event.scanTime + event.planTime}ms".getBytes(StandardCharsets.UTF_8)))
    mutation.put(cf, CqHits,     new Value(event.hits.toString.getBytes(StandardCharsets.UTF_8)))
    mutation
  }

  override def toEvent(entries: Iterable[Entry[Key, Value]]): QueryEvent = {
    if (entries.isEmpty) {
      return null
    }

    val (featureName, end) = AccumuloEventTransform.typeNameAndDate(entries.head.getKey)

    var user = "unknown"
    var hints = Collections.emptyMap[String, String]()
    var metadata = Collections.emptyMap[String, AnyRef]()
    var filter = ""
    var start = -1L
    var planTime = 0L
    var scanTime = 0L
    var hits = 0L

    entries.foreach { e =>
      e.getKey.getColumnQualifier match {
        case CqUser     => user = e.getValue.toString
        case CqFilter   => filter = e.getValue.toString
        case CqHints    => hints = parseHints(e.getValue.toString)
        case CqMetadata => metadata = AuditWriter.Gson.fromJson(e.getValue.toString, classOf[java.util.Map[String, AnyRef]])
        case CqStart    => start = e.getValue.toString.toLong
        case CqEnd      => // end is already extracted from the row key
        case CqPlanTime => planTime = e.getValue.toString.stripSuffix("ms").toLong
        case CqScanTime => scanTime = e.getValue.toString.stripSuffix("ms").toLong
        case CqHits     => hits = e.getValue.toString.toLong
        case CqTime     => // time is an aggregate, doesn't need to map back to anything
        case _          => logger.warn(s"Unmapped entry in query stat: ${e.getKey.getColumnQualifier.toString}")
      }
    }

    QueryEvent(StoreType, featureName, user, filter, hints, metadata, start, end, planTime, scanTime, hits)
  }

  private def parseHints(value: String): java.util.Map[String, String] = {
    if (value.isEmpty) {
      Collections.emptyMap()
    } else if (value.charAt(0) == '{') {
      AuditWriter.Gson.fromJson(value, classOf[java.util.Map[String, String]])
    } else {
      // old format, do our best to parse it out into distinct hints
      HintRegex.findAllMatchIn(value).map(m => m.group(1) -> m.group(2)).toMap.asJava
    }
  }
}
