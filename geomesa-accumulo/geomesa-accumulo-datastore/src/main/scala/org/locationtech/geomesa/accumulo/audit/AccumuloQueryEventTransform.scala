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
import org.locationtech.geomesa.index.audit.AuditedEvent.QueryEvent

import java.nio.charset.StandardCharsets
import java.util.Map.Entry

/**
 * Maps query stats to accumulo
 */
object AccumuloQueryEventTransform extends AccumuloEventTransform[QueryEvent] {

  private val CqUser     = new Text("user")
  private val CqFilter   = new Text("queryFilter")
  private val CqHints    = new Text("queryHints")
  private val CqPlanTime = new Text("timePlanning")
  private val CqScanTime = new Text("timeScanning")
  private val CqTime     = new Text("timeTotal")
  private val CqHits     = new Text("hits")

  override def toMutation(event: QueryEvent): Mutation = {
    val mutation = new Mutation(AccumuloEventTransform.toRowKey(event.typeName, event.date))
    // avoids collisions if timestamp is the same, not 100% foolproof but good enough
    val cf = AccumuloEventTransform.createRandomColumnFamily
    mutation.put(cf, CqUser,     new Value(event.user.getBytes(StandardCharsets.UTF_8)))
    mutation.put(cf, CqFilter,   new Value(event.filter.getBytes(StandardCharsets.UTF_8)))
    mutation.put(cf, CqHints,    new Value(event.hints.getBytes(StandardCharsets.UTF_8)))
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

    val (featureName, date) = AccumuloEventTransform.typeNameAndDate(entries.head.getKey)

    var user = "unknown"
    var queryHints = ""
    var queryFilter = ""
    var planTime = 0L
    var scanTime = 0L
    var hits = 0L

    entries.foreach { e =>
      e.getKey.getColumnQualifier match {
        case CqUser     => user = e.getValue.toString
        case CqFilter   => queryFilter = e.getValue.toString
        case CqHints    => queryHints = e.getValue.toString
        case CqPlanTime => planTime = e.getValue.toString.stripSuffix("ms").toLong
        case CqScanTime => scanTime = e.getValue.toString.stripSuffix("ms").toLong
        case CqHits     => hits = e.getValue.toString.toLong
        case CqTime     => // time is an aggregate, doesn't need to map back to anything
        case _          => logger.warn(s"Unmapped entry in query stat: ${e.getKey.getColumnQualifier.toString}")
      }
    }

    QueryEvent(StoreType, featureName, date, user, queryFilter, queryHints, planTime, scanTime, hits)
  }
}
