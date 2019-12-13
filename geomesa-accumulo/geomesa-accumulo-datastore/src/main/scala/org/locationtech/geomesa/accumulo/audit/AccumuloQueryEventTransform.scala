/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.audit

import java.nio.charset.StandardCharsets
import java.util.Map.Entry

import org.apache.accumulo.core.data.{Key, Mutation, Value}
import org.apache.hadoop.io.Text
import org.locationtech.geomesa.index.audit.QueryEvent

/**
 * Maps query stats to accumulo
 */
object AccumuloQueryEventTransform extends AccumuloEventTransform[QueryEvent] {

  private [audit] val CQ_USER     = new Text("user")
  private [audit] val CQ_FILTER   = new Text("queryFilter")
  private [audit] val CQ_HINTS    = new Text("queryHints")
  private [audit] val CQ_PLANTIME = new Text("timePlanning")
  private [audit] val CQ_SCANTIME = new Text("timeScanning")
  private [audit] val CQ_TIME     = new Text("timeTotal")
  private [audit] val CQ_HITS     = new Text("hits")
  private [audit] val CQ_DELETED  = new Text("deleted")

  override def toMutation(event: QueryEvent): Mutation = {
    val mutation = createMutation(event)
    val cf = createRandomColumnFamily
    mutation.put(cf, CQ_USER,     new Value(event.user.getBytes(StandardCharsets.UTF_8)))
    mutation.put(cf, CQ_FILTER,   new Value(event.filter.getBytes(StandardCharsets.UTF_8)))
    mutation.put(cf, CQ_HINTS,    new Value(event.hints.getBytes(StandardCharsets.UTF_8)))
    mutation.put(cf, CQ_PLANTIME, new Value(s"${event.planTime}ms".getBytes(StandardCharsets.UTF_8)))
    mutation.put(cf, CQ_SCANTIME, new Value(s"${event.scanTime}ms".getBytes(StandardCharsets.UTF_8)))
    mutation.put(cf, CQ_TIME,     new Value(s"${event.scanTime + event.planTime}ms".getBytes(StandardCharsets.UTF_8)))
    mutation.put(cf, CQ_HITS,     new Value(event.hits.toString.getBytes(StandardCharsets.UTF_8)))
    mutation.put(cf, CQ_DELETED,  new Value(event.deleted.toString.getBytes(StandardCharsets.UTF_8)))
    mutation
  }

  override def toEvent(entries: Iterable[Entry[Key, Value]]): QueryEvent = {
    if (entries.isEmpty) {
      return null
    }

    val (featureName, date) = typeNameAndDate(entries.head.getKey)
    val values = collection.mutable.Map.empty[Text, Any]

    entries.foreach { e =>
      e.getKey.getColumnQualifier match {
        case CQ_USER     => values.put(CQ_USER, e.getValue.toString)
        case CQ_FILTER   => values.put(CQ_FILTER, e.getValue.toString)
        case CQ_HINTS    => values.put(CQ_HINTS, e.getValue.toString)
        case CQ_PLANTIME => values.put(CQ_PLANTIME, e.getValue.toString.stripSuffix("ms").toLong)
        case CQ_SCANTIME => values.put(CQ_SCANTIME, e.getValue.toString.stripSuffix("ms").toLong)
        case CQ_HITS     => values.put(CQ_HITS, e.getValue.toString.toLong)
        case CQ_DELETED  => values.put(CQ_DELETED, e.getValue.toString.toBoolean)
        case CQ_TIME     => // time is an aggregate, doesn't need to map back to anything
        case _           => logger.warn(s"Unmapped entry in query stat: ${e.getKey.getColumnQualifier.toString}")
      }
    }

    val user = values.getOrElse(CQ_USER, "unknown").asInstanceOf[String]
    val queryHints = values.getOrElse(CQ_HINTS, "").asInstanceOf[String]
    val queryFilter = values.getOrElse(CQ_FILTER, "").asInstanceOf[String]
    val planTime = values.getOrElse(CQ_PLANTIME, 0L).asInstanceOf[Long]
    val scanTime = values.getOrElse(CQ_SCANTIME, 0L).asInstanceOf[Long]
    val hits = values.getOrElse(CQ_HITS, 0L).asInstanceOf[Long]
    val deleted = values.getOrElse(CQ_DELETED, false).asInstanceOf[Boolean]

    QueryEvent(AccumuloAuditService.StoreType, featureName, date, user, queryFilter, queryHints, planTime, scanTime, hits, deleted)
  }
}
