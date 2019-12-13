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
import org.locationtech.geomesa.utils.audit.DeletableEvent

case class SerializedQueryEvent(storeType: String,
                                typeName: String,
                                date:     Long,
                                deleted:  Boolean,
                                entries:  Map[(Text, Text), Value]) extends DeletableEvent {
  lazy val user = entries.find(_._1._2 == AccumuloQueryEventTransform.CQ_USER).map(_._2.toString).getOrElse("unknown")
}

object SerializedQueryEventTransform extends AccumuloEventTransform[SerializedQueryEvent] {

  import AccumuloQueryEventTransform.CQ_DELETED

  override def toMutation(event: SerializedQueryEvent): Mutation = {
    val mutation = createMutation(event)
    event.entries.foreach { case ((cf, cq), v) => mutation.put(cf, cq, v) }
    mutation.put(event.entries.head._1._1, CQ_DELETED, new Value(event.deleted.toString.getBytes(StandardCharsets.UTF_8)))
    mutation
  }

  override def toEvent(entries: Iterable[Entry[Key, Value]]): SerializedQueryEvent = {
    val (typeName, date) = typeNameAndDate(entries.head.getKey)
    val kvs = entries.map(e => (e.getKey.getColumnFamily, e.getKey.getColumnQualifier) -> e.getValue)
    val (delete, others) = kvs.partition(_._1._2 == CQ_DELETED)
    // noinspection MapGetOrElseBoolean
    val deleted = delete.headOption.map(_._2.toString.toBoolean).getOrElse(false)
    SerializedQueryEvent(AccumuloAuditService.StoreType, typeName, date, deleted, others.toMap)
  }
}
