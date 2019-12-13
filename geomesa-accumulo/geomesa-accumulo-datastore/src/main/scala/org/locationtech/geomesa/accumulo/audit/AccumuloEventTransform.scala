/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.audit

import java.time.format.DateTimeFormatter
import java.time.{ZoneOffset, ZonedDateTime}
import java.util.Map.Entry

import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.client.Scanner
import org.apache.accumulo.core.data.{Key, Mutation, Value}
import org.apache.hadoop.io.Text
import org.locationtech.geomesa.utils.audit.AuditedEvent
import org.locationtech.geomesa.utils.collection.{CloseableIterator, SelfClosingIterator}
import org.locationtech.geomesa.utils.text.DateParsing

import scala.util.Random

/**
 * Trait for mapping stats to accumulo and back
 */
trait AccumuloEventTransform[T <: AuditedEvent] extends LazyLogging {

  import AccumuloEventTransform.dateFormat

  private val RowId = "(.*)~(.*)".r

  protected def createMutation(stat: T): Mutation =
    new Mutation(s"${stat.typeName}~${DateParsing.formatMillis(stat.date, dateFormat)}")

  protected def typeNameAndDate(key: Key): (String, Long) = {
    val RowId(typeName, dateString) = key.getRow.toString
    val date = ZonedDateTime.parse(dateString, dateFormat).toInstant.toEpochMilli
    (typeName, date)
  }

  protected def createRandomColumnFamily: Text = new Text(Random.nextInt(9999).formatted("%1$04d"))

  /**
   * Convert an event to a mutation
   *
   * @param event event
   * @return
   */
  def toMutation(event: T): Mutation

  /**
   * Convert accumulo scan results into an event
   *
   * @param entries scan entries from a single row
   * @return
   */
  def toEvent(entries: Iterable[Entry[Key, Value]]): T

  /**
   * Creates an iterator that returns Stats from accumulo scans
   *
   * @param scanner accumulo scanner over stored events
   * @return
   */
  def iterator(scanner: Scanner): CloseableIterator[T] = {
    val iter = scanner.iterator()

    val wrappedIter = new CloseableIterator[T] {

      var last: Option[Entry[Key, Value]] = None

      override def close(): Unit = scanner.close()

      override def next(): T = {
        // get the data for the stat entry, which consists of a several CQ/values
        val entries = collection.mutable.ListBuffer.empty[Entry[Key, Value]]
        if (last.isEmpty) {
          last = Some(iter.next())
        }
        val lastRowKey = last.get.getKey.getRow.toString
        var next: Option[Entry[Key, Value]] = last
        while (next.isDefined && next.get.getKey.getRow.toString == lastRowKey) {
          entries.append(next.get)
          next = if (iter.hasNext) Some(iter.next()) else None
        }
        last = next
        // use the row data to return a Stat
        toEvent(entries)
      }

      override def hasNext: Boolean = last.isDefined || iter.hasNext
    }
    SelfClosingIterator(wrappedIter)
  }
}

object AccumuloEventTransform {
  val dateFormat: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd-HH:mm:ss.SSS").withZone(ZoneOffset.UTC)
}
