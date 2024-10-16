/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.audit

import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.client.Scanner
import org.apache.accumulo.core.data.{Key, Mutation, Range, Value}
import org.apache.hadoop.io.Text
import org.locationtech.geomesa.accumulo.audit.AccumuloEventTransform.RowGrouper
import org.locationtech.geomesa.index.audit.AuditedEvent
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.io.CloseQuietly
import org.locationtech.geomesa.utils.text.DateParsing

import java.time.format.DateTimeFormatter
import java.time.{ZoneOffset, ZonedDateTime}
import java.util.Map.Entry
import scala.util.Random
import scala.util.control.NonFatal

/**
 * Trait for mapping stats to accumulo and back
 */
trait AccumuloEventTransform[T <: AuditedEvent] extends LazyLogging {

  import AccumuloEventTransform.toRowKey

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
   * @param typeName type name to scan
   * @param dates dates to scan
   * @return
   */
  def iterator(scanner: Scanner, typeName: String, dates: (ZonedDateTime, ZonedDateTime)): CloseableIterator[T] = {
    try {
      scanner.setRange(new Range(toRowKey(typeName, dates._1), true, toRowKey(typeName, dates._2), false))
      new RowGrouper(scanner).flatMap(_.groupBy(_.getKey.getColumnFamily).values).map(toEvent)
    } catch {
      case NonFatal(e) =>
        CloseQuietly(scanner).foreach(e.addSuppressed)
        throw e
    }
  }
}

object AccumuloEventTransform {

  val DateFormat: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd-HH:mm:ss.SSS").withZone(ZoneOffset.UTC)

  private val RowId = "(.*)~(.*)".r

  /**
   * Extract type name and date from a row key
   *
   * @param key row key
   * @return
   */
  private[audit] def typeNameAndDate(key: Key): (String, Long) = {
    val RowId(typeName, dateString) = key.getRow.toString
    val date = ZonedDateTime.parse(dateString, DateFormat).toInstant.toEpochMilli
    (typeName, date)
  }

  /**
   * Create a row key from a type name and date
   *
   * @param typeName feature type name
   * @param date event date
   * @return
   */
  private[audit] def toRowKey(typeName: String, date: Long): String =
    s"$typeName~${DateParsing.formatMillis(date, DateFormat)}"

  /**
   * Create a row key from a type name and date
   *
   * @param typeName feature type name
   * @param date event date
   * @return
   */
  private[audit] def toRowKey(typeName: String, date: ZonedDateTime): String =
    s"$typeName~${DateParsing.format(date, DateFormat)}"

  /**
   * Create a random col family
   *
   * @return
   */
  private[audit] def createRandomColumnFamily: Text = new Text("%1$04d".format(Random.nextInt(10000)))

  /**
   * Groups scan entries by row
   *
   * @param scanner scanner
   */
  private class RowGrouper(scanner: Scanner) extends CloseableIterator[Seq[Entry[Key, Value]]] {

    private val iter = scanner.iterator()
    private var nextEntry: Entry[Key, Value] = if (iter.hasNext) { iter.next() } else { null }

    override def hasNext: Boolean = nextEntry != null

    override def next(): Seq[Entry[Key, Value]] = {
      if (nextEntry == null) {
        return null
      }
      val currentRowKey = nextEntry.getKey.getRow.toString
      val entries = Seq.newBuilder[Entry[Key, Value]]
      entries += nextEntry
      var currentEntry: Entry[Key, Value] = null
      while (iter.hasNext && { currentEntry = iter.next(); currentEntry.getKey.getRow.toString == currentRowKey }) {
        entries += currentEntry
        currentEntry = null
      }
      nextEntry = currentEntry
      entries.result()
    }

    override def close(): Unit = scanner.close()
  }
}
