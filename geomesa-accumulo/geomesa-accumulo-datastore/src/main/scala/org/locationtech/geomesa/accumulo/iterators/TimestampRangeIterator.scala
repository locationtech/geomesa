/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/
package org.locationtech.geomesa.accumulo.iterators

import java.util.{Date, Map => JMap, UUID}

import org.apache.accumulo.core.client.mapreduce.InputFormatBase
import org.apache.accumulo.core.client.{IteratorSetting, ScannerBase}
import org.apache.accumulo.core.data.{Key, Value}
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SkippingIterator, SortedKeyValueIterator}
import org.apache.hadoop.mapreduce.Job
import org.locationtech.geomesa.accumulo.iterators.TimestampRangeIterator._

import scala.collection.JavaConverters._

object TimestampRangeIterator {
  private val defaultPriority = 1

  def setupIterator(scanner: ScannerBase, startTime: Date, endTime: Date, priority: Int) {
    val iteratorName: String = "tri-" + UUID.randomUUID.toString
    val cfg = new IteratorSetting(priority, iteratorName, classOf[TimestampRangeIterator])
    cfg.addOptions(Map(startOption -> (startTime.getTime / 1000).toString,
                       endOption   -> (endTime.getTime / 1000).toString).asJava)
    scanner.addScanIterator(cfg)
  }

  def setupIterator(scanner: ScannerBase, startTime: Date, endTime: Date) {
    setupIterator(scanner, startTime, endTime, defaultPriority)
  }

  def setupIterator(job: Job, startTime: Date, endTime: Date, priority: Int) {
    val iteratorName: String = "tri-" + UUID.randomUUID.toString
    val cfg = new IteratorSetting(priority, iteratorName, classOf[TimestampRangeIterator])
    cfg.addOptions(Map(startOption -> (startTime.getTime / 1000).toString,
                       endOption   -> (endTime.getTime / 1000).toString).asJava)
    InputFormatBase.addIterator(job, cfg)
  }

  def setupIterator(job: Job, startTime: Date, endTime: Date) {
    setupIterator(job, startTime, endTime, defaultPriority)
  }

  var startOption: String = "startOption"
  var endOption: String = "endOption"
}


class TimestampRangeIterator(var start: Long, var end: Long)
    extends SkippingIterator {
  def this() = this(0, Long.MaxValue)

  @Override
  override protected def consume() {
    while (getSource.hasTop && !withinRange(getSource.getTopKey)) {
      getSource.next()
    }
  }

  private def withinRange(topKey: Key): Boolean = {
    topKey.getTimestamp >= start && topKey.getTimestamp <= end
  }

  @Override
  override def deepCopy(env: IteratorEnvironment): SortedKeyValueIterator[Key, Value] = {
    throw new UnsupportedOperationException
  }

  @Override
  override def init(source: SortedKeyValueIterator[Key, Value],
                    options: JMap[String, String],
                    env: IteratorEnvironment) {
    super.init(source, options, env)
    start = options.get(startOption).toLong
    end =   options.get(endOption).toLong
  }
}
