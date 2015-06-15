/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/
package org.locationtech.geomesa.accumulo.iterators

import java.util.{Map => JMap, UUID}

import org.apache.accumulo.core.client.mapreduce.InputFormatBase
import org.apache.accumulo.core.client.{IteratorSetting, ScannerBase}
import org.apache.accumulo.core.data.{Key, Value}
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SkippingIterator, SortedKeyValueIterator}
import org.apache.hadoop.mapreduce.Job
import org.locationtech.geomesa.accumulo.iterators.TimestampSetIterator._

import scala.collection.SortedSet

object TimestampSetIterator {
  def setupIterator(scanner: ScannerBase, timestampLongs: Long*) {
    val iteratorName: String = "tsi-" + UUID.randomUUID.toString
    val cfg = new IteratorSetting(10, iteratorName, classOf[TimestampSetIterator])
    cfg.addOption(timestampsOption, timestampLongs.map(_.toString).mkString(";"))
    scanner.addScanIterator(cfg)
  }

  def setupIterator(job: Job, timestampLongs: Long*) {
    val iteratorName: String = "tsi-" + UUID.randomUUID.toString
    val cfg = new IteratorSetting(10, iteratorName, classOf[TimestampSetIterator])
    cfg.addOption(timestampsOption, timestampLongs.map(_.toString).mkString(";"))
    InputFormatBase.addIterator(job, cfg)
  }

  final val timestampsOption: String = "timestampsOption"
}


class TimestampSetIterator(var timestamps: SortedSet[Long])
    extends SkippingIterator {
  @Override
  def this() = this(null)

  @Override
  override protected def consume() {
    while (getSource.hasTop && !isValid(getSource.getTopKey)) {
      getSource.next()
    }
  }

  private def isValid(topKey: Key): Boolean = timestamps.contains(topKey.getTimestamp)

  @Override
  override def deepCopy(env: IteratorEnvironment): SortedKeyValueIterator[Key, Value] = {
    throw new UnsupportedOperationException
  }

  @Override
  override def init(source: SortedKeyValueIterator[Key, Value],
                    options: JMap[String, String],
                    env: IteratorEnvironment) {
    super.init(source, options, env)
    timestamps = SortedSet(options.get(timestampsOption).split(";").map(_.toLong):_*)
  }
}
