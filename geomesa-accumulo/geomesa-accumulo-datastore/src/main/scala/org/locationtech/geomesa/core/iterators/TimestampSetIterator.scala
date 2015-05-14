/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.locationtech.geomesa.core.iterators

import java.util.{UUID, Map => JMap}

import org.apache.accumulo.core.client.mapreduce.InputFormatBase
import org.apache.accumulo.core.client.{IteratorSetting, ScannerBase}
import org.apache.accumulo.core.data.{Key, Value}
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SkippingIterator, SortedKeyValueIterator}
import org.apache.hadoop.mapreduce.Job
import org.locationtech.geomesa.core.iterators.TimestampSetIterator._

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
