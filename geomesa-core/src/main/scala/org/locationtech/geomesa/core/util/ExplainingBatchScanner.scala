/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.core.util

import java.util
import java.util.Map.Entry
import java.util.concurrent.TimeUnit

import org.apache.accumulo.core.client.{BatchScanner, IteratorSetting, Scanner}
import org.apache.accumulo.core.data.{Key, Range, Value}
import org.apache.hadoop.io.Text
import org.locationtech.geomesa.core.index.ExplainerOutputType

class ExplainingBatchScanner(output: ExplainerOutputType) extends ExplainingScanner(output) with BatchScanner {
  import scala.collection.JavaConversions._

  override def setRanges(r: util.Collection[Range]): Unit = ranges.appendAll(r)
}

class ExplainingScanner(output: ExplainerOutputType) extends Scanner with ExplainingConfig {

  val ranges = scala.collection.mutable.ArrayBuffer.empty[Range]
  val iterators = scala.collection.mutable.ArrayBuffer.empty[IteratorSetting]
  val cfs = scala.collection.mutable.ArrayBuffer.empty[Text]

  override def setTimeout(timeout: Long, timeUnit: TimeUnit): Unit = output(s"setTimeout($timeout, $timeUnit)")

  override def close(): Unit = {}

  override def updateScanIteratorOption(iteratorName: String, key: String, value: String): Unit = ???

  override def removeScanIterator(iteratorName: String): Unit = ???

  override def fetchColumnFamily(col: Text): Unit = cfs.append(col)

  override def getTimeout(timeUnit: TimeUnit): Long = { output(s"getTimeout($timeUnit)"); 0 }

  override def iterator(): util.Iterator[Entry[Key, Value]] = {
    new util.Iterator[Entry[Key, Value]] {
      override def next(): Entry[Key, Value] = null
      override def remove(): Unit = {}
      override def hasNext: Boolean = false
    }
  }

  override def clearScanIterators(): Unit = {
    iterators.clear()
    output(s"clearScanIterators")
  }

  override def fetchColumn(colFam: Text, colQual: Text): Unit = {}

  override def clearColumns(): Unit = {
    cfs.clear()
    output("clearColumns")
  }

  override def addScanIterator(cfg: IteratorSetting): Unit = {
    iterators.append(cfg)
    output(s"addScanIterator($cfg")
  }

  override def setTimeOut(timeOut: Int): Unit = {}

  override def getTimeOut: Int = ???

  override def setRange(range: Range): Unit = {
    ranges.clear()
    ranges.append(range)
    output(s"setRange: $range")
  }

  override def getRange: Range = ???

  override def setBatchSize(size: Int): Unit = output(s"setBatchSize: $size")

  override def getBatchSize: Int = ???

  override def enableIsolation(): Unit = {}

  override def disableIsolation(): Unit = {}

  // methods from ExplainingConfig
  override def getRanges() = ranges.toSeq

  override def getIterators() = iterators.toSeq

  override def getColumnFamilies() = cfs.toSeq
}

trait ExplainingConfig {
  def getRanges(): Seq[Range]
  def getIterators(): Seq[IteratorSetting]
  def getColumnFamilies(): Seq[Text]
}