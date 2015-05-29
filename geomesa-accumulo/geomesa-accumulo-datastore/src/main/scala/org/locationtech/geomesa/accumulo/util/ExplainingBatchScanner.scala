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

package org.locationtech.geomesa.accumulo.util

import java.util
import java.util.Map.Entry
import java.util.concurrent.TimeUnit

import org.apache.accumulo.core.client.{BatchScanner, IteratorSetting, Scanner}
import org.apache.accumulo.core.data.{Key, Range, Value}
import org.apache.hadoop.io.Text
import org.locationtech.geomesa.accumulo.index.ExplainerOutputType

class ExplainingBatchScanner(output: ExplainerOutputType) extends ExplainingScanner(output) with BatchScanner {
  override def setRanges(ranges: util.Collection[Range]): Unit = {}
}

class ExplainingScanner(output: ExplainerOutputType) extends Scanner {

  override def setTimeout(timeout: Long, timeUnit: TimeUnit): Unit = output(s"setTimeout($timeout, $timeUnit)")

  override def close(): Unit = {}

  override def updateScanIteratorOption(iteratorName: String, key: String, value: String): Unit =
    output(s"updateScanIterator($iteratorName, $key, $value")

  override def removeScanIterator(iteratorName: String): Unit = output(s"removeScanIterator($iteratorName)")

  override def fetchColumnFamily(col: Text): Unit = {}

  override def getTimeout(timeUnit: TimeUnit): Long = { output(s"getTimeout($timeUnit)"); 0 }

  override def iterator(): util.Iterator[Entry[Key, Value]] = {
    new util.Iterator[Entry[Key, Value]] {
      override def next(): Entry[Key, Value] = null
      override def remove(): Unit = {}
      override def hasNext: Boolean = false
    }
  }

  override def clearScanIterators(): Unit = output(s"clearScanIterators")

  override def fetchColumn(colFam: Text, colQual: Text): Unit = {}

  override def clearColumns(): Unit = output("clearColumns")

  override def addScanIterator(cfg: IteratorSetting): Unit = output(s"addScanIterator($cfg")

  override def setTimeOut(timeOut: Int): Unit = {}

  override def getTimeOut: Int = ???

  override def setRange(range: Range): Unit = output(s"setRange: $range")

  override def getRange: Range = ???

  override def setBatchSize(size: Int): Unit = output(s"setBatchSize: $size")

  override def getBatchSize: Int = ???

  override def enableIsolation(): Unit = {}

  override def disableIsolation(): Unit = {}
}
