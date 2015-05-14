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

import java.io.IOException
import java.util.UUID

import org.apache.accumulo.core.client.{IteratorSetting, ScannerBase}
import org.apache.accumulo.core.data._
import org.apache.accumulo.core.iterators._
import org.apache.accumulo.start.classloader.AccumuloClassLoader
import org.apache.accumulo.start.classloader.vfs.AccumuloVFSClassLoader

import scala.collection.JavaConversions._

object AggregatingKeyIterator {
  val aggClass = "aggClass"
  val aggOpt = "aggOpt"

  val obj = new AggregatingKeyIterator
  def setupAggregatingKeyIterator(scanner: ScannerBase, aggregatorClass: Class[_ <: KeyAggregator]) {
    setupAggregatingKeyIterator(scanner,
                                Integer.MAX_VALUE,
                                aggregatorClass,
                                Map[String, String]())
  }

  def setupAggregatingKeyIterator(scanner: ScannerBase,
                                  priority: Int,
                                  aggregatorClass: Class[_ <: KeyAggregator],
                                  options: Map[String, String]) {
    val cfg = new IteratorSetting(priority,
                                  "aggregatingKeyIterator-" + UUID.randomUUID.toString.subSequence(0, 5),
                                  classOf[AggregatingKeyIterator])
    setAggClass(cfg, aggregatorClass)
    options.foreach({case (k, v) => cfg.addOption(k, v) })
    scanner.addScanIterator(cfg)
  }

  def setAggClass(cfg: IteratorSetting, aggrClass: Class[_ <: KeyAggregator]) {
    cfg.addOption(aggClass, aggrClass.getCanonicalName)
  }
}

class AggregatingKeyIterator extends SortedKeyValueIterator[Key, Value] with OptionDescriber {

  def deepCopy(env:IteratorEnvironment) = null
  private def aggregateRowColumn(aggr: KeyAggregator) {
    if (iterator.getTopKey.isDeleted) {
      return
    }
    workKey.set(iterator.getTopKey)
    aggr.reset()
    aggr.collect(workKey, iterator.getTopValue)
    iterator.next()
    while (iterator.hasTop && !iterator.getTopKey.isDeleted) {
      aggr.collect(iterator.getTopKey, iterator.getTopValue)
      iterator.next()
    }
    aggrKey = workKey
    aggrValue = aggr.aggregate
  }

  private def findTop() {
    if (iterator.hasTop) {
      aggregateRowColumn(aggregator)
    }
  }

  def getTopKey: Key = {
    if (aggrKey != null) {
      return aggrKey
    }
    iterator.getTopKey
  }

  def getTopValue: Value = {
    if (aggrKey != null) {
      return aggrValue
    }
    iterator.getTopValue
  }

  def hasTop: Boolean = {
    aggrKey != null || iterator.hasTop
  }

  def next() {
    if (aggrKey != null) {
      aggrKey = null
      aggrValue = null
    }
    else {
      iterator.next()
    }
    findTop()
  }

  def seek(range: Range, columnFamilies: java.util.Collection[ByteSequence], inclusive: Boolean) {
    val seekRange: Range = IteratorUtil.maximizeStartKeyTimeStamp(range)
    iterator.seek(seekRange, columnFamilies, inclusive)
    findTop()
    if (range.getStartKey != null) {
      while (hasTop && getTopKey.equals(range.getStartKey,
                                        PartialKey.ROW_COLFAM_COLQUAL_COLVIS_TIME) &&
             getTopKey.getTimestamp > range.getStartKey.getTimestamp) {
        next()
      }
      while (hasTop && range.beforeStartKey(getTopKey)) {
        next()
      }
    }
  }

  def init(source: SortedKeyValueIterator[Key, Value],
           options: java.util.Map[String, String],
           env: IteratorEnvironment) {
    this.iterator = source
    try {
      val clazz = options(AggregatingKeyIterator.aggClass)
      val aggClazz = AccumuloVFSClassLoader.loadClass(clazz)
      this.aggregator = aggClazz.newInstance.asInstanceOf[KeyAggregator]
    } catch {
      case e: Throwable => throw new IOException(e)
    }
    for (key <- options.keySet) {
      if (key.startsWith(AggregatingKeyIterator.aggOpt)) {
        this.aggregator.setOpt(key, options(key))
      }
    }
  }

   def describeOptions: OptionDescriber.IteratorOptions = {
    new OptionDescriber.IteratorOptions("agg",
                                        "Aggregators apply aggregating functions to all values",
                                        null,
                                        List[String]("aggClass <aggregatorClass>", "aggOpt.* <aggregator specific options>"))
  }

  def validateOptions(options: java.util.Map[String, String]): Boolean = {
    for (entry <- options.entrySet) {
      var clazz: Class[_ <: Combiner] = null
      try {
        clazz = AccumuloClassLoader.getClassLoader.loadClass(entry.getValue).asInstanceOf[Class[_ <: Combiner]]
        clazz.newInstance
      }
      catch {
        case e: Throwable => throw new IllegalArgumentException(e)
      }
    }
    true
  }

  private var iterator: SortedKeyValueIterator[Key, Value] = null
  private var aggregator: KeyAggregator = null
  private val workKey: Key = new Key
  private var aggrKey: Key = null
  private var aggrValue: Value = null

}
