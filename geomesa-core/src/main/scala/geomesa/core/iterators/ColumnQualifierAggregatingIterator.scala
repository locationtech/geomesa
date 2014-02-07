/*
 * Copyright 2013 Commonwealth Computer Research, Inc.
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

package geomesa.core.iterators

import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data._
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}
import org.apache.accumulo.start.classloader.AccumuloClassLoader

class ColumnQualifierAggregatingIterator(other: ColumnQualifierAggregatingIterator, env: IteratorEnvironment)
  extends SortedKeyValueIterator[Key, Value]{

  import geomesa.core._

  var source: SortedKeyValueIterator[Key,Value] = null
  var aggrKey: Key = null
  var aggrValue: Value = null
  var aggregator: ColumnQualifierAggregator = null
  var nextKey: Key = null
  var nextValue: Value = null

  if (other != null && env != null) {
    source = other.source.deepCopy(env)
    aggregator = other.aggregator
  }

  def this() = this(null,null)

  def init(source: SortedKeyValueIterator[Key, Value],
           options: java.util.Map[String, String],
           env: IteratorEnvironment) {
    this.source = source
    val aggClass = options.get(DEFAULT_AGGREGATOR_CLASS_PROPERTY_NAME)
    val clazz = AccumuloClassLoader.getClassLoader.loadClass(aggClass)
    aggregator = clazz.newInstance().asInstanceOf[ColumnQualifierAggregator]
  }

  def hasTop = aggrKey != null || source.hasTop

  def findTop() {
    // in all cases, resetting the top means clearing out the aggregator
    aggregator.reset()

    nextKey = null
    nextValue = null

    if (source.hasTop) {
      val dstk = source.getTopKey
      nextKey = new Key(dstk.getRow, dstk.getColumnFamily)

      def getCurValue: Option[(Key,Value)] = {
        val tk = source.getTopKey
        val tv = new Value(source.getTopValue)
        if (!tk.equals(nextKey, PartialKey.ROW_COLFAM)) None
        else Some((tk, tv))
      }

      def genNext : Option[(Key,Value)] = {
        source.next()
        if (!source.hasTop) None
        else getCurValue
      }

      val values : List[(Key,Value)] = getCurValue.get :: Iterator.continually(genNext).takeWhile(_.isDefined).toList.map(_.get)
      values.foreach { case (k,v) => aggregator.collect(k,v) }
      nextValue = aggregator.aggregate()
    }
  }

  def next() {
    if (nextValue == null) {
      aggrKey = null
      aggrValue = null
    } else {
      aggrKey = nextKey
      aggrValue = new Value(nextValue)

      findTop()
    }
  }

  def seek(range: Range, columnFamilies: java.util.Collection[ByteSequence], inclusive: Boolean) {
    source.seek(range, columnFamilies, inclusive)
    findTop()
    next()
  }

  def getTopKey = aggrKey

  def getTopValue = aggrValue

  def deepCopy(env: IteratorEnvironment) = new ColumnQualifierAggregatingIterator(this, env)
}

object ColumnQualifierAggregatingIterator {

  import geomesa.core._

  def setAggregatorClass(cfg: IteratorSetting, aggrClass: Class[_ <: ColumnQualifierAggregator]) {
    cfg.addOption(DEFAULT_AGGREGATOR_CLASS_PROPERTY_NAME, aggrClass.getCanonicalName)
  }
}