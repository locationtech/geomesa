/*
 * Copyright 2015 Commonwealth Computer Research, Inc.
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

package org.locationtech.geomesa.accumulo.iterators

import java.util.{Collection => jCollection}

import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data.{Range => AccRange, _}
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}
import org.apache.hadoop.io.Text
import org.locationtech.geomesa.accumulo.iterators.RowSkippingIterator._

import scala.collection.SortedSet

/**
 * Iterator that will skip rows based on a set of suffix strings.
 */
class RowSkippingIterator extends GeomesaFilteringIterator {

  var suffixes: SortedSet[String] = null
  var suffixLength: Int = -1

  var currentRange: AccRange = null
  var currentRangeInclusive: Boolean = false
  var currentColumnFamilies: jCollection[ByteSequence] = null

  override def init(source: SortedKeyValueIterator[Key, Value],
                    options: java.util.Map[String, String],
                    env: IteratorEnvironment) = {
    super.init(source, options, env)
    val suffix = options.get(GEOMESA_ITERATORS_ROW_SUFFIX)
    suffixes = SortedSet(suffix.split(","): _*)
    suffixLength = suffixes.head.length
  }

  override def seek(range: AccRange, columnFamilies: jCollection[ByteSequence], inclusive: Boolean) = {
    currentRange = range
    currentRangeInclusive = inclusive
    currentColumnFamilies = columnFamilies
    super.seek(range, columnFamilies, inclusive)
  }

  override def setTopConditionally() = {
    val key = source.getTopKey
    val row = key.getRow.toString
    val suffix = row.substring(row.length - suffixLength)
    if (suffixes.contains(suffix)) {
      topKey = key
      topValue = source.getTopValue
    } else {
      // seek to the next valid row
      suffixes.find(_ > suffix).foreach { nextSuffix =>
        val nextRow = new Text(row.substring(0, row.length - suffixLength) + nextSuffix)
        val start = new Key(nextRow)
        val toClip = if (currentRange.afterEndKey(start)) {
          // seek to the end of the range to exhaust the iterator
          new AccRange(currentRange.getEndKey, true, null, false)
        } else {
          new AccRange(start, true, null, false)
        }
        val range = currentRange.clip(toClip)
        seek(range, currentColumnFamilies, currentRangeInclusive)
      }
    }
  }

  override def deepCopy(env: IteratorEnvironment): SortedKeyValueIterator[Key, Value]  = {
    import scala.collection.JavaConverters._
    val iter = new RowSkippingIterator
    val options = Map(GEOMESA_ITERATORS_ROW_SUFFIX -> suffixes.mkString(",")).asJava
    iter.init(source, options, env)
    iter
  }
}

object RowSkippingIterator {

  val GEOMESA_ITERATORS_ROW_SUFFIX = "geomesa.iterators.row-suffix"

  def configure(cfg: IteratorSetting, suffixes: Seq[String]): Unit = {
    cfg.addOption(GEOMESA_ITERATORS_ROW_SUFFIX, suffixes.mkString(","))
  }
}