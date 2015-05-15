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


package org.locationtech.geomesa.accumulo.iterators

import com.typesafe.scalalogging.slf4j.Logging
import org.apache.accumulo.core.data._
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}
import org.apache.hadoop.io.Text
import org.locationtech.geomesa.accumulo.data.tables.SpatioTemporalTable
import org.locationtech.geomesa.accumulo.data.tables.SpatioTemporalTable.{DATA_CHECK, INDEX_CHECK}
import org.locationtech.geomesa.accumulo.index.IndexValueEncoder
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.accumulo.GEOMESA_ITERATORS_VERSION

class ConsistencyCheckingIterator extends SortedKeyValueIterator[Key, Value] with Logging {

  import scala.collection.JavaConversions._

  private var indexSource: SortedKeyValueIterator[Key, Value] = null
  private var dataSource: SortedKeyValueIterator[Key, Value] = null

  private var topKey: Key = null
  private val topValue: Value = new Value(Array[Byte]())
  private var nextKey: Key = null
  private var curId: String = null

  def init(source: SortedKeyValueIterator[Key, Value],
           options: java.util.Map[String, String],
           env: IteratorEnvironment) {
    logger.debug("Checking consistency")
    this.indexSource = source.deepCopy(env)
    this.dataSource = source.deepCopy(env)
  }

  def hasTop = nextKey != null || topKey != null

  def getTopKey = topKey

  def getTopValue = topValue

  def findTop() {
    logger.debug("Finding top")
    // clear out the reference to the next entry
    nextKey = null

    while (nextKey == null && indexSource.hasTop) {
      nextKey = indexSource.getTopKey
      if (SpatioTemporalTable.isDataEntry(nextKey)) {
        nextKey = null
      } else {
        logger.debug(s"Checking $nextKey")
        curId = nextKey.getColumnQualifier.toString

        val dataSeekKey = new Key(new Text(nextKey.getRow.toString.replace(INDEX_CHECK, DATA_CHECK)),
          nextKey.getColumnFamily, nextKey.getColumnQualifier)
        dataSource.seek(new Range(dataSeekKey, null), Seq.empty[ByteSequence], false)

        if (!dataSource.hasTop ||
            dataSource.getTopKey.getColumnQualifier.toString != nextKey.getColumnQualifier.toString) {
          logger.warn(s"Found an inconsistent entry: $nextKey")
        } else {
          nextKey = null
        }
      }
      indexSource.next()
    }
  }

  def next(): Unit = {
    if(nextKey != null) {
      topKey = nextKey
      findTop()
    } else {
      topKey = null
    }
  }

  def seek(range: Range, columnFamilies: java.util.Collection[ByteSequence], inclusive: Boolean): Unit = {
    logger.trace(s"Seeking $range")
    indexSource.seek(range, columnFamilies, inclusive)
    findTop()

    if(nextKey != null) next()
  }

  def deepCopy(env: IteratorEnvironment): SortedKeyValueIterator[Key, Value] = throw new UnsupportedOperationException()
}
