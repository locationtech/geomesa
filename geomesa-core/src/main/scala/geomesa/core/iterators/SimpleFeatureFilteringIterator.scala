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

import geomesa.core.data.SimpleFeatureEncoder
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data._
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}
import org.geotools.data.DataUtilities
import org.geotools.filter.text.ecql.ECQL
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter
import org.apache.log4j.Logger

class SimpleFeatureFilteringIterator(other: SimpleFeatureFilteringIterator, env: IteratorEnvironment)
  extends SortedKeyValueIterator[Key, Value]{

  private val log = Logger.getLogger(classOf[SimpleFeatureFilteringIterator])

  import geomesa.core._
  SpatioTemporalIntersectingIterator.initClassLoader(log)

  var source: SortedKeyValueIterator[Key,Value] = null
  var topKey: Key = null
  var topValue: Value = null
  var nextKey: Key = null
  var nextValue: Value = null

  var simpleFeatureType : SimpleFeatureType = null

  // the default filter accepts everything
  var filter : Filter = null
  // defer converting the value into a feature until we know we need it
  lazy val wrappedFilter =
    if (filter != null)
      (value:Value) =>
        value != null && filter.evaluate(SimpleFeatureEncoder.decode(simpleFeatureType, value))
    else
      (value:Value) => true

  if (other != null && env != null) {
    source = other.source.deepCopy(env)
    filter = other.filter
    simpleFeatureType = other.simpleFeatureType
  }

  def this() = this(null,null)

  def init(source: SortedKeyValueIterator[Key, Value],
           options: java.util.Map[String, String],
           env: IteratorEnvironment) {
    this.source = source

    val simpleFeatureTypeSpec = options.get(GEOMESA_ITERATORS_SIMPLE_FEATURE_TYPE)
    simpleFeatureType = DataUtilities.createType(this.getClass.getCanonicalName, simpleFeatureTypeSpec)

    // read off the filter expression, if applicable
    val filterExpression = options.get(GEOMESA_ITERATORS_ECQL_FILTER)
    if (filterExpression!= null && filterExpression.length > 0) {
      // parse the filter expression here
      filter = ECQL.toFilter(filterExpression)
    }

    topKey = null
    topValue = null
    nextKey = null
    nextValue = null
  }

  def hasTop = topKey != null || source.hasTop

  def getTopKey = topKey

  def getTopValue = new Value(topValue)

  def findTop() {
    nextKey = null
    nextValue = null

    while (source.hasTop && nextValue == null) {
      // apply the filter to see whether this value (qua SimpleFeature) should be accepted
      if (wrappedFilter(source.getTopValue)) {
        // if accepted, copy the value, because reusing them is UNSAFE
        nextKey = new Key(source.getTopKey)
        nextValue = new Value(source.getTopValue)
      }

      // you MUST advance to the next key
      source.next()
    }
  }

  def next() {
    if (nextValue == null) {
      topKey = null
      topValue = null
    } else {
      topKey = nextKey
      topValue = nextValue
      findTop()
    }
  }

  def seek(range: Range, columnFamilies: java.util.Collection[ByteSequence], inclusive: Boolean) {
    topKey = null
    topValue = null
    nextKey = null
    nextValue = null

    source.seek(range, columnFamilies, inclusive)
    findTop()
    if (nextKey != null) next()
  }

  def deepCopy(env: IteratorEnvironment) = new SimpleFeatureFilteringIterator(this, env)
}

object SimpleFeatureFilteringIterator {
  import geomesa.core._

  def setFeatureType(cfg: IteratorSetting, featureType: String) {
    cfg.addOption(GEOMESA_ITERATORS_SIMPLE_FEATURE_TYPE, featureType)
  }

  def setECQLFilter(cfg: IteratorSetting, ecql: String) {
    cfg.addOption(GEOMESA_ITERATORS_ECQL_FILTER, ecql)
  }
}
