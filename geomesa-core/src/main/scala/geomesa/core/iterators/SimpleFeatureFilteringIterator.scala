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

import com.typesafe.scalalogging.slf4j.Logging
import geomesa.core.data._
import geomesa.core.transform.TransformCreator
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data._
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}
import org.geotools.data.DataUtilities
import org.geotools.filter.text.ecql.ECQL
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter
import scala.util.{Failure, Success, Try}

class SimpleFeatureFilteringIterator(other: SimpleFeatureFilteringIterator, env: IteratorEnvironment)
  extends SortedKeyValueIterator[Key, Value]
  with Logging {

  import geomesa.core._

  SpatioTemporalIntersectingIterator.initClassLoader(logger)

  var source: SortedKeyValueIterator[Key,Value] = null
  var topKey: Key = null
  var topValue: Value = null
  var nextKey: Key = null
  var nextValue: Value = null
  var nextFeature: SimpleFeature = null

  var simpleFeatureType: SimpleFeatureType = null
  var targetFeatureType: SimpleFeatureType = null

  var featureEncoder: SimpleFeatureEncoder = null

  // the default filter accepts everything
  var filter: Filter = null

  var transform: (SimpleFeature => Value) = (_: SimpleFeature) => source.getTopValue

  def evalFilter(v: Value) = {
    Try(featureEncoder.decode(simpleFeatureType, v)) match {
      case Success(feature) =>
        nextFeature = feature
        filter.evaluate(nextFeature)
      case Failure(e) =>
        logger.error(s"Error decoding value to simple feature for key '${source.getTopKey}': ", e)
        nextFeature = null
        false
    }
  }

  if (other != null && env != null) {
    source = other.source.deepCopy(env)
    filter = other.filter
    simpleFeatureType = other.simpleFeatureType
  }

  def this() = this(null, null)

  def init(source: SortedKeyValueIterator[Key, Value],
           options: java.util.Map[String, String],
           env: IteratorEnvironment) {
    this.source = source

    // default to text if not found for backwards compatibility
    val encodingOpt = Option(options.get(FEATURE_ENCODING)).getOrElse(FeatureEncoding.TEXT.toString)
    featureEncoder = SimpleFeatureEncoderFactory.createEncoder(encodingOpt)

    val simpleFeatureTypeSpec = options.get(GEOMESA_ITERATORS_SIMPLE_FEATURE_TYPE)
    simpleFeatureType = DataUtilities.createType(this.getClass.getCanonicalName, simpleFeatureTypeSpec)
    simpleFeatureType.decodeUserData(options, GEOMESA_ITERATORS_SIMPLE_FEATURE_TYPE)

    val transformSchema = options.get(GEOMESA_ITERATORS_TRANSFORM_SCHEMA)
    targetFeatureType =
      if (transformSchema != null) DataUtilities.createType(this.getClass.getCanonicalName, transformSchema)
      else simpleFeatureType
    // if the targetFeatureType comes from a transform, also insert the UserData
    if (transformSchema != null) targetFeatureType.decodeUserData(options, GEOMESA_ITERATORS_TRANSFORM_SCHEMA)

    val transformString = options.get(GEOMESA_ITERATORS_TRANSFORM)
    transform =
      if(transformString != null) TransformCreator.createTransform(targetFeatureType, featureEncoder, transformString)
      else _ => source.getTopValue

    // read off the filter expression, if applicable
    filter =
      Try {
        val expr = options.get(GEOMESA_ITERATORS_ECQL_FILTER)
        ECQL.toFilter(expr)
      }.getOrElse(Filter.INCLUDE)

    topKey = null
    topValue = null
    nextKey = null
    nextValue = null
  }

  def hasTop = topKey != null || source.hasTop

  def getTopKey = topKey

  def getTopValue = topValue

  def findTop() {
    nextKey = null
    nextValue = null

    while (source.hasTop && nextValue == null) {
      // apply the filter to see whether this value (qua SimpleFeature) should be accepted
      if (evalFilter(source.getTopValue)) {
        // if accepted, copy the value, because reusing them is UNSAFE
        nextKey = new Key(source.getTopKey)
        nextValue = new Value(transform(nextFeature))
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

  def setECQLFilter(cfg: IteratorSetting, ecql: String) {
    cfg.addOption(GEOMESA_ITERATORS_ECQL_FILTER, ecql)
  }
}
