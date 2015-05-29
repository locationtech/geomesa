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

import java.util.{Collection => JCollection, Map => JMap}

import com.typesafe.scalalogging.slf4j.Logging
import org.apache.accumulo.core.data.{ByteSequence, Key, Range => ARange, Value}
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.geometry.jts.JTSFactoryFinder
import org.locationtech.geomesa.accumulo._
import org.locationtech.geomesa.accumulo.data.DEFAULT_ENCODING
import org.locationtech.geomesa.accumulo.iterators.FeatureAggregatingIterator.Result
import org.locationtech.geomesa.features.avro.AvroSimpleFeatureFactory
import org.locationtech.geomesa.features._
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeatureType

import scala.util.Random

abstract class FeatureAggregatingIterator[T <: Result](val other: FeatureAggregatingIterator[T],
                                                       val env: IteratorEnvironment)
  extends SortedKeyValueIterator[Key, Value] {

  var curRange: ARange = null
  var projectedSFT: SimpleFeatureType = null
  var featureBuilder: SimpleFeatureBuilder = null
  var topKey: Option[Key] = None
  var topValue: Option[Value] = None

  var simpleFeatureType: SimpleFeatureType = null
  var source: SortedKeyValueIterator[Key,Value] = null

  var originalDecoder: SimpleFeatureDeserializer = null
  var featureEncoder: SimpleFeatureSerializer = null

  var projectedSFTDef: String = null

  if (other != null && env != null) {
    source = other.source.deepCopy(env)
    simpleFeatureType = other.simpleFeatureType
  }

  def init(source: SortedKeyValueIterator[Key, Value],
           options: JMap[String, String],
           env: IteratorEnvironment): Unit = {
    this.source = source

    val simpleFeatureTypeSpec = options.get(GEOMESA_ITERATORS_SIMPLE_FEATURE_TYPE)
    simpleFeatureType = SimpleFeatureTypes.createType(this.getClass.getCanonicalName, simpleFeatureTypeSpec)
    simpleFeatureType.decodeUserData(options, GEOMESA_ITERATORS_SIMPLE_FEATURE_TYPE)

    val encodingOpt = Option(options.get(FEATURE_ENCODING)).map(SerializationType.withName).getOrElse(DEFAULT_ENCODING)
    originalDecoder = SimpleFeatureDeserializers(simpleFeatureType, encodingOpt)

    initProjectedSFTDefClassSpecificVariables(source, options, env)

    projectedSFT = SimpleFeatureTypes.createType(simpleFeatureType.getTypeName, projectedSFTDef)

    // Use density SFT for the encoder since we are transforming the feature into
    // a sparse matrix as the result type of this iterator
    featureEncoder = SimpleFeatureSerializers(projectedSFT, encodingOpt)
    featureBuilder = AvroSimpleFeatureFactory.featureBuilder(projectedSFT)
  }

  /**
   * Subclasses must override this class.  It should set the variable `projectedSFTDef` (if not set in
   * class initialization) which provides the SFT specification for the projected result feature of this
   * aggregating iterator.  Any other subclass-specific initialization can be done in this function as well.
   * @param source
   * @param options
   * @param env
   */
  def initProjectedSFTDefClassSpecificVariables(source: SortedKeyValueIterator[Key, Value],
                                                options: JMap[String, String],
                                                env: IteratorEnvironment): Unit

  /**
   * Combines the results from the underlying iterator stack
   * into a single feature
   */
  def findTop() = {
    var topSourceKeyO: Option[Key] = None
    var result: Option[T] = None

    while(source.hasTop && !curRange.afterEndKey(source.getTopKey)) {
      val topSourceKey = source.getTopKey
      topSourceKeyO = Some(topSourceKey)

      result = Some(handleKeyValue(result, topSourceKey, source.getTopValue))

      // Advance the source iterator
      source.next()
    }

    // if we found anything, set the current value
    topSourceKeyO.foreach { topSourceKey =>
      featureBuilder.reset()
      result.getOrElse(throw new Exception("missing result")).addToFeature(featureBuilder)

      val feature = featureBuilder.buildFeature(Random.nextString(6))
      topKey = Some(topSourceKey)
      topValue = Some(new Value(featureEncoder.serialize(feature)))
    }
  }

  def handleKeyValue(currResult: Option[T], topSourceKey: Key, topSourceValue: Value): T

  override def seek(range: ARange,
                    columnFamilies: JCollection[ByteSequence],
                    inclusive: Boolean): Unit = {
    curRange = range
    source.seek(range, columnFamilies, inclusive)
    findTop()
  }

  def hasTop: Boolean = topKey.nonEmpty

  def getTopKey: Key = topKey.orNull

  def getTopValue = topValue.orNull

  def deepCopy(env: IteratorEnvironment): SortedKeyValueIterator[Key, Value] =
    throw new UnsupportedOperationException("not implemented")

  def next(): Unit =
    if(!source.hasTop) {
      topKey = None
      topValue = None
    } else {
      findTop()
    }
}

object FeatureAggregatingIterator extends Logging {
  val geomFactory = JTSFactoryFinder.getGeometryFactory

  trait Result {
    def addToFeature(sfb: SimpleFeatureBuilder): Unit
  }
}
