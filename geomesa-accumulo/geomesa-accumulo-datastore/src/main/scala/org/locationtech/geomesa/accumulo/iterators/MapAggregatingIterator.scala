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

import java.util.{Map => JMap}

import com.typesafe.scalalogging.slf4j.Logging
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data.{Key, Value}
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.locationtech.geomesa.accumulo.iterators.FeatureAggregatingIterator.Result
import org.locationtech.geomesa.accumulo.sumNumericValueMutableMaps
import org.locationtech.geomesa.utils.geotools.{GeometryUtils, SimpleFeatureTypes}
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.languageFeature.implicitConversions

class MapAggregatingIterator(other: MapAggregatingIterator, env: IteratorEnvironment)
  extends FeatureAggregatingIterator[MapAggregatingIteratorResult](other, env) {

  var mapAttribute: String = null

  def this() = this(null, null)

  override def initProjectedSFTDefClassSpecificVariables(source: SortedKeyValueIterator[Key, Value],
                                                         options: JMap[String, String],
                                                         env: IteratorEnvironment): Unit = {

    mapAttribute = options.get(MapAggregatingIterator.MAP_ATTRIBUTE)
    projectedSFTDef = MapAggregatingIterator.projectedSFTDef(mapAttribute, simpleFeatureType)
  }

  override def handleKeyValue(resultO: Option[MapAggregatingIteratorResult],
                              topSourceKey: Key,
                              topSourceValue: Value): MapAggregatingIteratorResult = {

    val feature = originalDecoder.deserialize(topSourceValue.get)
    val currCounts = feature.getAttribute(mapAttribute).asInstanceOf[JMap[AnyRef, Int]].asScala

    val result = resultO.getOrElse(MapAggregatingIteratorResult(mapAttribute))
    sumNumericValueMutableMaps(Seq(currCounts), result.countMap)
    result
  }
}

object MapAggregatingIterator extends Logging {

  val MAP_ATTRIBUTE = "map_attribute"
  def projectedSFTDef(mapAttributeName: String, underlyingSFT: SimpleFeatureType) = {
    val mapAttributeSpec =
      SimpleFeatureTypes.AttributeSpecFactory.fromAttributeDescriptor(
        underlyingSFT,
        underlyingSFT.getDescriptor(mapAttributeName))

    s"${mapAttributeSpec.toSpec},geom:Point:srid=4326"
  }

  def configure(cfg: IteratorSetting, mapAttribute: String) =
    setMapAttribute(cfg, mapAttribute)

  def setMapAttribute(iterSettings: IteratorSetting, mapAttribute: String): Unit =
    iterSettings.addOption(MAP_ATTRIBUTE, mapAttribute)
}

case class MapAggregatingIteratorResult(mapAttributeName: String,
                                        countMap: mutable.Map[AnyRef, Int] = mutable.Map()) extends Result {
  override def addToFeature(sfb: SimpleFeatureBuilder): Unit =  {
    sfb.add(countMap.toMap)
    sfb.add(GeometryUtils.zeroPoint)
  }
}

