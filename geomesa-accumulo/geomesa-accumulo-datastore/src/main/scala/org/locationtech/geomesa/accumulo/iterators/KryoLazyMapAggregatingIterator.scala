/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.iterators

import java.util.{UUID, Map => jMap}

import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.client.IteratorSetting
import org.geotools.data.Query
import org.geotools.factory.Hints
import org.locationtech.geomesa.accumulo.index.AccumuloFeatureIndex.AccumuloFeatureIndex
import org.locationtech.geomesa.accumulo.index.QueryHints._
import org.locationtech.geomesa.accumulo.index.QueryPlanner.SFIter
import org.locationtech.geomesa.accumulo.sumNumericValueMutableMaps
import org.locationtech.geomesa.accumulo.util.CloseableIterator
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.locationtech.geomesa.utils.geotools.{AttributeSpec, GeometryUtils, SimpleFeatureTypes}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.languageFeature.implicitConversions

class KryoLazyMapAggregatingIterator extends KryoLazyAggregatingIterator[mutable.Map[AnyRef, Int]] {

  import KryoLazyMapAggregatingIterator._

  var mapAttribute: Int = -1
  var serializer: KryoFeatureSerializer = null
  var featureToSerialize: SimpleFeature = null

  override def init(options: Map[String, String]): mutable.Map[AnyRef, Int] = {
    val attributeName = options(MAP_ATTRIBUTE)
    mapAttribute = sft.indexOf(attributeName)
    val mapSft = SimpleFeatureTypes.createType("", createMapSft(sft, attributeName))
    val kryoOptions = if (index.serializedWithId) SerializationOptions.none else SerializationOptions.withoutId
    serializer = new KryoFeatureSerializer(mapSft, kryoOptions)
    featureToSerialize = new ScalaSimpleFeature("", mapSft, Array(null, GeometryUtils.zeroPoint))
    mutable.Map.empty[AnyRef, Int]
  }

  override def aggregateResult(sf: SimpleFeature, result: mutable.Map[AnyRef, Int]): Unit = {
    val currCounts = sf.getAttribute(mapAttribute).asInstanceOf[jMap[AnyRef, Int]].asScala
    sumNumericValueMutableMaps(Seq(currCounts), result)
  }

  override def encodeResult(result: mutable.Map[AnyRef, Int]): Array[Byte] = {
    featureToSerialize.setAttribute(0, result.asJava)
    serializer.serialize(featureToSerialize)
  }
}

object KryoLazyMapAggregatingIterator extends LazyLogging {

  val DEFAULT_PRIORITY = 30
  private val MAP_ATTRIBUTE = "map"

  /**
   * Creates an iterator config for the z3 density iterator
   */
  def configure(sft: SimpleFeatureType,
                index: AccumuloFeatureIndex,
                filter: Option[Filter],
                hints: Hints,
                deduplicate: Boolean,
                priority: Int = DEFAULT_PRIORITY): IteratorSetting = {
    val mapAttribute = hints.getMapAggregatingAttribute
    val is = new IteratorSetting(priority, "map-aggregate-iter", classOf[KryoLazyMapAggregatingIterator])
    KryoLazyAggregatingIterator.configure(is, sft, index, filter, deduplicate, None)
    is.addOption(MAP_ATTRIBUTE, mapAttribute)
    is
  }

  def createMapSft(sft: SimpleFeatureType, mapAttribute: String) =
    s"${AttributeSpec(sft, sft.getDescriptor(mapAttribute)).toSpec},*geom:Point:srid=4326"

  def reduceMapAggregationFeatures(features: SFIter, query: Query): SFIter = {
    val sft = query.getHints.getReturnSft
    val mapIndex = sft.indexOf(query.getHints.getMapAggregatingAttribute)

    val maps = features.map(_.getAttribute(mapIndex).asInstanceOf[jMap[AnyRef, Int]].asScala)

    if (maps.nonEmpty) {
      val reducedMap = sumNumericValueMutableMaps(maps.toIterable).toMap // to immutable map
      val attributes = Array(reducedMap.asJava, GeometryUtils.zeroPoint) // filler value as feature requires a geometry
      val result = new ScalaSimpleFeature(UUID.randomUUID().toString, sft, attributes)
      Iterator(result)
    } else {
      CloseableIterator.empty
    }
  }
}
