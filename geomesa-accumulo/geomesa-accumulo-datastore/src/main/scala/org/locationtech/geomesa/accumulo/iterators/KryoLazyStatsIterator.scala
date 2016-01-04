/***********************************************************************
  * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
  * All rights reserved. This program and the accompanying materials
  * are made available under the terms of the Apache License, Version 2.0 which
  * accompanies this distribution and is available at
  * http://www.opensource.org/licenses/apache2.0.php.
  *************************************************************************/

package org.locationtech.geomesa.accumulo.iterators

import java.util.{Map => jMap, UUID}

import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data.{Key, Value}
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}
import org.apache.commons.codec.binary.Base64
import org.geotools.data.Query
import org.geotools.factory.Hints
import org.locationtech.geomesa.accumulo.index.QueryHints._
import org.locationtech.geomesa.accumulo.index.QueryPlanner.SFIter
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.buildTypeName
import org.locationtech.geomesa.utils.geotools.{GeometryUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.stats._
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

/**
 * Reads simple features and observe them with a Stat server-side
 *
 * Only works with z3IdxStrategy for now (queries that date filters)
 */
class KryoLazyStatsIterator extends KryoLazyAggregatingIterator[Stat] {

  import org.locationtech.geomesa.accumulo.iterators.KryoLazyStatsIterator._

  var stat: Stat = null
  var serializer: KryoFeatureSerializer = null
  var featureToSerialize: SimpleFeature = null
  var result: Stat = null

  override def init(src: SortedKeyValueIterator[Key, Value],
                    jOptions: jMap[String, String],
                    env: IteratorEnvironment): Unit = {
    val statString = jOptions.get(STATS_STRING_KEY)
    sft = SimpleFeatureTypes.createType("", jOptions.get(KryoLazyAggregatingIterator.SFT_OPT))
    if (result == null)
      result = Stat(sft, statString)
    super.init(src, jOptions, env)

    val statsSft = SimpleFeatureTypes.createType("", STATS_ITERATOR_SFT_STRING)
    serializer = new KryoFeatureSerializer(statsSft)
    featureToSerialize = new ScalaSimpleFeature("", statsSft, Array(null, GeometryUtils.zeroPoint))
  }

  override def newResult() = result

  override def aggregateResult(sf: SimpleFeature, result: Stat): Unit = result.observe(sf)

  override def encodeResult(result: Stat): Array[Byte] = {
    featureToSerialize.setAttribute(0, encodeStat(result))
    serializer.serialize(featureToSerialize)
  }
}

object KryoLazyStatsIterator extends LazyLogging {
  val DEFAULT_PRIORITY = 30
  val STATS_STRING_KEY = "geomesa.stats.string"
  val STATS_FEATURE_TYPE_KEY = "geomesa.stats.featuretype"
  val STATS = "stats"
  val STATS_ITERATOR_SFT_STRING = s"$STATS:String,geom:Geometry"

  def configure(sft: SimpleFeatureType,
                filter: Option[Filter],
                hints: Hints,
                deduplicate: Boolean,
                priority: Int = DEFAULT_PRIORITY): IteratorSetting = {
    val is = new IteratorSetting(priority, "stats-iter", classOf[KryoLazyStatsIterator])
    KryoLazyAggregatingIterator.configure(is, sft, filter, deduplicate, None)
    is.addOption(STATS_STRING_KEY, hints.get(STATS_STRING).asInstanceOf[String])
    is
  }

  def createFeatureType(origFeatureType: SimpleFeatureType) = {
    //Need a filler namespace, else geoserver throws nullptr exception for xml output
    val (namespace, name) = buildTypeName(origFeatureType.getTypeName)
    val outNamespace = if (namespace == null){
      "NullNamespace"
    } else {
      namespace
    }
    SimpleFeatureTypes.createType(outNamespace, name, KryoLazyStatsIterator.STATS_ITERATOR_SFT_STRING)
  }

  def combineStats(stat1: Stat, stat2: Stat): Stat = {
    stat1.add(stat2)
  }

  def encodeStat(stat: Stat): String = {
    Base64.encodeBase64URLSafeString(StatSerialization.pack(stat))
  }

  def decodeStat(encoded: String): Stat = {
    val bytes = Base64.decodeBase64(encoded)
    StatSerialization.unpack(bytes)
  }

  /**
   * Reduces computed simple features which contain stat information into one on the client
   *
   * @param features iterator of features received per tablet server from query
   * @param query query that the stats are being run against
   * @return aggregated iterator of features
   */
  def reduceFeatures(features: SFIter, query: Query): SFIter = {
    val encode = query.getHints.containsKey(RETURN_ENCODED)
    val sft = query.getHints.getReturnSft

    val decodedStats = features.map(f => decodeStat(f.getAttribute(STATS).toString))
    val summedStats = decodedStats.reduceOption(combineStats)

    summedStats.iterator.map { sum =>
      val time = if (encode) encodeStat(sum) else sum.toJson()
      new ScalaSimpleFeature(UUID.randomUUID().toString, sft, Array(time, GeometryUtils.zeroPoint))
    }
  }
}
