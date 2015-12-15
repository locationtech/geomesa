/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.iterators

import java.util.{HashMap => JHMap, Map => JMap}

import com.typesafe.scalalogging.slf4j.Logging
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data.{Key, Value}
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}
import org.apache.commons.codec.binary.Base64
import org.geotools.data.Query
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.locationtech.geomesa.accumulo.index.QueryHints._
import org.locationtech.geomesa.accumulo.index.QueryPlanner.SFIter
import org.locationtech.geomesa.accumulo.iterators.FeatureAggregatingIterator.Result
import org.locationtech.geomesa.features.ScalaSimpleFeatureFactory
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.buildTypeName
import org.locationtech.geomesa.utils.geotools.{GeometryUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.stats._
import org.opengis.feature.simple.SimpleFeatureType

/**
 * Reads simple features and observe them with a Stat server-side
 *
 * @param other
 * @param env
 */
class KryoLazyStatsIterator(other: KryoLazyStatsIterator, env: IteratorEnvironment)
  extends FeatureAggregatingIterator[StatsIteratorResult](other, env) {

  import org.locationtech.geomesa.accumulo.iterators.KryoLazyStatsIterator.STATS_ITERATOR_SFT_STRING

  var stat: Stat = null

  projectedSFTDef = STATS_ITERATOR_SFT_STRING

  def this() = this(null, null)

  override def initProjectedSFTDefClassSpecificVariables(source: SortedKeyValueIterator[Key, Value],
                                                         options: JMap[String, String],
                                                         env: IteratorEnvironment): Unit = {
    val statString = KryoLazyStatsIterator.getStatsString(options)
    stat = Stat(simpleFeatureType, statString)
  }

  override def handleKeyValue(resultO: Option[StatsIteratorResult],
                              topSourceKey: Key,
                              topSourceValue: Value): StatsIteratorResult = {
    val sf = originalDecoder.deserialize(topSourceValue.get())
    val result = resultO.getOrElse(StatsIteratorResult(stat))
    result.stat.observe(sf)
    result
  }
}

object KryoLazyStatsIterator extends Logging {
  val STATS_STRING_KEY = "geomesa.stats.string"
  val STATS_FEATURE_TYPE_KEY = "geomesa.stats.featuretype"
  val STATS = "stats"
  val STATS_ITERATOR_SFT_STRING = s"$STATS:String,geom:Geometry"

  /**
   * Adds key-value pairs consisting of the statString and featureType to the cfg client-side
   * The config is then read server-side to set up the iterator
   *
   * @param cfg iterator config settings
   * @param statString stat query hint string (e.g. "MinMax(foo)")
   */
  def configure(cfg: IteratorSetting, statString: String) = {
    setStatsString(cfg, statString)
  }

  def setStatsString(iterSettings: IteratorSetting, statString: String): Unit = {
    iterSettings.addOption(STATS_STRING_KEY, statString)
  }

  def getStatsString(options: JMap[String, String]): String = {
    options.get(STATS_STRING_KEY)
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

    val feature = summedStats.map { sum =>
      val featureBuilder = ScalaSimpleFeatureFactory.featureBuilder(sft)
      if (encode) {
        featureBuilder.add(encodeStat(sum))
      } else {
        featureBuilder.add(sum.toJson())
      }
      featureBuilder.add(GeometryUtils.zeroPoint) // Filler value as Feature requires a geometry
      featureBuilder.buildFeature(null)
    }

    feature.iterator
  }
}

case class StatsIteratorResult(stat: Stat) extends Result {
  override def addToFeature(featureBuilder: SimpleFeatureBuilder): Unit = {
    featureBuilder.add(KryoLazyStatsIterator.encodeStat(stat))
  }
}
