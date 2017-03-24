/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.iterators

import java.util.Map.Entry

import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data.{Key, Value}
import org.apache.commons.codec.binary.Base64
import org.geotools.factory.Hints
import org.locationtech.geomesa.accumulo.AccumuloFeatureIndexType
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.collection.CloseableIterator
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

  var serializer: StatSerializer = null

  override def init(options: Map[String, String]): Stat = {
    sft = IteratorCache.sft(options(KryoLazyAggregatingIterator.SFT_OPT))
    serializer = StatSerializer(sft)
    Stat(sft, options(STATS_STRING_KEY))
  }

  override def aggregateResult(sf: SimpleFeature, result: Stat): Unit = result.observe(sf)

  override def encodeResult(result: Stat): Array[Byte] = serializer.serialize(result)
}

object KryoLazyStatsIterator extends LazyLogging {

  import org.locationtech.geomesa.index.conf.QueryHints.{ENCODE_STATS, STATS_STRING}

  val DEFAULT_PRIORITY = 30
  val STATS_STRING_KEY = "geomesa.stats.string"
  val STATS_FEATURE_TYPE_KEY = "geomesa.stats.featuretype"
  // Need a filler namespace, else geoserver throws NPE for xml output
  val StatsSft = SimpleFeatureTypes.createType("stats:stats", "stats:String,geom:Geometry")

  def configure(sft: SimpleFeatureType,
                index: AccumuloFeatureIndexType,
                filter: Option[Filter],
                hints: Hints,
                deduplicate: Boolean,
                priority: Int = DEFAULT_PRIORITY): IteratorSetting = {
    val is = new IteratorSetting(priority, "stats-iter", classOf[KryoLazyStatsIterator])
    KryoLazyAggregatingIterator.configure(is, sft, index, filter, deduplicate, None)
    is.addOption(STATS_STRING_KEY, hints.get(STATS_STRING).asInstanceOf[String])
    is
  }

  def kvsToFeatures(sft: SimpleFeatureType): (Entry[Key, Value]) => SimpleFeature = {
    val sf = new ScalaSimpleFeature("", StatsSft)
    sf.setAttribute(1, GeometryUtils.zeroPoint)
    (e: Entry[Key, Value]) => {
      // value is the already serialized stat
      sf.setAttribute(0, Base64.encodeBase64URLSafeString(e.getValue.get()))
      sf
    }
  }

  /**
    * Encodes a stat as a base64 string.
    *
    * Creates a new serializer each time, so don't call repeatedly.
    *
    * @param stat stat to encode
    * @param sft simple feature type of underlying schema
    * @return base64 string
    */
  def encodeStat(stat: Stat, sft: SimpleFeatureType): String =
    Base64.encodeBase64URLSafeString(StatSerializer(sft).serialize(stat))

  /**
    * Decodes a stat string from a result simple feature.
    *
    * Creates a new serializer each time, so not used internally.
    *
    * @param encoded encoded string
    * @param sft simple feature type of the underlying schema
    * @return stat
    */
  def decodeStat(encoded: String, sft: SimpleFeatureType): Stat =
    StatSerializer(sft).deserialize(Base64.decodeBase64(encoded))

  /**
   * Reduces computed simple features which contain stat information into one on the client
   *
   * @param features iterator of features received per tablet server from query
   * @param hints query hints that the stats are being run against
   * @return aggregated iterator of features
   */
  def reduceFeatures(sft: SimpleFeatureType, hints: Hints)
                    (features: CloseableIterator[SimpleFeature]): CloseableIterator[SimpleFeature] = {
    val serializer = StatSerializer(sft)

    val decodedStats = features.map { f =>
      serializer.deserialize(Base64.decodeBase64(f.getAttribute(0).toString))
    }

    val sum = if (decodedStats.isEmpty) {
      // create empty stat based on the original input so that we always return something
      Stat(sft, hints.get(STATS_STRING).asInstanceOf[String])
    } else {
      val sum = decodedStats.next()
      decodedStats.foreach(sum += _)
      sum
    }
    decodedStats.close()

    val stats = if (hints.containsKey(ENCODE_STATS) && hints.get(ENCODE_STATS).asInstanceOf[Boolean]) encodeStat(sum, sft) else sum.toJson
    Iterator(new ScalaSimpleFeature("stat", StatsSft, Array(stats, GeometryUtils.zeroPoint)))
  }
}