/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.iterators

import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.codec.binary.Base64
import org.geotools.factory.Hints
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.index.api.GeoMesaFeatureIndex
import org.locationtech.geomesa.index.iterators.StatsScan.StatResult
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.locationtech.geomesa.utils.geotools.GeometryUtils
import org.locationtech.geomesa.utils.interop.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.CloseWithLogging
import org.locationtech.geomesa.utils.stats.{Stat, StatSerializer}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

import scala.util.control.NonFatal

trait StatsScan extends AggregatingScan[StatResult] with LazyLogging {

  import org.locationtech.geomesa.index.iterators.StatsScan.Configuration._

  override protected def initResult(
      sft: SimpleFeatureType,
      transform: Option[SimpleFeatureType],
      options: Map[String, String]): StatResult = {
    new StatResult(transform.getOrElse(sft), options(STATS_STRING_KEY))
  }

  override protected def aggregateResult(sf: SimpleFeature, result: StatResult): Unit = {
    try { result.observe(sf) } catch {
      case NonFatal(e) => logger.warn(s"Error observing feature $sf", e)
    }
  }

  override protected def notFull(result: StatResult): Boolean = result.notFull()

  // encode the result as a byte array
  override protected def encodeResult(result: StatResult): Array[Byte] = result.serialize()
}

object StatsScan {

  val BatchSize = SystemProperty("geomesa.stats.batch.size", "100000")

  val StatsSft: SimpleFeatureType = SimpleFeatureTypes.createType("stats:stats", "stats:String,geom:Geometry")

  object Configuration {
    val STATS_STRING_KEY       = "geomesa.stats.string"
    val STATS_FEATURE_TYPE_KEY = "geomesa.stats.featuretype"
  }

  def configure(sft: SimpleFeatureType,
                index: GeoMesaFeatureIndex[_, _],
                filter: Option[Filter],
                hints: Hints): Map[String, String] = {
    import Configuration.STATS_STRING_KEY
    import org.locationtech.geomesa.index.conf.QueryHints.{RichHints, STATS_STRING}
    AggregatingScan.configure(sft, index, filter, hints.getTransform, hints.getSampling) ++
      Map(STATS_STRING_KEY -> hints.get(STATS_STRING).asInstanceOf[String])
  }

  /**
    * Encodes a stat as a base64 string.
    *
    * @param sft simple feature type of underlying schema
    * @return function to encode a stat as a base64 string
    */
  def encodeStat(sft: SimpleFeatureType): Stat => String = {
    val serializer = StatSerializer(sft)
    stat => Base64.encodeBase64URLSafeString(serializer.serialize(stat))
  }

  /**
    * Decodes a stat string from a result simple feature.
    *
    * @param sft simple feature type of the underlying schema
    * @return function to convert an encoded encoded string to a stat
    */
  def decodeStat(sft: SimpleFeatureType): String => Stat = {
    val serializer = StatSerializer(sft)
    encoded => serializer.deserialize(Base64.decodeBase64(encoded))
  }


  /**
    * Reduces computed simple features which contain stat information into one on the client
    *
    * @param features iterator of features received per tablet server from query
    * @param hints query hints that the stats are being run against
    * @return aggregated iterator of features
    */
  def reduceFeatures(sft: SimpleFeatureType,
                     hints: Hints)
                    (features: CloseableIterator[SimpleFeature]): CloseableIterator[SimpleFeature] = {
    import org.locationtech.geomesa.index.conf.QueryHints.RichHints

    val statSft = hints.getTransformSchema.getOrElse(sft)
    val sum = try {
      if (features.isEmpty) {
        // create empty stat based on the original input so that we always return something
        Stat(statSft, hints.getStatsQuery)
      } else {
        val decode = decodeStat(statSft)
        val sum = decode(features.next.getAttribute(0).asInstanceOf[String])
        while (features.hasNext) {
          sum += decode(features.next.getAttribute(0).asInstanceOf[String])
        }
        sum
      }
    } finally {
      CloseWithLogging(features)
    }

    val stats = if (hints.isStatsEncode) { encodeStat(statSft)(sum) } else { sum.toJson }
    CloseableIterator(Iterator(new ScalaSimpleFeature(StatsSft, "stat", Array(stats, GeometryUtils.zeroPoint))))
  }

  /**
   * Result wrapper for stats
   *
   * @param sft simple feature type
   * @param definition stat string
   */
  class StatResult(sft: SimpleFeatureType, definition: String) {

    private val stat = Stat(sft, definition)
    private val serializer = StatSerializer(sft)
    private val batch = StatsScan.BatchSize.toInt.get // has a valid default so should be safe to .get
    private var _count: Int = 0

    def observe(sf: SimpleFeature): Unit = {
      _count += 1
      stat.observe(sf)
    }

    def notFull(): Boolean = _count < batch

    def serialize(): Array[Byte] = serializer.serialize(stat)

    def isEmpty: Boolean = stat.isEmpty

    def clear(): Unit = {
      _count = 0
      stat.clear()
    }
  }
}
