/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.iterators

import java.util.Objects

import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.codec.binary.Base64
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.index.api.GeoMesaFeatureIndex
import org.locationtech.geomesa.index.api.QueryPlan.{FeatureReducer, ResultsToFeatures}
import org.locationtech.geomesa.index.iterators.StatsScan.StatResult
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.locationtech.geomesa.utils.geotools.{GeometryUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.CloseWithLogging
import org.locationtech.geomesa.utils.stats.{Stat, StatSerializer}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

trait StatsScan extends AggregatingScan[StatResult] with LazyLogging {

  import org.locationtech.geomesa.index.iterators.StatsScan.Configuration._

  override protected def createResult(
      sft: SimpleFeatureType,
      transform: Option[SimpleFeatureType],
      batchSize: Int,
      options: Map[String, String]): StatResult = {
    new StatResult(transform.getOrElse(sft), options(STATS_STRING_KEY))
  }

  override protected def defaultBatchSize: Int =
    StatsScan.BatchSize.toInt.get // has a valid default so should be safe to .get
}

object StatsScan {

  val BatchSize: SystemProperty = SystemProperty("geomesa.stats.batch.size", "10000")

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
    val batchSize = StatsScan.BatchSize.toInt.get // has a valid default so should be safe to .get
    AggregatingScan.configure(sft, index, filter, hints.getTransform, hints.getSampling, batchSize) ++
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
   * Result wrapper for stats
   *
   * @param sft simple feature type
   * @param definition stat string
   */
  class StatResult(sft: SimpleFeatureType, definition: String) extends AggregatingScan.Result {

    private val stat = Stat(sft, definition)
    private val serializer = StatSerializer(sft)

    override def init(): Unit = {}

    override def aggregate(sf: SimpleFeature): Int = {
      stat.observe(sf)
      1
    }

    override def encode(): Array[Byte] = try { serializer.serialize(stat) } finally { stat.clear() }

    override def cleanup(): Unit = {}
  }

  /**
    * Stats results to features
    *
    * @tparam T result type
    */
  abstract class StatsResultsToFeatures[T] extends ResultsToFeatures[T] {

    override def state: Map[String, String] = Map.empty

    override def init(state: Map[String, String]): Unit = {}

    override def schema: SimpleFeatureType = StatsScan.StatsSft

    override def apply(result: T): SimpleFeature = {
      val values = Array[AnyRef](Base64.encodeBase64URLSafeString(bytes(result)), GeometryUtils.zeroPoint)
      new ScalaSimpleFeature(StatsScan.StatsSft, "", values)
    }

    protected def bytes(result: T): Array[Byte]

    def canEqual(other: Any): Boolean = other.isInstanceOf[StatsResultsToFeatures[T]]

    override def equals(other: Any): Boolean = other match {
      case that: StatsResultsToFeatures[T] if that.canEqual(this) => true
      case _ => false
    }

    override def hashCode(): Int = schema.hashCode()
  }

  /**
    * Reduces computed simple features which contain stat information into one on the client
    *
    * @param sft sft used for the stat query
    * @param query stat query
    * @param encode encode results or return as json
    */
  class StatsReducer(
      private var sft: SimpleFeatureType,
      private var query: String,
      private var encode: Boolean
    ) extends FeatureReducer {

    def this() = this(null, null, false) // no-arg constructor required for serialization

    override def init(state: Map[String, String]): Unit = {
      sft = SimpleFeatureTypes.createType(state("sft"), state("spec"))
      query = state("q")
      encode = state("e").toBoolean
    }

    override def state: Map[String, String] = Map(
      "sft"  -> sft.getTypeName,
      "spec" -> SimpleFeatureTypes.encodeType(sft, includeUserData = true),
      "q"    -> query,
      "e"    -> encode.toString
    )

    override def apply(features: CloseableIterator[SimpleFeature]): CloseableIterator[SimpleFeature] = {
      try {
        // if no results, create empty stat based on the original input so that we always return something
        val sum = if (features.isEmpty) { Stat(sft, query) } else {
          val decode = decodeStat(sft)
          features.map(f => decode(f.getAttribute(0).asInstanceOf[String])).reduceLeft(reducer)
        }
        val result = if (encode) { encodeStat(sft)(sum) } else { sum.toJson }
        CloseableIterator.single(new ScalaSimpleFeature(StatsSft, "stat", Array(result, GeometryUtils.zeroPoint)))
      } finally {
        CloseWithLogging(features)
      }
    }

    private def reducer(sum: Stat, next: Stat): Stat = { sum += next; sum }

    def canEqual(other: Any): Boolean = other.isInstanceOf[StatsReducer]

    override def equals(other: Any): Boolean = other match {
      case that: StatsReducer if that.canEqual(this) => sft == that.sft && query == that.query && encode == that.encode
      case _ => false
    }

    override def hashCode(): Int = {
      val state = Seq(sft, query, encode)
      state.map(Objects.hashCode).foldLeft(0)((a, b) => 31 * a + b)
    }
  }

  object StatsReducer {
    import org.locationtech.geomesa.index.conf.QueryHints.RichHints

    def apply(sft: SimpleFeatureType, hints: Hints): StatsReducer =
      new StatsReducer(hints.getTransformSchema.getOrElse(sft), hints.getStatsQuery, hints.isStatsEncode)
  }
}
