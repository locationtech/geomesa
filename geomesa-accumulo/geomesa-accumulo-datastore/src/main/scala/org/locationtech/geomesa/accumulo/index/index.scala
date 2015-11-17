/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo

import com.typesafe.scalalogging.slf4j.Logging
import com.vividsolutions.jts.geom.Envelope
import org.apache.accumulo.core.data.{Key, Range => AccRange, Value}
import org.geotools.factory.Hints
import org.geotools.factory.Hints.{ClassKey, IntegerKey}
import org.geotools.filter.identity.FeatureIdImpl
import org.geotools.geometry.jts.ReferencedEnvelope
import org.joda.time.{DateTime, DateTimeZone}
import org.locationtech.geomesa.accumulo.data._
import org.locationtech.geomesa.accumulo.index.Strategy.StrategyType._
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.identity.FeatureId

import scala.languageFeature.implicitConversions

/**
 * These are package-wide constants.
 */
package object index {
  // constrain these dates to the range GeoMesa can index (four-digit years)
  val MIN_DATE = new DateTime(0, 1, 1, 0, 0, 0, DateTimeZone.forID("UTC"))
  val MAX_DATE = new DateTime(9999, 12, 31, 23, 59, 59, DateTimeZone.forID("UTC"))

  val spec = "geom:Geometry:srid=4326,dtg:Date,dtg_end_time:Date"
  val indexSFT = SimpleFeatureTypes.createType("geomesa-idx", spec)

  implicit def string2id(s: String): FeatureId = new FeatureIdImpl(s)

  type KeyValuePair = (Key, Value)

  object QueryHints {
    val RETURN_SFT_KEY                = new ClassKey(classOf[SimpleFeatureType])
    val QUERY_STRATEGY_KEY            = new ClassKey(classOf[StrategyType])

    val DENSITY_BBOX_KEY              = new ClassKey(classOf[ReferencedEnvelope])
    val DENSITY_WEIGHT                = new ClassKey(classOf[java.lang.String])
    val WIDTH_KEY                     = new IntegerKey(256)
    val HEIGHT_KEY                    = new IntegerKey(256)

    val RANGE_HISTOGRAM_KEY           = new ClassKey(classOf[java.lang.Boolean])
    val RANGE_HISTOGRAM_INTERVAL_KEY  = new ClassKey(classOf[com.google.common.collect.Range[java.lang.Long]])
    val RANGE_HISTOGRAM_BUCKETS_KEY   = new IntegerKey(256)
    val RANGE_HISTOGRAM_ATTRIBUTE     = new ClassKey(classOf[java.lang.String])
    val RETURN_ENCODED                = new ClassKey(classOf[java.lang.Boolean])

    val MAP_AGGREGATION_KEY           = new ClassKey(classOf[java.lang.String])

    val EXACT_COUNT                   = new ClassKey(classOf[java.lang.Boolean])

    val BIN_TRACK_KEY                 = new ClassKey(classOf[java.lang.String])
    val BIN_GEOM_KEY                  = new ClassKey(classOf[java.lang.String])
    val BIN_DTG_KEY                   = new ClassKey(classOf[java.lang.String])
    val BIN_LABEL_KEY                 = new ClassKey(classOf[java.lang.String])
    val BIN_SORT_KEY                  = new ClassKey(classOf[java.lang.Boolean])
    val BIN_BATCH_SIZE_KEY            = new ClassKey(classOf[java.lang.Integer])

    implicit class RichHints(val hints: Hints) extends AnyRef {
      def getReturnSft: SimpleFeatureType = hints.get(RETURN_SFT_KEY).asInstanceOf[SimpleFeatureType]
      def getRequestedStrategy: Option[StrategyType] =
        Option(hints.get(QUERY_STRATEGY_KEY).asInstanceOf[StrategyType])
      def isBinQuery: Boolean = hints.containsKey(BIN_TRACK_KEY)
      def getBinTrackIdField: String = hints.get(BIN_TRACK_KEY).asInstanceOf[String]
      def getBinGeomField: Option[String] = Option(hints.get(BIN_GEOM_KEY).asInstanceOf[String])
      def getBinDtgField: Option[String] = Option(hints.get(BIN_DTG_KEY).asInstanceOf[String])
      def getBinLabelField: Option[String] = Option(hints.get(BIN_LABEL_KEY).asInstanceOf[String])
      def getBinBatchSize: Int = hints.get(BIN_BATCH_SIZE_KEY).asInstanceOf[Int]
      def isBinSorting: Boolean = hints.get(BIN_SORT_KEY).asInstanceOf[Boolean]
      def isDensityQuery: Boolean = hints.containsKey(DENSITY_BBOX_KEY)
      def getDensityEnvelope: Option[Envelope] = Option(hints.get(DENSITY_BBOX_KEY).asInstanceOf[Envelope])
      def getDensityBounds: Option[(Int, Int)] =
        for { w <- Option(hints.get(WIDTH_KEY).asInstanceOf[Int])
              h <- Option(hints.get(HEIGHT_KEY).asInstanceOf[Int]) } yield (w, h)
      def getDensityWeight: Option[String] = Option(hints.get(DENSITY_WEIGHT).asInstanceOf[String])
      def isRangeHistogramQuery: Boolean = hints.containsKey(RANGE_HISTOGRAM_KEY)
      def isMapAggregatingQuery: Boolean = hints.containsKey(MAP_AGGREGATION_KEY)
      def getTransformDefinition: Option[String] = Option(hints.get(TRANSFORMS).asInstanceOf[String])
      def getTransformSchema: Option[SimpleFeatureType] =
        Option(hints.get(TRANSFORM_SCHEMA).asInstanceOf[SimpleFeatureType])
      def getTransform: Option[(String, SimpleFeatureType)] =
        hints.getTransformDefinition.flatMap(d => hints.getTransformSchema.map((d, _)))
    }
  }

  type ExplainerOutputType = ( => String) => Unit

  object ExplainerOutputType {

    def toString(r: AccRange) = {
      val first = if (r.isStartKeyInclusive) "[" else "("
      val last =  if (r.isEndKeyInclusive) "]" else ")"
      val start = Option(r.getStartKey).map(_.toStringNoTime).getOrElse("-inf")
      val end = Option(r.getEndKey).map(_.toStringNoTime).getOrElse("+inf")
      first + start + ", " + end + last
    }
  }

  object ExplainPrintln extends ExplainerOutputType {
    override def apply(v1: => String): Unit = println(v1)
  }

  object ExplainNull extends ExplainerOutputType {
    override def apply(v1: => String): Unit = {}
  }

  class ExplainString extends ExplainerOutputType {
    private val string: StringBuilder = new StringBuilder()
    override def apply(v1: => String) = {
      string.append(v1).append('\n')
    }
    override def toString() = string.toString()
  }

  trait ExplainingLogging extends Logging {
    def log(stringFnx: => String) = {
      lazy val s: String = stringFnx
      logger.trace(s)
    }
  }
}

