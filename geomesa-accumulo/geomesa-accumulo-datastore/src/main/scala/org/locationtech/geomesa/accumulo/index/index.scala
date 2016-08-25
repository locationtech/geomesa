/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo

import com.vividsolutions.jts.geom.Envelope
import org.apache.accumulo.core.data.{Key, Value}
import org.geotools.factory.Hints
import org.geotools.factory.Hints.{ClassKey, IntegerKey}
import org.geotools.geometry.jts.ReferencedEnvelope
import org.joda.time.{DateTime, DateTimeZone}
import org.locationtech.geomesa.accumulo.data._
import org.locationtech.geomesa.accumulo.index.AccumuloFeatureIndex.AccumuloFeatureIndex
import org.locationtech.geomesa.accumulo.index.QueryPlanner.CostEvaluation
import org.locationtech.geomesa.accumulo.index.QueryPlanner.CostEvaluation.CostEvaluation
import org.opengis.feature.simple.SimpleFeatureType

import scala.languageFeature.implicitConversions

/**
 * These are package-wide constants.
 */
package object index {

  // constrain these dates to the range GeoMesa can index (four-digit years)
  val MIN_DATE = new DateTime(0, 1, 1, 0, 0, 0, DateTimeZone.forID("UTC"))
  val MAX_DATE = new DateTime(9999, 12, 31, 23, 59, 59, DateTimeZone.forID("UTC"))

  type KeyValuePair = (Key, Value)

  object QueryHints {
    val RETURN_SFT_KEY       = new ClassKey(classOf[SimpleFeatureType])
    val QUERY_INDEX_KEY      = new ClassKey(classOf[AccumuloFeatureIndex])
    val COST_EVALUATION_KEY  = new ClassKey(classOf[CostEvaluation])

    val DENSITY_BBOX_KEY     = new ClassKey(classOf[ReferencedEnvelope])
    val DENSITY_WEIGHT       = new ClassKey(classOf[java.lang.String])
    val WIDTH_KEY            = new IntegerKey(256)
    val HEIGHT_KEY           = new IntegerKey(256)

    val STATS_KEY            = new ClassKey(classOf[java.lang.String])
    val RETURN_ENCODED_KEY   = new ClassKey(classOf[java.lang.Boolean])
    val MAP_AGGREGATION_KEY  = new ClassKey(classOf[java.lang.String])

    val EXACT_COUNT          = new ClassKey(classOf[java.lang.Boolean])
    val LOOSE_BBOX           = new ClassKey(classOf[java.lang.Boolean])

    val SAMPLING_KEY         = new ClassKey(classOf[java.lang.Float])
    val SAMPLE_BY_KEY        = new ClassKey(classOf[String])

    val BIN_TRACK_KEY        = new ClassKey(classOf[java.lang.String])
    val BIN_GEOM_KEY         = new ClassKey(classOf[java.lang.String])
    val BIN_DTG_KEY          = new ClassKey(classOf[java.lang.String])
    val BIN_LABEL_KEY        = new ClassKey(classOf[java.lang.String])
    val BIN_SORT_KEY         = new ClassKey(classOf[java.lang.Boolean])
    val BIN_BATCH_SIZE_KEY   = new ClassKey(classOf[java.lang.Integer])

    val CONFIGURED_KEY       = new ClassKey(classOf[java.lang.Boolean])

    implicit class RichHints(val hints: Hints) extends AnyRef {
      import org.locationtech.geomesa.accumulo.GeomesaSystemProperties.QueryProperties.QUERY_COST_TYPE

      def getReturnSft: SimpleFeatureType = hints.get(RETURN_SFT_KEY).asInstanceOf[SimpleFeatureType]
      def getRequestedIndex: Option[AccumuloFeatureIndex] =
        Option(hints.get(QUERY_INDEX_KEY).asInstanceOf[AccumuloFeatureIndex])
      def getCostEvaluation: CostEvaluation = {
        Option(hints.get(COST_EVALUATION_KEY).asInstanceOf[CostEvaluation])
            .orElse(QUERY_COST_TYPE.option.flatMap(t => CostEvaluation.values.find(_.toString.equalsIgnoreCase(t))))
            .getOrElse(CostEvaluation.Stats)
      }
      def isBinQuery: Boolean = hints.containsKey(BIN_TRACK_KEY)
      def getBinTrackIdField: String = hints.get(BIN_TRACK_KEY).asInstanceOf[String]
      def getBinGeomField: Option[String] = Option(hints.get(BIN_GEOM_KEY).asInstanceOf[String])
      def getBinDtgField: Option[String] = Option(hints.get(BIN_DTG_KEY).asInstanceOf[String])
      def getBinLabelField: Option[String] = Option(hints.get(BIN_LABEL_KEY).asInstanceOf[String])
      def getBinBatchSize: Int =
        Option(hints.get(BIN_BATCH_SIZE_KEY).asInstanceOf[Integer]).map(_.intValue).getOrElse(1000)
      def isBinSorting: Boolean = hints.get(BIN_SORT_KEY).asInstanceOf[Boolean]
      def getSamplePercent: Option[Float] = Option(hints.get(SAMPLING_KEY)).map(_.asInstanceOf[Float])
      def getSampleByField: Option[String] = Option(hints.get(SAMPLE_BY_KEY).asInstanceOf[String])
      def getSampling: Option[(Float, Option[String])] = getSamplePercent.map((_, getSampleByField))
      def isDensityQuery: Boolean = hints.containsKey(DENSITY_BBOX_KEY)
      def getDensityEnvelope: Option[Envelope] = Option(hints.get(DENSITY_BBOX_KEY).asInstanceOf[Envelope])
      def getDensityBounds: Option[(Int, Int)] =
        for { w <- Option(hints.get(WIDTH_KEY).asInstanceOf[Int])
              h <- Option(hints.get(HEIGHT_KEY).asInstanceOf[Int]) } yield (w, h)
      def getDensityWeight: Option[String] = Option(hints.get(DENSITY_WEIGHT).asInstanceOf[String])
      def isStatsIteratorQuery: Boolean = hints.containsKey(STATS_KEY)
      def getStatsIteratorQuery: String = hints.get(STATS_KEY).asInstanceOf[String]
      def isMapAggregatingQuery: Boolean = hints.containsKey(MAP_AGGREGATION_KEY)
      def getMapAggregatingAttribute: String = hints.get(MAP_AGGREGATION_KEY).asInstanceOf[String]
      def getTransformDefinition: Option[String] = Option(hints.get(TRANSFORMS).asInstanceOf[String])
      def getTransformSchema: Option[SimpleFeatureType] =
        Option(hints.get(TRANSFORM_SCHEMA).asInstanceOf[SimpleFeatureType])
      def getTransform: Option[(String, SimpleFeatureType)] =
        hints.getTransformDefinition.flatMap(d => hints.getTransformSchema.map((d, _)))
      def isExactCount: Option[Boolean] = Option(hints.get(EXACT_COUNT)).map(_.asInstanceOf[Boolean])
    }
  }
}

