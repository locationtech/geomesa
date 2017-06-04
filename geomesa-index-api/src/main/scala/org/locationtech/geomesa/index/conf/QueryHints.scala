/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.conf

import com.vividsolutions.jts.geom.Envelope
import org.geotools.factory.Hints
import org.geotools.factory.Hints.{ClassKey, IntegerKey}
import org.geotools.geometry.jts.ReferencedEnvelope
import org.locationtech.geomesa.index.api.QueryPlanner.CostEvaluation
import org.locationtech.geomesa.index.api.QueryPlanner.CostEvaluation.CostEvaluation
import org.locationtech.geomesa.index.api.{GeoMesaFeatureIndex, WrappedFeature}
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.utils.text.StringSerialization
import org.opengis.feature.simple.SimpleFeatureType

object QueryHints {

  val QUERY_INDEX      = new ClassKey(classOf[GeoMesaFeatureIndex[_, _, _]])
  val COST_EVALUATION  = new ClassKey(classOf[CostEvaluation])

  val DENSITY_BBOX     = new ClassKey(classOf[ReferencedEnvelope])
  val DENSITY_WEIGHT   = new ClassKey(classOf[java.lang.String])
  val DENSITY_WIDTH    = new IntegerKey(256)
  val DENSITY_HEIGHT   = new IntegerKey(256)

  val STATS_STRING     = new ClassKey(classOf[java.lang.String])
  val ENCODE_STATS     = new ClassKey(classOf[java.lang.Boolean])
  val MAP_AGGREGATION  = new ClassKey(classOf[java.lang.String])

  val EXACT_COUNT      = new ClassKey(classOf[java.lang.Boolean])
  val LOOSE_BBOX       = new ClassKey(classOf[java.lang.Boolean])

  val SAMPLING         = new ClassKey(classOf[java.lang.Float])
  val SAMPLE_BY        = new ClassKey(classOf[String])

  val BIN_TRACK        = new ClassKey(classOf[java.lang.String])
  val BIN_GEOM         = new ClassKey(classOf[java.lang.String])
  val BIN_DTG          = new ClassKey(classOf[java.lang.String])
  val BIN_LABEL        = new ClassKey(classOf[java.lang.String])
  val BIN_SORT         = new ClassKey(classOf[java.lang.Boolean])
  val BIN_BATCH_SIZE   = new ClassKey(classOf[java.lang.Integer])

  val ARROW_ENCODE             = new ClassKey(classOf[java.lang.Boolean])
  val ARROW_INCLUDE_FID        = new ClassKey(classOf[java.lang.Boolean])
  val ARROW_DICTIONARY_FIELDS  = new ClassKey(classOf[java.lang.String])
  val ARROW_DICTIONARY_VALUES  = new ClassKey(classOf[java.lang.String])
  val ARROW_DICTIONARY_COMPUTE = new ClassKey(classOf[java.lang.Boolean])
  val ARROW_BATCH_SIZE         = new ClassKey(classOf[java.lang.Integer])
  val ARROW_SORT_FIELD         = new ClassKey(classOf[java.lang.String])
  val ARROW_SORT_REVERSE       = new ClassKey(classOf[java.lang.Boolean])

  // internal hints that shouldn't be set directly by users
  object Internal {
    val RETURN_SFT       = new ClassKey(classOf[SimpleFeatureType])
    val TRANSFORMS       = new ClassKey(classOf[String])
    val TRANSFORM_SCHEMA = new ClassKey(classOf[SimpleFeatureType])
  }

  implicit class RichHints(val hints: Hints) extends AnyRef {

    def getReturnSft: SimpleFeatureType = hints.get(Internal.RETURN_SFT).asInstanceOf[SimpleFeatureType]
    def getRequestedIndex[O <: GeoMesaDataStore[O, F, W], F <: WrappedFeature, W]: Option[GeoMesaFeatureIndex[O, F, W]] =
      Option(hints.get(QUERY_INDEX).asInstanceOf[GeoMesaFeatureIndex[O, F, W]])
    def getCostEvaluation: CostEvaluation = {
      Option(hints.get(COST_EVALUATION).asInstanceOf[CostEvaluation])
          .orElse(QueryProperties.QUERY_COST_TYPE.option.flatMap(t => CostEvaluation.values.find(_.toString.equalsIgnoreCase(t))))
          .getOrElse(CostEvaluation.Stats)
    }
    def isBinQuery: Boolean = hints.containsKey(BIN_TRACK)
    def getBinTrackIdField: String = hints.get(BIN_TRACK).asInstanceOf[String]
    def getBinGeomField: Option[String] = Option(hints.get(BIN_GEOM).asInstanceOf[String])
    def getBinDtgField: Option[String] = Option(hints.get(BIN_DTG).asInstanceOf[String])
    def getBinLabelField: Option[String] = Option(hints.get(BIN_LABEL).asInstanceOf[String])
    def getBinBatchSize: Int =
      Option(hints.get(BIN_BATCH_SIZE).asInstanceOf[Integer]).map(_.intValue).getOrElse(1000)
    def isBinSorting: Boolean = hints.get(BIN_SORT).asInstanceOf[Boolean]
    def getSamplePercent: Option[Float] = Option(hints.get(SAMPLING)).map(_.asInstanceOf[Float])
    def getSampleByField: Option[String] = Option(hints.get(SAMPLE_BY).asInstanceOf[String])
    def getSampling: Option[(Float, Option[String])] = getSamplePercent.map((_, getSampleByField))
    def isDensityQuery: Boolean = hints.containsKey(DENSITY_BBOX)
    def getDensityEnvelope: Option[Envelope] = Option(hints.get(DENSITY_BBOX).asInstanceOf[Envelope])
    def getDensityBounds: Option[(Int, Int)] =
      for { w <- Option(hints.get(DENSITY_WIDTH).asInstanceOf[Int])
            h <- Option(hints.get(DENSITY_HEIGHT).asInstanceOf[Int]) } yield (w, h)
    def getDensityWeight: Option[String] = Option(hints.get(DENSITY_WEIGHT).asInstanceOf[String])
    def isArrowQuery: Boolean = Option(hints.get(ARROW_ENCODE).asInstanceOf[java.lang.Boolean]).exists(Boolean.unbox)
    def isArrowIncludeFid: Boolean = Option(hints.get(ARROW_INCLUDE_FID).asInstanceOf[java.lang.Boolean]).forall(Boolean.unbox)
    def getArrowDictionaryFields: Seq[String] =
      Option(hints.get(ARROW_DICTIONARY_FIELDS).asInstanceOf[String]).toSeq.flatMap(_.split(",")).map(_.trim).filter(_.nonEmpty)
    def isArrowComputeDictionaries: Boolean =
      Option(hints.get(ARROW_DICTIONARY_COMPUTE).asInstanceOf[java.lang.Boolean]).forall(Boolean.unbox)
    def getArrowDictionaryEncodedValues: Map[String, Seq[AnyRef]] =
      Option(hints.get(ARROW_DICTIONARY_VALUES).asInstanceOf[String]).map(StringSerialization.decodeSeqMap).getOrElse(Map.empty)
    def getArrowBatchSize: Option[Int] = Option(hints.get(ARROW_BATCH_SIZE).asInstanceOf[Integer]).map(_.intValue)
    def getArrowSort: Option[(String, Boolean)] =
      Option(hints.get(ARROW_SORT_FIELD).asInstanceOf[String]).map { field =>
        (field, Option(hints.get(ARROW_SORT_REVERSE)).exists(_.asInstanceOf[Boolean]))
      }
    def isStatsIteratorQuery: Boolean = hints.containsKey(STATS_STRING)
    def getStatsIteratorQuery: String = hints.get(STATS_STRING).asInstanceOf[String]
    def isMapAggregatingQuery: Boolean = hints.containsKey(MAP_AGGREGATION)
    def getMapAggregatingAttribute: String = hints.get(MAP_AGGREGATION).asInstanceOf[String]
    def getTransformDefinition: Option[String] = Option(hints.get(Internal.TRANSFORMS).asInstanceOf[String])
    def getTransformSchema: Option[SimpleFeatureType] =
      Option(hints.get(Internal.TRANSFORM_SCHEMA).asInstanceOf[SimpleFeatureType])
    def getTransform: Option[(String, SimpleFeatureType)] =
      hints.getTransformDefinition.flatMap(d => hints.getTransformSchema.map((d, _)))
    def isExactCount: Option[Boolean] = Option(hints.get(EXACT_COUNT)).map(_.asInstanceOf[Boolean])
  }
}
