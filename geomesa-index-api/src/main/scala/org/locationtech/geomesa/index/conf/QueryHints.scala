/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.conf

import org.locationtech.jts.geom.Envelope
import org.geotools.factory.Hints
import org.geotools.factory.Hints.{ClassKey, IntegerKey}
import org.geotools.geometry.jts.ReferencedEnvelope
import org.locationtech.geomesa.index.planning.QueryPlanner.CostEvaluation
import org.locationtech.geomesa.index.planning.QueryPlanner.CostEvaluation.CostEvaluation
import org.locationtech.geomesa.utils.text.StringSerialization
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.sort.{SortBy, SortOrder}

object QueryHints {

  val QUERY_INDEX      = new ClassKey(classOf[String])
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
  val ARROW_PROXY_FID          = new ClassKey(classOf[java.lang.Boolean])
  val ARROW_BATCH_SIZE         = new ClassKey(classOf[java.lang.Integer])
  val ARROW_SORT_FIELD         = new ClassKey(classOf[java.lang.String])
  val ARROW_SORT_REVERSE       = new ClassKey(classOf[java.lang.Boolean])

  val ARROW_DICTIONARY_FIELDS  = new ClassKey(classOf[java.lang.String])
  val ARROW_DICTIONARY_VALUES  = new ClassKey(classOf[java.lang.String])
  val ARROW_DICTIONARY_CACHED  = new ClassKey(classOf[java.lang.Boolean])

  val ARROW_MULTI_FILE         = new ClassKey(classOf[java.lang.Boolean])
  val ARROW_DOUBLE_PASS        = new ClassKey(classOf[java.lang.Boolean])

  val LAMBDA_QUERY_PERSISTENT  = new ClassKey(classOf[java.lang.Boolean])
  val LAMBDA_QUERY_TRANSIENT   = new ClassKey(classOf[java.lang.Boolean])

  // internal hints that shouldn't be set directly by users
  object Internal {
    val RETURN_SFT       = new ClassKey(classOf[SimpleFeatureType])
    val TRANSFORMS       = new ClassKey(classOf[String])
    val TRANSFORM_SCHEMA = new ClassKey(classOf[SimpleFeatureType])
    val SORT_FIELDS      = new ClassKey(classOf[String])
    val MAX_FEATURES     = new ClassKey(classOf[java.lang.Integer])
    val SKIP_REDUCE      = new ClassKey(classOf[java.lang.Boolean])

    def toSortHint(sortBy: Array[SortBy]): String = {
      val hints = sortBy.map {
        case SortBy.NATURAL_ORDER => ":false"
        case SortBy.REVERSE_ORDER => ":true"
        case sb =>
          val name = Option(sb.getPropertyName).map(_.getPropertyName).getOrElse("")
          s"$name:${sb.getSortOrder == SortOrder.DESCENDING}"
      }
      hints.mkString(",")
    }

    def fromSortHint(hint: String): Seq[(String, Boolean)] = {
      hint.split(",").toSeq.map { h =>
        h.split(":") match {
          case Array(field, reverse) => (field, reverse.toBoolean)
          case _ => throw new IllegalArgumentException(s"Invalid sort field, expected 'name:reverse' but got '$h'")
        }
      }
    }
  }

  implicit class RichHints(val hints: Hints) extends AnyRef {

    def getReturnSft: SimpleFeatureType = hints.get(Internal.RETURN_SFT).asInstanceOf[SimpleFeatureType]
    def getRequestedIndex: Option[String] = Option(hints.get(QUERY_INDEX).asInstanceOf[String])
    def getCostEvaluation: CostEvaluation = {
      Option(hints.get(COST_EVALUATION).asInstanceOf[CostEvaluation])
          .orElse(QueryProperties.QueryCostType.option.flatMap(t => CostEvaluation.values.find(_.toString.equalsIgnoreCase(t))))
          .getOrElse(CostEvaluation.Index)
    }
    def isSkipReduce: Boolean = Option(hints.get(Internal.SKIP_REDUCE).asInstanceOf[java.lang.Boolean]).exists(_.booleanValue())
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
    def isArrowMultiFile: Boolean = Option(hints.get(ARROW_MULTI_FILE).asInstanceOf[java.lang.Boolean]).exists(Boolean.unbox)
    def isArrowDoublePass: Boolean = Option(hints.get(ARROW_DOUBLE_PASS).asInstanceOf[java.lang.Boolean]).exists(Boolean.unbox)
    def isArrowIncludeFid: Boolean = Option(hints.get(ARROW_INCLUDE_FID).asInstanceOf[java.lang.Boolean]).forall(Boolean.unbox)
    def isArrowProxyFid: Boolean = Option(hints.get(ARROW_PROXY_FID).asInstanceOf[java.lang.Boolean]).exists(Boolean.unbox)
    def getArrowDictionaryFields: Seq[String] =
      Option(hints.get(ARROW_DICTIONARY_FIELDS).asInstanceOf[String]).toSeq.flatMap(_.split(",")).map(_.trim).filter(_.nonEmpty)
    def isArrowCachedDictionaries: Boolean =
      Option(hints.get(ARROW_DICTIONARY_CACHED).asInstanceOf[java.lang.Boolean]).forall(Boolean.unbox)
    def getArrowDictionaryEncodedValues(sft: SimpleFeatureType): Map[String, Array[AnyRef]] =
      Option(hints.get(ARROW_DICTIONARY_VALUES).asInstanceOf[String]).map(StringSerialization.decodeSeqMap(sft, _)).getOrElse(Map.empty)
    def setArrowDictionaryEncodedValues(values: Map[String, Seq[AnyRef]]): Unit =
      hints.put(ARROW_DICTIONARY_VALUES, StringSerialization.encodeSeqMap(values))
    def getArrowBatchSize: Option[Int] = Option(hints.get(ARROW_BATCH_SIZE).asInstanceOf[Integer]).map(_.intValue)
    def getArrowSort: Option[(String, Boolean)] =
      Option(hints.get(ARROW_SORT_FIELD).asInstanceOf[String]).map { field =>
        (field, Option(hints.get(ARROW_SORT_REVERSE)).exists(_.asInstanceOf[Boolean]))
      }

    def isStatsQuery: Boolean = hints.containsKey(STATS_STRING)
    def getStatsQuery: String = hints.get(STATS_STRING).asInstanceOf[String]
    // noinspection ExistsEquals
    def isStatsEncode: Boolean = Option(hints.get(ENCODE_STATS).asInstanceOf[Boolean]).exists(_ == true)
    def isMapAggregatingQuery: Boolean = hints.containsKey(MAP_AGGREGATION)
    def getMapAggregatingAttribute: String = hints.get(MAP_AGGREGATION).asInstanceOf[String]
    def getTransformDefinition: Option[String] = Option(hints.get(Internal.TRANSFORMS).asInstanceOf[String])
    def getTransformSchema: Option[SimpleFeatureType] =
      Option(hints.get(Internal.TRANSFORM_SCHEMA).asInstanceOf[SimpleFeatureType])
    def getTransform: Option[(String, SimpleFeatureType)] =
      hints.getTransformDefinition.flatMap(d => hints.getTransformSchema.map((d, _)))
    def clearTransforms(): Unit = {
      hints.remove(Internal.TRANSFORM_SCHEMA)
      hints.remove(Internal.TRANSFORMS)
    }
    def getSortFields: Option[Seq[(String, Boolean)]] =
      Option(hints.get(Internal.SORT_FIELDS).asInstanceOf[String]).map(Internal.fromSortHint).filterNot(_.isEmpty)
    def getSortReadableString: String =
      getSortFields.map(_.map { case (f, r) => s"$f ${if (r) "DESC" else "ASC" }"}.mkString(", ")).getOrElse("none")
    def getMaxFeatures: Option[Int] = Option(hints.get(Internal.MAX_FEATURES).asInstanceOf[Integer]).map(_.intValue())
    def isExactCount: Option[Boolean] = Option(hints.get(EXACT_COUNT)).map(_.asInstanceOf[Boolean])
    def isLambdaQueryPersistent: Boolean =
      Option(hints.get(LAMBDA_QUERY_PERSISTENT).asInstanceOf[java.lang.Boolean]).forall(_.booleanValue)
    def isLambdaQueryTransient: Boolean =
      Option(hints.get(LAMBDA_QUERY_TRANSIENT).asInstanceOf[java.lang.Boolean]).forall(_.booleanValue)
  }
}
