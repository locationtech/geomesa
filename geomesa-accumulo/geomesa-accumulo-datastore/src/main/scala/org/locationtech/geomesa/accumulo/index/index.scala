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
import org.geotools.data.Query
import org.geotools.factory.Hints
import org.geotools.factory.Hints.{ClassKey, IntegerKey}
import org.geotools.filter.identity.FeatureIdImpl
import org.geotools.geometry.jts.ReferencedEnvelope
import org.joda.time.{DateTime, DateTimeZone}
import org.locationtech.geomesa.accumulo.data._
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

  val SF_PROPERTY_GEOMETRY   = "geomesa_index_geometry"
  val SF_PROPERTY_START_TIME = SimpleFeatureTypes.DEFAULT_DATE_FIELD
  val SF_PROPERTY_END_TIME   = "geomesa_index_end_time"
  val SFT_INDEX_SCHEMA       = "geomesa_index_schema"
  val SF_TABLE_SHARING       = "geomesa_table_sharing"

  // wrapping function in option to protect against incorrect values in SF_PROPERTY_START_TIME
  def getDtgFieldName(sft: SimpleFeatureType) =
    for {
      nameFromUserData <- Option(sft.getUserData.get(SF_PROPERTY_START_TIME)).map { _.toString }
      if Option(sft.getDescriptor(nameFromUserData)).isDefined
    } yield nameFromUserData

  // wrapping function in option to protect against incorrect values in SF_PROPERTY_START_TIME
  def getDtgDescriptor(sft: SimpleFeatureType) = getDtgFieldName(sft).flatMap{name => Option(sft.getDescriptor(name))}

  def setDtgDescriptor(sft: SimpleFeatureType, dateFieldName: String) {
    sft.getUserData.put(SF_PROPERTY_START_TIME, dateFieldName)
  }

  def getIndexSchema(sft: SimpleFeatureType) = Option(sft.getUserData.get(SFT_INDEX_SCHEMA)).map { _.toString }
  def setIndexSchema(sft: SimpleFeatureType, indexSchema: String) {
    sft.getUserData.put(SFT_INDEX_SCHEMA, indexSchema)
  }

  def getTableSharing(sft: SimpleFeatureType): Boolean = {
    //  If no data is stored in Accumulo, it means we have an old table, so that means 'false'
    //  If no user data is specified when creating a new SFT, we should default to 'true'.
    if (sft.getUserData.containsKey(SF_TABLE_SHARING)) {
      java.lang.Boolean.valueOf(sft.getUserData.get(SF_TABLE_SHARING).toString).booleanValue()
    } else {
      true
    }
  }

  def setTableSharing(sft: SimpleFeatureType, sharing: java.lang.Boolean) {
    sft.getUserData.put(SF_TABLE_SHARING, sharing)
  }

  def getTableSharingPrefix(sft: SimpleFeatureType): String =
    if(getTableSharing(sft)) s"${sft.getTypeName}~"
    else                     ""

  /**
   * Get the transforms set in the query
   */
  def getTransformDefinition(query: Query): Option[String] = getTransformDefinition(query.getHints)

  /**
   * Get the transforms set in the query
   */
  def getTransformDefinition(hints: Hints): Option[String] = Option(hints.get(TRANSFORMS).asInstanceOf[String])

  /**
   * Get the transform schema set in the query
   */
  def getTransformSchema(query: Query): Option[SimpleFeatureType] = getTransformSchema(query.getHints)

  /**
   * Get the transform schema set in the query hints
   */
  def getTransformSchema(hints: Hints): Option[SimpleFeatureType] =
    Option(hints.get(TRANSFORM_SCHEMA).asInstanceOf[SimpleFeatureType])

  val spec = "geom:Geometry:srid=4326,dtg:Date,dtg_end_time:Date"
  val indexSFT = SimpleFeatureTypes.createType("geomesa-idx", spec)

  implicit def string2id(s: String): FeatureId = new FeatureIdImpl(s)

  type KeyValuePair = (Key, Value)

  object QueryHints {
    val RETURN_SFT_KEY       = new ClassKey(classOf[SimpleFeatureType])

    val DENSITY_BBOX_KEY     = new ClassKey(classOf[ReferencedEnvelope])
    val DENSITY_WEIGHT       = new ClassKey(classOf[java.lang.String])
    val WIDTH_KEY            = new IntegerKey(256)
    val HEIGHT_KEY           = new IntegerKey(256)

    val TEMPORAL_DENSITY_KEY = new ClassKey(classOf[java.lang.Boolean])
    val TIME_INTERVAL_KEY    = new ClassKey(classOf[org.joda.time.Interval])
    val TIME_BUCKETS_KEY     = new IntegerKey(256)
    val RETURN_ENCODED       = new ClassKey(classOf[java.lang.Boolean])

    val MAP_AGGREGATION_KEY  = new ClassKey(classOf[java.lang.String])

    val EXACT_COUNT          = new ClassKey(classOf[java.lang.Boolean])

    val BIN_TRACK_KEY        = new ClassKey(classOf[java.lang.String])
    val BIN_GEOM_KEY         = new ClassKey(classOf[java.lang.String])
    val BIN_DTG_KEY          = new ClassKey(classOf[java.lang.String])
    val BIN_LABEL_KEY        = new ClassKey(classOf[java.lang.String])
    val BIN_SORT_KEY         = new ClassKey(classOf[java.lang.Boolean])
    val BIN_BATCH_SIZE_KEY   = new ClassKey(classOf[java.lang.Integer])

    implicit class RichHints(val hints: Hints) extends AnyRef {
      def getReturnSft: SimpleFeatureType = hints.get(RETURN_SFT_KEY).asInstanceOf[SimpleFeatureType]
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
      def isTemporalDensityQuery: Boolean = hints.containsKey(TEMPORAL_DENSITY_KEY)
      def isMapAggregatingQuery: Boolean = hints.containsKey(MAP_AGGREGATION_KEY)
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

    def toString(q: Query) = q.toString.replaceFirst("\\n\\s*", " ").replaceAll("\\n\\s*", ", ")
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

