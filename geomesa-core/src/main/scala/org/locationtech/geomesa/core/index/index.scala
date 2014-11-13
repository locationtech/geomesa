/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.core

import java.util.{List => JList}

import com.typesafe.scalalogging.slf4j.Logging
import org.apache.accumulo.core.data.{Key, Value, Range => AccRange}
import org.calrissian.mango.types.LexiTypeEncoders
import org.geotools.data.Query
import org.geotools.factory.Hints.{ClassKey, IntegerKey}
import org.geotools.filter.identity.FeatureIdImpl
import org.geotools.geometry.jts.ReferencedEnvelope
import org.joda.time.DateTime
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.identity.FeatureId

/**
 * These are package-wide constants.
 */
package object index {
  val MIN_DATE = new DateTime(Long.MinValue)
  val MAX_DATE = new DateTime(Long.MaxValue)

  val SF_PROPERTY_GEOMETRY   = "geomesa_index_geometry"
  val SF_PROPERTY_START_TIME = "geomesa_index_start_time"
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

  val spec = "geom:Geometry:srid=4326,dtg:Date,dtg_end_time:Date"
  val indexSFT = SimpleFeatureTypes.createType("geomesa-idx", spec)

  implicit def string2id(s: String): FeatureId = new FeatureIdImpl(s)

  type KeyValuePair = (Key, Value)

  object QueryHints {
    val DENSITY_KEY = new ClassKey(classOf[java.lang.Boolean])
    val WIDTH_KEY   = new IntegerKey(256)
    val HEIGHT_KEY  = new IntegerKey(256)
    val BBOX_KEY    = new ClassKey(classOf[ReferencedEnvelope])
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
    private var string: StringBuilder = new StringBuilder()
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

  val doubleAlias = "double"//Double.getClass.getSimpleName.toLowerCase(Locale.US)

  def lexiEncodeDoubleToString(number: Double): String = LexiTypeEncoders.LEXI_TYPES.encode(number)

  def lexiDecodeStringToDouble(str: String): Double = LexiTypeEncoders.LEXI_TYPES.decode(doubleAlias, str).asInstanceOf[Double]
}

