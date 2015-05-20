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

package org.locationtech.geomesa.accumulo

import com.typesafe.scalalogging.slf4j.Logging
import com.vividsolutions.jts.geom.Geometry
import org.apache.accumulo.core.data.{Key, Range => AccRange, Value}
import org.geotools.data.Query
import org.geotools.factory.Hints.{ClassKey, IntegerKey}
import org.geotools.feature.AttributeTypeBuilder
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.geotools.filter.FunctionExpressionImpl
import org.geotools.filter.identity.FeatureIdImpl
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.process.vector.TransformProcess
import org.geotools.process.vector.TransformProcess.Definition
import org.joda.time.{DateTime, DateTimeZone}
import org.locationtech.geomesa.accumulo.data._
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.GeometryAttribute
import org.opengis.feature.`type`.{AttributeDescriptor, GeometryDescriptor}
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.expression.PropertyName
import org.opengis.filter.identity.FeatureId

import scala.collection.JavaConversions._
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
  def getTransformDefinition(query: Query): Option[String] =
    Option(query.getHints.get(TRANSFORMS).asInstanceOf[String])

  /**
   * Get the transform schema set in the query
   */
  def getTransformSchema(query: Query): Option[SimpleFeatureType] =
    Option(query.getHints.get(TRANSFORM_SCHEMA).asInstanceOf[SimpleFeatureType])

  /**
   * Checks for attribute transforms in the query and sets them as hints if found
   *
   * @param query
   * @param sft
   * @return
   */
  def setQueryTransforms(query: Query, sft: SimpleFeatureType) =
    if (query.getProperties != null && !query.getProperties.isEmpty) {
      val (transformProps, regularProps) = query.getPropertyNames.partition(_.contains('='))
      val convertedRegularProps = regularProps.map { p => s"$p=$p" }
      val allTransforms = convertedRegularProps ++ transformProps
      // ensure that the returned props includes geometry, otherwise we get exceptions everywhere
      val geomName = sft.getGeometryDescriptor.getLocalName
      val geomTransform = if (allTransforms.exists(_.matches(s"$geomName\\s*=.*"))) {
        Nil
      } else {
        Seq(s"$geomName=$geomName")
      }
      val transforms = (allTransforms ++ geomTransform).mkString(";")
      val transformDefs = TransformProcess.toDefinition(transforms)
      val derivedSchema = computeSchema(sft, transformDefs)
      query.setProperties(Query.ALL_PROPERTIES)
      query.getHints.put(TRANSFORMS, transforms)
      query.getHints.put(TRANSFORM_SCHEMA, derivedSchema)
    }

  private def computeSchema(origSFT: SimpleFeatureType, transforms: Seq[Definition]): SimpleFeatureType = {
    val attributes: Seq[AttributeDescriptor] = transforms.map { definition =>
      val name = definition.name
      val cql  = definition.expression
      cql match {
        case p: PropertyName =>
          val origAttr = origSFT.getDescriptor(p.getPropertyName)
          val ab = new AttributeTypeBuilder()
          ab.init(origAttr)
          val descriptor = if (origAttr.isInstanceOf[GeometryDescriptor]) {
            ab.buildDescriptor(name, ab.buildGeometryType())
          } else {
            ab.buildDescriptor(name, ab.buildType())
          }
          descriptor.getUserData.putAll(origAttr.getUserData)
          descriptor

        case f: FunctionExpressionImpl  =>
          val clazz = f.getFunctionName.getReturn.getType
          val ab = new AttributeTypeBuilder().binding(clazz)
          if (classOf[Geometry].isAssignableFrom(clazz)) {
            ab.buildDescriptor(name, ab.buildGeometryType())
          } else {
            ab.buildDescriptor(name, ab.buildType())
          }
      }
    }

    val geomAttributes = attributes.filter(_.isInstanceOf[GeometryAttribute]).map(_.getLocalName)
    val sftBuilder = new SimpleFeatureTypeBuilder()
    sftBuilder.setName(origSFT.getName)
    sftBuilder.addAll(attributes.toArray)
    if (geomAttributes.nonEmpty) {
      val defaultGeom = if (geomAttributes.size == 1) { geomAttributes.head } else {
        // try to find a geom with the same name as the original default geom
        val origDefaultGeom = origSFT.getGeometryDescriptor.getLocalName
        geomAttributes.find(_ == origDefaultGeom).getOrElse(geomAttributes.head)
      }
      sftBuilder.setDefaultGeometry(defaultGeom)
    }
    val schema = sftBuilder.buildFeatureType()
    schema.getUserData.putAll(origSFT.getUserData)
    schema
  }

  val spec = "geom:Geometry:srid=4326,dtg:Date,dtg_end_time:Date"
  val indexSFT = SimpleFeatureTypes.createType("geomesa-idx", spec)

  implicit def string2id(s: String): FeatureId = new FeatureIdImpl(s)

  type KeyValuePair = (Key, Value)

  object QueryHints {
    val DENSITY_KEY          = new ClassKey(classOf[java.lang.Boolean])
    val WIDTH_KEY            = new IntegerKey(256)
    val HEIGHT_KEY           = new IntegerKey(256)
    val BBOX_KEY             = new ClassKey(classOf[ReferencedEnvelope])

    val TEMPORAL_DENSITY_KEY = new ClassKey(classOf[java.lang.Boolean])
    val TIME_INTERVAL_KEY    = new ClassKey(classOf[org.joda.time.Interval])
    val TIME_BUCKETS_KEY     = new IntegerKey(256)
    val RETURN_ENCODED       = new ClassKey(classOf[java.lang.Boolean])

    val MAP_AGGREGATION_KEY  = new ClassKey(classOf[java.lang.String])

    val EXACT_COUNT          = new ClassKey(classOf[java.lang.Boolean])
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

