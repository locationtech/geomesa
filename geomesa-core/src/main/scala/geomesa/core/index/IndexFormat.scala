/*
 * Copyright 2013 Commonwealth Computer Research, Inc.
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

package geomesa.core.index

import com.vividsolutions.jts.geom.Geometry
import org.apache.accumulo.core.client.BatchScanner
import org.apache.accumulo.core.data.Key
import org.geotools.data.DataUtilities
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.joda.time.DateTime
import org.opengis.feature.`type`.GeometryDescriptor
import org.opengis.feature.simple.{SimpleFeatureType, SimpleFeature}
import scala.collection.JavaConversions._
import org.joda.time.format.ISODateTimeFormat

trait IndexEntry extends SimpleFeature {
  def getGeometry : Geometry = {
    getAttribute(SF_PROPERTY_GEOMETRY).asInstanceOf[Geometry]
  }
  def setGeometry(geometry:Geometry) {
    setAttribute(SF_PROPERTY_GEOMETRY, geometry)
  }

  private def getTime(attr: String) = getAttribute(attr).asInstanceOf[java.util.Date]
  def getStartTime = getTime(SF_PROPERTY_START_TIME)
  def getEndTime = getTime(SF_PROPERTY_END_TIME)

  private def setTime(attr: String, time: DateTime): Unit =
    setAttribute(attr, if (time==null) null else time.toDate)
  def setStartTime(time: DateTime): Unit = setTime(SF_PROPERTY_START_TIME, time)
  def setEndTime(time: DateTime): Unit = setTime(SF_PROPERTY_END_TIME, time)
}

abstract class TypeInitializer {
  def getTypeName = this.getClass.getCanonicalName
  def getTypeSpec : String
  lazy val expectedNumberOfProperties = getTypeSpec.split(",").length
  lazy val defaultSimpleFeatureType = DataUtilities.createType(getTypeName, getTypeSpec)
  lazy val emptyPropertiesList = List.fill(expectedNumberOfProperties)(null.asInstanceOf[Object])
  lazy val encodedSimpleFeatureType = DataUtilities.encodeType(defaultSimpleFeatureType)
}

object IndexEntryType extends TypeInitializer {
  def getTypeSpec : String =
    SF_PROPERTY_GEOMETRY + ":Geometry:srid=4326," +
    SF_PROPERTY_START_TIME + ":Date," +
    SF_PROPERTY_END_TIME + ":Date"

  def quietlyRemoveAttributeByName(builder:SimpleFeatureTypeBuilder, attributeName:String) {
    try {
      builder.remove(attributeName)
    } catch {
      case e:IllegalArgumentException =>  // nothing; just a trap
    }
  }

  def blendWithType(mixinType:SimpleFeatureType) : SimpleFeatureType = {
    // if you already have the three base fields, then use the given mixin type
    (mixinType.getDescriptor(SF_PROPERTY_GEOMETRY) != null &&
      mixinType.getDescriptor(SF_PROPERTY_START_TIME) != null &&
      mixinType.getDescriptor(SF_PROPERTY_END_TIME) != null) match {

      case true => mixinType
      case false => {
        // initialize the builder to the mixin
        val builder = new SimpleFeatureTypeBuilder
        builder.setName(mixinType.getTypeName)
        builder.init(mixinType)

        // remove the default geometry, if any, exists for this base type
        quietlyRemoveAttributeByName(builder, mixinType.getGeometryDescriptor.getLocalName)
        builder.setDefaultGeometry(null)

        // write the base-type attributes over those provided by the mixin
        val baseType = DataUtilities.createType(mixinType.getTypeName, getTypeSpec)
        baseType.getAttributeDescriptors.foreach { attributeDescriptor => {
          // get rid of the old feature that may have this name
          quietlyRemoveAttributeByName(builder, attributeDescriptor.getLocalName)

          // add the replacement feature
          builder.add(attributeDescriptor)

          // if this is a geometry feature, set it as the default
          if (attributeDescriptor.getType.isInstanceOf[GeometryDescriptor] && builder.getDefaultGeometry==null)
            builder.setDefaultGeometry(attributeDescriptor.getLocalName)
        }}

        // return the net feature type
        builder.buildFeatureType()
      }
    }
  }
}

trait IndexEntryEncoder[E <: IndexEntry] {
  def encode(entry: E): Seq[KeyValuePair]
}
trait IndexEntryDecoder[E <: IndexEntry] {
  def decode(key: Key): E
}

trait Filter

trait QueryPlanner[E <: IndexEntry] {
  def planQuery(bs: BatchScanner, filter: Filter): BatchScanner
}

trait IndexSchema[E <: IndexEntry] {
  val encoder: IndexEntryEncoder[E]
  val decoder: IndexEntryDecoder[E]
  val planner: QueryPlanner[E]

  def encode(entry: E) = encoder.encode(entry)
  def decode(key: Key): E = decoder.decode(key)
  def planQuery(bs: BatchScanner, filter: Filter) = planner.planQuery(bs, filter)
}
