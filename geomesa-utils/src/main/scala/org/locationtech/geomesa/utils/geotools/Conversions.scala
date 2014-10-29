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

package org.locationtech.geomesa.utils.geotools

import java.util.{Collection => JCollection, Map => JMap}

import com.vividsolutions.jts.geom._
import org.geotools.data.simple.SimpleFeatureIterator
import org.geotools.factory.Hints
import org.geotools.geometry.DirectPosition2D
import org.geotools.temporal.`object`.{DefaultInstant, DefaultPeriod, DefaultPosition}
import org.geotools.util.{Converter, ConverterFactory}
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.SimpleFeature
import org.opengis.temporal.Instant

object Conversions {

  class RichSimpleFeatureIterator(iter: SimpleFeatureIterator) extends SimpleFeatureIterator
      with Iterator[SimpleFeature] {
    private[this] var open = true

    def isClosed = !open

    def hasNext = {
      if (isClosed) false
      if(iter.hasNext) true else{close(); false}
    }
    def next() = iter.next
    def close() { if(!isClosed) {iter.close(); open = false} }
  }

  implicit def toRichSimpleFeatureIterator(iter: SimpleFeatureIterator) = new RichSimpleFeatureIterator(iter)
  implicit def opengisInstantToJodaInstant(instant: Instant): org.joda.time.Instant = new DateTime(instant.getPosition.getDate).toInstant
  implicit def jodaInstantToOpengisInstant(instant: org.joda.time.Instant): org.opengis.temporal.Instant = new DefaultInstant(new DefaultPosition(instant.toDate))
  implicit def jodaIntervalToOpengisPeriod(interval: org.joda.time.Interval): org.opengis.temporal.Period =
    new DefaultPeriod(interval.getStart.toInstant, interval.getEnd.toInstant)


  implicit class RichCoord(val c: Coordinate) extends AnyVal {
    def toPoint2D = new DirectPosition2D(c.x, c.y)
  }

  implicit class RichGeometry(val geom: Geometry) extends AnyVal {
    def bufferMeters(meters: Double): Geometry = geom.buffer(distanceDegrees(meters))
    def distanceDegrees(meters: Double) = GeometryUtils.distanceDegrees(geom, meters)
  }

  implicit class RichSimpleFeature(val sf: SimpleFeature) extends AnyVal {
    def geometry = sf.getDefaultGeometry.asInstanceOf[Geometry]
    def polygon = sf.getDefaultGeometry.asInstanceOf[Polygon]
    def point = sf.getDefaultGeometry.asInstanceOf[Point]
    def lineString = sf.getDefaultGeometry.asInstanceOf[LineString]
    def multiPolygon = sf.getDefaultGeometry.asInstanceOf[MultiPolygon]
    def multiPoint = sf.getDefaultGeometry.asInstanceOf[MultiPoint]
    def multiLineString = sf.getDefaultGeometry.asInstanceOf[MultiLineString]
  }

  import scala.collection.JavaConversions._
  implicit class RichAttributeDescriptor(val attr: AttributeDescriptor) extends AnyVal {
    def isIndexed = attr.getUserData.getOrElse("index", false).asInstanceOf[java.lang.Boolean]
    def isCollection = classOf[JCollection[_]].isAssignableFrom(attr.getType.getBinding)
    def isMap = classOf[JMap[_, _]].isAssignableFrom(attr.getType.getBinding)
    def isMultiValued = isCollection || isMap
  }
}

class JodaConverterFactory extends ConverterFactory {
  private val df = ISODateTimeFormat.dateTime()
  def createConverter(source: Class[_], target: Class[_], hints: Hints) =
    if(classOf[java.util.Date].isAssignableFrom(source) && classOf[String].isAssignableFrom(target)) {
      // Date => String
      new Converter {
        def convert[T](source: scala.Any, target: Class[T]): T =
          df.print(new DateTime(source.asInstanceOf[java.util.Date])).asInstanceOf[T]
      }
    } else if(classOf[java.util.Date].isAssignableFrom(target) && classOf[String].isAssignableFrom(source)) {
      // String => Date
      new Converter {
        def convert[T](source: scala.Any, target: Class[T]): T =
          df.parseDateTime(source.asInstanceOf[String]).toDate.asInstanceOf[T]
      }
    } else null.asInstanceOf[Converter]
}

class ScalaCollectionsConverterFactory extends ConverterFactory {

  def createConverter(source: Class[_], target: Class[_], hints: Hints): Converter =
    if (classOf[Seq[_]].isAssignableFrom(source)
        && classOf[java.util.List[_]].isAssignableFrom(target)) {
      new ListToListConverter(true)
    } else if (classOf[java.util.List[_]].isAssignableFrom(source)
        && classOf[Seq[_]].isAssignableFrom(target)) {
      new ListToListConverter(false)
    } else if (classOf[Map[_, _]].isAssignableFrom(source)
        && classOf[java.util.Map[_, _]].isAssignableFrom(target)) {
      new MapToMapConverter(true)
    } else if (classOf[java.util.Map[_, _]].isAssignableFrom(source)
        && classOf[Map[_, _]].isAssignableFrom(target)) {
      new MapToMapConverter(false)
    } else {
      null
    }
}

/**
 * Convert between scala and java lists
 *
 * @param scalaToJava
 */
class ListToListConverter(scalaToJava: Boolean) extends Converter {

  import scala.collection.JavaConverters._

  override def convert[T](source: scala.Any, target: Class[T]): T =
    if (scalaToJava) {
      source.asInstanceOf[Seq[_]].asJava.asInstanceOf[T]
    } else {
      source.asInstanceOf[java.util.List[_]].asScala.asInstanceOf[T]
    }

}

/**
 * Convert between scala and java maps
 *
 * @param scalaToJava
 */
class MapToMapConverter(scalaToJava: Boolean) extends Converter {

  import scala.collection.JavaConverters._

  override def convert[T](source: scala.Any, target: Class[T]): T =
    if (scalaToJava) {
      source.asInstanceOf[Map[_, _]].asJava.asInstanceOf[T]
    } else {
      source.asInstanceOf[java.util.Map[_, _]].asScala.asInstanceOf[T]
    }
}