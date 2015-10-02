/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.utils.geotools

import java.util.Date

import com.vividsolutions.jts.geom._
import org.geotools.data.FeatureReader
import org.geotools.data.simple.SimpleFeatureIterator
import org.geotools.factory.Hints
import org.geotools.feature.AttributeTypeBuilder
import org.geotools.geometry.DirectPosition2D
import org.geotools.temporal.`object`.{DefaultInstant, DefaultPeriod, DefaultPosition}
import org.geotools.util.{Converter, ConverterFactory}
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat
import org.locationtech.geomesa.CURRENT_SCHEMA_VERSION
import org.locationtech.geomesa.utils.stats.Cardinality._
import org.locationtech.geomesa.utils.stats.IndexCoverage._
import org.locationtech.geomesa.utils.stats.{Cardinality, IndexCoverage}
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.temporal.Instant

import scala.reflect.ClassTag
import scala.util.Try

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

  implicit class RichSimpleFeatureReader(val r: FeatureReader[SimpleFeatureType, SimpleFeature]) extends AnyVal {
    def getIterator: Iterator[SimpleFeature] = new Iterator[SimpleFeature] {
      override def hasNext: Boolean = r.hasNext
      override def next(): SimpleFeature = r.next()
    }
  }

  implicit def toRichSimpleFeatureIterator(iter: SimpleFeatureIterator): RichSimpleFeatureIterator = new RichSimpleFeatureIterator(iter)
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

    def get[T](i: Int) = sf.getAttribute(i).asInstanceOf[T]
    def get[T](name: String) = sf.getAttribute(name).asInstanceOf[T]

    def getDouble(str: String): Double = {
      val ret = sf.getAttribute(str)
      ret match {
        case d: java.lang.Double  => d
        case f: java.lang.Float   => f.toDouble
        case i: java.lang.Integer => i.toDouble
        case _                    => throw new Exception(s"Input $ret is not a numeric type.")
      }
    }

    def userData[T](key: AnyRef)(implicit ct: ClassTag[T]): Option[T] = {
      Option(sf.getUserData.get(key)).flatMap {
        case ct(x) => Some(x)
        case _ => None
      }
    }
  }
}




object RichIterator {
  implicit class RichIterator[T](val iter: Iterator[T]) extends AnyVal {
    def head = iter.next()
    def headOption = if (iter.hasNext) Some(iter.next()) else None
  }
}

/**
 * Contains GeoMesa specific attribute descriptor information
 */
object RichAttributeDescriptors {

  import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes._

  //noinspection AccessorLikeMethodIsEmptyParen
  implicit class RichAttributeDescriptor(val ad: AttributeDescriptor) extends AnyVal {

    def setIndexCoverage(coverage: IndexCoverage): Unit = ad.getUserData.put(OPT_INDEX, coverage.toString)

    def getIndexCoverage(): IndexCoverage =
      Option(ad.getUserData.get(OPT_INDEX).asInstanceOf[String])
          .flatMap(c => Try(IndexCoverage.withName(c)).toOption).getOrElse(IndexCoverage.NONE)

    def setIndexValue(indexValue: Boolean): Unit = ad.getUserData.put(OPT_INDEX_VALUE, indexValue.toString)

    def isIndexValue(): Boolean = Option(ad.getUserData.get(OPT_INDEX_VALUE)).exists(_ == "true")

    def setCardinality(cardinality: Cardinality): Unit =
      ad.getUserData.put(OPT_CARDINALITY, cardinality.toString)

    def getCardinality(): Cardinality =
      Option(ad.getUserData.get(OPT_CARDINALITY).asInstanceOf[String])
          .flatMap(c => Try(Cardinality.withName(c)).toOption).getOrElse(Cardinality.UNKNOWN)

    def setBinTrackId(opt: Boolean): Unit = ad.getUserData.put(OPT_BIN_TRACK_ID, opt.toString)

    def isBinTrackId: Boolean = Option(ad.getUserData.get(OPT_BIN_TRACK_ID)).exists(_ == "true")

    def setCollectionType(typ: Class[_]): Unit = ad.getUserData.put(USER_DATA_LIST_TYPE, typ)

    def getCollectionType(): Option[Class[_]] =
      Option(ad.getUserData.get(USER_DATA_LIST_TYPE)).map(_.asInstanceOf[Class[_]])

    def setMapTypes(keyType: Class[_], valueType: Class[_]): Unit = {
      ad.getUserData.put(USER_DATA_MAP_KEY_TYPE, keyType)
      ad.getUserData.put(USER_DATA_MAP_VALUE_TYPE, valueType)
    }

    def getMapTypes(): Option[(Class[_], Class[_])] = for {
      keyClass   <- Option(ad.getUserData.get(USER_DATA_MAP_KEY_TYPE))
      valueClass <- Option(ad.getUserData.get(USER_DATA_MAP_VALUE_TYPE))
    } yield {
      (keyClass.asInstanceOf[Class[_]], valueClass.asInstanceOf[Class[_]])
    }

    def isIndexed = getIndexCoverage() match {
      case IndexCoverage.FULL | IndexCoverage.JOIN => true
      case IndexCoverage.NONE => false
    }

    def isCollection = getCollectionType().isDefined

    def isMap = getMapTypes().isDefined

    def isMultiValued = isCollection || isMap
  }

  implicit class RichAttributeTypeBuilder(val builder: AttributeTypeBuilder) extends AnyVal {

    def indexCoverage(coverage: IndexCoverage) = builder.userData(OPT_INDEX, coverage.toString)

    def indexValue(indexValue: Boolean) = builder.userData(OPT_INDEX_VALUE, indexValue)

    def cardinality(cardinality: Cardinality) = builder.userData(OPT_CARDINALITY, cardinality.toString)

    def collectionType(typ: Class[_]) = builder.userData(USER_DATA_LIST_TYPE, typ)

    def mapTypes(keyType: Class[_], valueType: Class[_]) =
      builder.userData(USER_DATA_MAP_KEY_TYPE, keyType).userData(USER_DATA_MAP_VALUE_TYPE, valueType)
  }
}

object RichSimpleFeatureType {

  import RichAttributeDescriptors.RichAttributeDescriptor

  import scala.collection.JavaConversions._

  val SCHEMA_VERSION_KEY  = "geomesa.version"
  val TABLE_SHARING_KEY   = "geomesa.table.sharing"
  val SHARING_PREFIX_KEY  = "geomesa.table.sharing.prefix"
  val DEFAULT_DATE_KEY    = "geomesa.index.dtg"
  val ST_INDEX_SCHEMA_KEY = "geomesa.index.st.schema"

  // in general we store everything as strings so that it's easy to pass to accumulo iterators
  implicit class RichSimpleFeatureType(val sft: SimpleFeatureType) extends AnyVal {

    def getGeomField: String = sft.getGeometryDescriptor.getLocalName
    def getGeomIndex: Int = sft.indexOf(sft.getGeometryDescriptor.getLocalName)

    def getDtgField: Option[String] = userData[String](DEFAULT_DATE_KEY)
    def getDtgIndex: Option[Int] = getDtgField.map(sft.indexOf).filter(_ != -1)
    def getDtgDescriptor = getDtgIndex.map(sft.getDescriptor)
    def clearDtgField(): Unit = sft.getUserData.remove(DEFAULT_DATE_KEY)
    def setDtgField(dtg: String): Unit = {
      val descriptor = sft.getDescriptor(dtg)
      require(descriptor != null && descriptor.getType.getBinding == classOf[Date],
        s"Invalid date field '$dtg' for schema $sft")
      sft.getUserData.put(DEFAULT_DATE_KEY, dtg)
    }

    def getStIndexSchema: String = userData[String](ST_INDEX_SCHEMA_KEY).orNull
    def setStIndexSchema(schema: String): Unit = sft.getUserData.put(ST_INDEX_SCHEMA_KEY, schema)

    def getBinTrackId: Option[String] = sft.getAttributeDescriptors.find(_.isBinTrackId).map(_.getLocalName)

    def getSchemaVersion: Int =
      userData[String](SCHEMA_VERSION_KEY).map(_.toInt).getOrElse(CURRENT_SCHEMA_VERSION)
    def setSchemaVersion(version: Int): Unit = sft.getUserData.put(SCHEMA_VERSION_KEY, version.toString)

    def isPoints = sft.getGeometryDescriptor.getType.getBinding == classOf[Point]
    def isLines = sft.getGeometryDescriptor.getType.getBinding == classOf[LineString]

    //  If no user data is specified when creating a new SFT, we should default to 'true'.
    def isTableSharing: Boolean = userData[String](TABLE_SHARING_KEY).map(_.toBoolean).getOrElse(true)
    def setTableSharing(sharing: Boolean): Unit = sft.getUserData.put(TABLE_SHARING_KEY, sharing.toString)

    def getTableSharingPrefix: String = userData[String](SHARING_PREFIX_KEY).getOrElse("")
    def setTableSharingPrefix(prefix: String): Unit = sft.getUserData.put(SHARING_PREFIX_KEY, prefix)

    def getEnabledTables: String = userData[String](SimpleFeatureTypes.ENABLED_INDEXES).getOrElse("")
    def setEnabledTables(tables: String): Unit = sft.getUserData.put(SimpleFeatureTypes.ENABLED_INDEXES, tables)

    def userData[T](key: AnyRef): Option[T] = Option(sft.getUserData.get(key).asInstanceOf[T])
  }
}

class JodaConverterFactory extends ConverterFactory {
  private val df = ISODateTimeFormat.dateTime().withZoneUTC()
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