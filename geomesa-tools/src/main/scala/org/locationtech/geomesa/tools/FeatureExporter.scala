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
package org.locationtech.geomesa.tools

import java.io._
import java.text.SimpleDateFormat
import java.util.{Date, TimeZone}

import com.typesafe.scalalogging.slf4j.Logging
import com.vividsolutions.jts.geom.{Coordinate, Geometry}
import org.apache.commons.lang.StringEscapeUtils
import org.geotools.GML
import org.geotools.GML.Version
import org.geotools.data.DataUtilities
import org.geotools.data.shapefile.{ShapefileDataStore, ShapefileDataStoreFactory}
import org.geotools.data.simple.{SimpleFeatureCollection, SimpleFeatureStore}
import org.geotools.geojson.feature.FeatureJSON
import org.geotools.geometry.jts.JTSFactoryFinder
import org.locationtech.geomesa.filter.function._
import org.locationtech.geomesa.tools.Utils.Formats
import org.locationtech.geomesa.tools.commands.ExportCommand.ExportParameters
import org.locationtech.geomesa.utils.geotools.Conversions._
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.util.Try

trait FeatureExporter extends AutoCloseable with Flushable {
  def write(featureCollection: SimpleFeatureCollection): Unit
}

class GeoJsonExport(writer: Writer) extends FeatureExporter {

  val featureJson = new FeatureJSON()

  override def write(features: SimpleFeatureCollection) =
    featureJson.writeFeatureCollection(features, writer)

  override def flush() = writer.flush()
  override def close() = {
    flush()
    writer.close()
  }
}

class GmlExport(os: OutputStream) extends FeatureExporter {

  val encode = new GML(Version.WFS1_0)
  encode.setNamespace("location", "location.xsd")

  override def write(features: SimpleFeatureCollection) = encode.encode(os, features)

  override def flush() = os.flush()
  override def close() = {
    os.flush()
    os.close()
  }
}

class ShapefileExport(file: File) extends FeatureExporter {

  override def write(features: SimpleFeatureCollection) = {
    val url = DataUtilities.fileToURL(file)
    val factory = new ShapefileDataStoreFactory()
    val newShapeFile = factory.createDataStore(url).asInstanceOf[ShapefileDataStore]

    newShapeFile.createSchema(features.getSchema)
    val store = newShapeFile.getFeatureSource.asInstanceOf[SimpleFeatureStore]
    store.addFeatures(features)
  }

  override def flush() = {}
  override def close() = {}

}

object ShapefileExport {

  def modifySchema(sft: SimpleFeatureType): String = {
    // When exporting to Shapefile, we must rename the Geometry Attribute Descriptor to "the_geom", per
    // the requirements of Geotools' ShapefileDataStore and ShapefileFeatureWriter. The easiest way to do this
    // is transform the attribute when retrieving the SimpleFeatureCollection.
    val attrs = sft.getAttributeDescriptors.map(_.getLocalName)
    val geomDescriptor = sft.getGeometryDescriptor.getLocalName
    val modifiedAttrs =
      if (attrs.contains(geomDescriptor)) {
        attrs.updated(attrs.indexOf(geomDescriptor), s"the_geom=$geomDescriptor")
      } else {
       attrs ++ List(s"the_geom=$geomDescriptor")
      }

    modifiedAttrs.mkString(",")
  }
}

class DelimitedExport(writer: Writer,
                      format: String,
                      attributes: Option[String],
                      idAttribute: Option[String],
                      latAttribute: Option[String],
                      lonAttribute: Option[String],
                      dtgAttribute: Option[String]) extends FeatureExporter with Logging {

  val delimiter = format match {
   case Formats.CSV => ","
   case Formats.TSV => "\t"
  }

  lazy val geometryFactory = JTSFactoryFinder.getGeometryFactory
  lazy val dateFormat = {
    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    df.setTimeZone(TimeZone.getTimeZone("UTC"))
    df
  }

  override def write(features: SimpleFeatureCollection) = {

    val attrArr   = attributes.map(_.split("""(?<!\\),""")).getOrElse(Array.empty[String])
    val idAttrArr = List(idAttribute).flatten

    val attributeTypes = idAttrArr ++ attrArr
    val idField = attributeTypes.map(_.split(":")(0).split("=").head.trim).headOption

    val attrNames =
      if (attributeTypes.nonEmpty) {
        attributeTypes.map(_.split(":")(0).split("=").head.trim)
      } else {
        features.getSchema.getAttributeDescriptors.map(_.getLocalName)
      }

    // write out a header line
    writer.write(attrNames.mkString(delimiter))
    writer.write("\n")

    var count = 0
    features.features.foreach { sf =>
      writeFeature(sf, writer, attrNames, idField)
      count += 1
      if (count % 10000 == 0) logger.debug("wrote {} features", "" + count)
    }
    logger.info(s"Exported $count features")
  }

  val getGeom: SimpleFeature => Option[Geometry] =
    (sf: SimpleFeature) =>
      (latAttribute, lonAttribute) match {
        case (Some(a), Some(b)) =>
          val lat = sf.getAttribute(latAttribute.get).toString.toDouble
          val lon = sf.getAttribute(lonAttribute.get).toString.toDouble
          Some(geometryFactory.createPoint(new Coordinate(lon, lat)))
        case (Some(lat), _) =>
          logger.warn("Lattitude attribute provided by no longitude attribute provided...ignoring attribute overrides")
          None
        case ( _, Some(lon)) =>
          logger.warn("Longitude attribute provided by no lattitude attribute provided...ignoring attribute overrides")
          None
        case (_, _) => None
      }

  val getDate: SimpleFeature => Option[Date] =
    (sf: SimpleFeature) =>
      dtgAttribute.map { attr =>
        val date = sf.getAttribute(attr)
        if (date.isInstanceOf[Date]) {
          date.asInstanceOf[Date]
        } else {
          dateFormat.parse(date.toString)
        }
      }

  def writeFeature(sf: SimpleFeature, writer: Writer, attrNames: Seq[String], idField: Option[String]) = {
    val attrMap = mutable.Map.empty[String, Object]

    // copy attributes into map where we can manipulate them
    attrNames.foreach(a => Try(attrMap.put(a, sf.getAttribute(a))))

    // check that ID is set in the map
    if (idField.nonEmpty && attrMap.getOrElse(idField.get, "").toString.isEmpty) attrMap.put(idField.get, sf.getID)

    // update geom and date as needed
    getGeom(sf).foreach { geom => attrMap("*geom") = geom }
    getDate(sf).foreach { date => attrMap("dtg") = date }

    // toString, escape, and join with delimiter
    val escapedStrings = attrNames.map { n => escape(stringify(attrMap.getOrElse(n, null))) }
    var first = true
    for (s <- escapedStrings) {
      if (first) {
        writer.write(s)
        first = false
      } else {
        writer.write(delimiter)
        writer.write(s)
      }
    }
    writer.write("\n")
  }

  def stringify(o: Object): String = o match {
    case null                      => ""
    case d if d.isInstanceOf[Date] => dateFormat.format(o.asInstanceOf[java.util.Date])
    case _                         => o.toString
  }

  val escape: String => String = format match {
    case Formats.CSV => StringEscapeUtils.escapeCsv
    case _           => (s: String) => s
  }

  override def flush() = writer.flush()

  override def close() = {
    writer.flush()
    writer.close()
  }

}

object DelimitedExport {
  def apply(writer: Writer, params: ExportParameters) =
    new DelimitedExport(writer,
      params.format.toLowerCase,
      Option(params.attributes),
      Option(params.idAttribute),
      Option(params.latAttribute),
      Option(params.lonAttribute),
      Option(params.dateAttribute))
}

object BinFileExport {

  var DEFAULT_TIME = "dtg"

  def getAttributeList(p: ExportParameters): String = {
    val dtg = Option(p.dateAttribute).getOrElse(DEFAULT_TIME)
    Seq(p.latAttribute, p.lonAttribute, p.idAttribute, dtg, p.labelAttribute)
        .filter(_ != null)
        .mkString(",")
  }

  def apply(os: OutputStream, params: ExportParameters) =
    new BinFileExport(os,
                      Option(params.dateAttribute).getOrElse(DEFAULT_TIME),
                      Option(params.idAttribute),
                      Option(params.latAttribute),
                      Option(params.lonAttribute),
                      Option(params.labelAttribute))
}

class BinFileExport(os: OutputStream,
                    dtgAttribute: String,
                    idAttribute: Option[String],
                    latAttribute: Option[String],
                    lonAttribute: Option[String],
                    lblAttribute: Option[String]) extends FeatureExporter {

  import org.locationtech.geomesa.filter.function.BinaryOutputEncoder._

  val id = idAttribute.orElse(Some("id"))
  val latLon = latAttribute.flatMap(lat => lonAttribute.map(lon => (lat, lon)))

  override def write(fc: SimpleFeatureCollection) =
    encodeFeatureCollection(fc, os, dtgAttribute, id, lblAttribute, latLon, AxisOrder.LonLat, false)

  override def flush() = os.flush()

  override def close() = os.close()
}
