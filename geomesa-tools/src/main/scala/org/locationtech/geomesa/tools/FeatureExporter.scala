/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/
package org.locationtech.geomesa.tools

import java.io._
import java.text.SimpleDateFormat
import java.util.{Date, TimeZone}

import com.typesafe.scalalogging.slf4j.Logging
import org.apache.commons.lang.StringEscapeUtils
import org.geotools.GML
import org.geotools.GML.Version
import org.geotools.data.DataUtilities
import org.geotools.data.shapefile.{ShapefileDataStore, ShapefileDataStoreFactory}
import org.geotools.data.simple.{SimpleFeatureCollection, SimpleFeatureStore}
import org.geotools.geojson.feature.FeatureJSON
import org.locationtech.geomesa.filter.function._
import org.locationtech.geomesa.tools.Utils.Formats
import org.locationtech.geomesa.tools.commands.ExportCommand.ExportParameters
import org.locationtech.geomesa.utils.geotools.Conversions._
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConversions._

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

class DelimitedExport(writer: Writer, format: String, attributes: Option[String])
    extends FeatureExporter with Logging {

  val delimiter = format match {
   case Formats.CSV => ","
   case Formats.TSV => "\t"
  }

  lazy val dateFormat = {
    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    df.setTimeZone(TimeZone.getTimeZone("UTC"))
    df
  }

  val escape: String => String = format match {
    case Formats.CSV => StringEscapeUtils.escapeCsv
    case _           => (s: String) => s
  }

  override def write(features: SimpleFeatureCollection): Unit = {

    val sft = features.getSchema

    // split apart the optional attributes and then split any derived values to just get the names
    val names = attributes.map(_.split("""(?<!\\),""").toSeq.map(_.split(":")(0).split("=").head.trim))
        .getOrElse(sft.getAttributeDescriptors.map(_.getLocalName))

    val indices = names.map(sft.indexOf)

    def findMessage(i: Int) = {
      val index = indices.indexOf(i)
      s"Attribute ${names(index)} does not exist in the feature type."
    }

    indices.foreach(i => assert(i != -1, findMessage(i)))

    // write out a header line
    writer.write(names.mkString("", delimiter, "\n"))

    var count = 0
    features.features.foreach { sf =>
      val values = indices.map(i => escape(stringify(sf.getAttribute(i))))
      writer.write(values.mkString("", delimiter, "\n"))
      count += 1
      if (count % 10000 == 0) {
        logger.debug(s"wrote $count features")
      }
    }
    logger.info(s"Exported $count features")
  }

  def stringify(o: Object): String = o match {
    case null    => ""
    case d: Date => dateFormat.format(d)
    case _       => o.toString
  }

  override def flush() = writer.flush()

  override def close() = {
    writer.flush()
    writer.close()
  }

}

object DelimitedExport {
  def apply(writer: Writer, params: ExportParameters) =
    new DelimitedExport(writer, params.format.toLowerCase, Option(params.attributes))
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

  import BinaryOutputEncoder._

  val id = idAttribute.orElse(Some("id"))
  val latLon = latAttribute.flatMap(lat => lonAttribute.map(lon => (lat, lon)))

  override def write(fc: SimpleFeatureCollection) =
    encodeFeatureCollection(fc, os, dtgAttribute, id, lblAttribute, latLon, AxisOrder.LonLat)

  override def flush() = os.flush()

  override def close() = os.close()
}
