/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.tools

import java.io._
import java.util.{Date, List => jList, Map => jMap}

import com.typesafe.scalalogging.LazyLogging
import com.vividsolutions.jts.geom.Geometry
import org.apache.commons.csv.{CSVFormat, QuoteMode}
import org.geotools.GML
import org.geotools.GML.Version
import org.geotools.data.DataUtilities
import org.geotools.data.shapefile.{ShapefileDataStore, ShapefileDataStoreFactory}
import org.geotools.data.simple.{SimpleFeatureCollection, SimpleFeatureStore}
import org.geotools.geojson.feature.FeatureJSON
import org.locationtech.geomesa.features.avro.AvroDataFileWriter
import org.locationtech.geomesa.filter.function._
import org.locationtech.geomesa.tools.Utils.Formats
import org.locationtech.geomesa.tools.Utils.Formats.Formats
import org.locationtech.geomesa.tools.commands.ExportCommand.ExportParameters
import org.locationtech.geomesa.utils.geotools.Conversions._
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.ListSplitter
import org.locationtech.geomesa.utils.text.WKTUtils
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
  // JNH: "location" is unlikely to be a valid namespace.
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

  /**
   * If the attribute string has the geometry attribute in it, we will replace the name of the
   * geom descriptor with "the_geom," since that is what Shapefile expect the geom to be named.
   *
   * @param attributes
   * @param sft
   * @return
   */
  def replaceGeomInAttributesString(attributes: String, sft: SimpleFeatureType): String = {
    val trimmedAttributes = scala.collection.mutable.LinkedList(new ListSplitter().parse(attributes):_*)
    val geomDescriptor = sft.getGeometryDescriptor.getLocalName

    if (trimmedAttributes.contains(geomDescriptor)) {
      val idx = trimmedAttributes.indexOf(geomDescriptor)
      trimmedAttributes.set(idx, s"the_geom=$geomDescriptor")
    }

    trimmedAttributes.mkString(",")
  }
}

class DelimitedExport(writer: Writer, format: Formats) extends FeatureExporter with LazyLogging {

  import org.locationtech.geomesa.utils.geotools.GeoToolsDateFormat

  val printer = format match {
   case Formats.CSV => CSVFormat.DEFAULT.withQuoteMode(QuoteMode.MINIMAL).print(writer)
   case Formats.TSV => CSVFormat.TDF.withQuoteMode(QuoteMode.MINIMAL).print(writer)
  }

  override def write(features: SimpleFeatureCollection): Unit = {

    val sft = features.getSchema

    val names = sft.getAttributeDescriptors.map(_.getLocalName)
    val indices = names.map(sft.indexOf)

    val headers = indices.map(sft.getDescriptor).map(SimpleFeatureTypes.encodeDescriptor(sft, _))

    // write out a header line
    printer.print("id")
    printer.printRecord(headers: _*)

    var count = 0L
    features.features.foreach { sf =>
      printer.print(sf.getID)
      printer.printRecord(sf.getAttributes.map(stringify): _*)
      count += 1
      if (count % 10000 == 0) {
        logger.debug(s"wrote $count features")
      }
    }
    logger.info(s"Exported $count features")
  }

  def stringify(o: Any): String = o match {
    case null          => ""
    case g: Geometry   => WKTUtils.write(g)
    case d: Date       => GeoToolsDateFormat.print(d.getTime)
    case l: jList[_]   => l.map(stringify).mkString(",")
    case m: jMap[_, _] => m.map { case (k, v) => s"${stringify(k)}->${stringify(v)}"}.mkString(",")
    case _             => o.toString
  }

  override def flush() = printer.flush()

  override def close() = {
    printer.flush()
    printer.close()
  }
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

class AvroExport(os: OutputStream, sft: SimpleFeatureType, compression: Int) extends FeatureExporter {

  val writer = new AvroDataFileWriter(os, sft, compression)

  override def write(fc: SimpleFeatureCollection): Unit = writer.append(fc)

  override def flush() = {
    writer.flush()
    os.flush()
  }

  override def close() = {
    writer.close()
    os.close()
  }
}