/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.export.formats

import java.io._

import com.typesafe.scalalogging.LazyLogging
import com.vividsolutions.jts.geom.{Coordinate, Geometry}
import org.geotools.geojson.feature.FeatureJSON
import org.locationtech.geomesa.tools.Command.user
import org.locationtech.geomesa.tools.export.formats.LeafletMapExporter.{SimpleCoordinate, _}
import org.locationtech.geomesa.tools.export.{ExportCommand, FileExportParams}
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.io.Source
import scala.io.StdIn.readLine

class LeafletMapExporter(params: FileExportParams) extends FeatureExporter with LazyLogging {

  private val json = new FeatureJSON()
  private val coordMap = mutable.Map[SimpleCoordinate[Double], Int]()

  private var first = true
  private var sft: SimpleFeatureType = _
  private var totalCount: Long = 0L

  Option(params.maxFeatures) match {
    case Some(limit) =>
      if (limit > 10000) {
        // Note we have to ask if the user wants to continue like this or the output will be
        // hidden since this command runs in a sub-shell
        user.warn("A large number of features might be exported. This can cause performance issues when using the map. For large numbers of features it is recommended to use GeoServer to render a map.\nDo you want to continue? [y/N]")
        // readLine() could be string, null or empty string, handle all
        val response = Option(readLine()).getOrElse("").headOption.getOrElse("n").toString
        if (response.matches("^[nN]")) {
          throw new RuntimeException("User requested program termination.")
        }
      }
    case None => params.maxFeatures = 10000
  }

  if (params.noHeader) { user.warn("NoHeader parameter cannot be used with leaflet format, ignoring.") }
  if (params.gzip != null) { user.warn("GZip parameter cannot be used with leaflet format, ignoring.") }

  val GEOMESA_HOME = SystemProperty("geomesa.home", "/tmp")
  val indexFile: File = getDestination(Option(params.file).getOrElse{
    new File(new File(GEOMESA_HOME.get), "leaflet")
  })
  val indexWriter: Writer = ExportCommand.getWriter(indexFile, null)

  val (indexHead, indexTail): (String, String) = {
    val indexStream: InputStream = getClass.getClassLoader.getResourceAsStream("leaflet/index.html")
    try {
      val indexString: String = Source.fromInputStream(indexStream).mkString
      val indexArray: Array[String] = indexString.split("\\|codegen\\|")
      require(indexArray.length == 2, "Malformed index.html unable to render map.")
      (indexArray(0), indexArray(1))
    } finally {
      indexStream.close()
    }
  }

  override def start(sft: SimpleFeatureType): Unit = {
    this.sft = sft
    indexWriter.write(indexHead)
    indexWriter.write("""var points = {"type":"FeatureCollection","features":[""")
  }

  override def export(features: Iterator[SimpleFeature]): Option[Long] = {
    var count = 0L
    features.foreach { feature =>
      if (first) {
        first = false
      } else {
        indexWriter.write(",")
      }
      json.writeFeature(feature, indexWriter)
      storeFeature(feature, coordMap)
      count += 1L
    }
    indexWriter.flush()
    totalCount += count
    Some(count)
  }

  override def close(): Unit  = {
    // Finish writing GeoJson
    indexWriter.write("]};\n\n")
    indexWriter.write(getFeatureInfo(sft))
    // Write Heatmap Data
    val values = normalizeValues(coordMap)
    first = true
    indexWriter.write("""var heat = L.heatLayer([""" + "\n")
    values.foreach{ c =>
      if (first) {
        first = false
      } else {
        indexWriter.write(",\n")
      }
      indexWriter.write(c._1.render(c._2))
    }
    indexWriter.write("\n    ], {radius: 25});\n\n")
    indexWriter.write(indexTail)
    indexWriter.close()

    if (totalCount < 1) {
      user.warn("No features were exported. This will cause the map to fail to render correctly.")
    }
    user.info(s"Successfully wrote Leaflet html to: ${indexFile.getAbsolutePath}")
  }
}

object LeafletMapExporter {
  def getDestination(file: File): File = {
    implicit class WritableFile(file: File) {
      def ensureWritable: File = {
        if (file.canWrite) { file }
        else { throw new SecurityException(s"Unable to create output destination ${file.toString}, check permissions.") }
      }
    }

    // Handle both files and directories that could exist or not
    if (file.exists()) {
      if (file.isDirectory) {
        new File(file, "index.html").ensureWritable
      } else {
        if (file.toString.endsWith(".html")) { file.ensureWritable }
        else { throw new RuntimeException(s"Destination file ${file.toString} must end with '.html'") }
      }
    } else {
      file.mkdirs()
      new File(file, "index.html").ensureWritable
    }
  }

  def getFeatureInfo(sft: SimpleFeatureType): String = {
    val str: StringBuilder = new StringBuilder()
    str.append("    function onEachFeature(feature, layer) {\n")
    Option(sft) match {
      case None    => str.append("\n    };\n\n").toString
      case Some(_) =>
        str.append("""        layer.bindPopup("ID: " + feature.id + "<br>" + """)
        str.append(""""GEOM: " + feature.geometry.type + "[" + feature.geometry.coordinates + "]<br>" + """)
        str.append(sft.getAttributeDescriptors.filter(_.getLocalName != "geom") .map(attr =>
          s""""${attr.getLocalName}: " + feature.properties.${attr.getLocalName}"""
        ).mkString(""" + "<br>" + """))

    }
    str.append(");\n    }\n\n")
    str.toString()
  }

  def storeFeature(feature: SimpleFeature, coordMap: mutable.Map[SimpleCoordinate[Double], Int]): Unit = {
    val coords: Array[Coordinate] = feature.getDefaultGeometry match {
      case geom: Geometry => geom.getCoordinates
      case _ => Array[Coordinate]()
    }
    coords.map(c => SimpleCoordinate(c.x, c.y)).foreach{ sc =>
      coordMap.put(sc, 1) match {
        case Some(count) => coordMap.put(sc, count + 1)
        case None =>
      }
    }
  }

  def normalizeValues(coordMap: mutable.Map[SimpleCoordinate[Double], Int]): Map[SimpleCoordinate[Double], Float] = {
    if (coordMap.nonEmpty) {
      val max: Float = coordMap.maxBy(_._2)._2
      coordMap.map(c => (c._1, c._2 / max)).toMap
    } else Map[SimpleCoordinate[Double], Float]()
  }

  case class SimpleCoordinate[@specialized(Double) T](x: T, y: T) {
    def render(weight: Float): String = {
      s"""        [${y.toString}, ${x.toString}, ${weight.toString}]"""
    }
  }
}
