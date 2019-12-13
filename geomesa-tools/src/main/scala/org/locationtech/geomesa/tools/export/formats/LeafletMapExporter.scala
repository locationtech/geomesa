/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.export.formats

import java.io._
import java.nio.charset.StandardCharsets

import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.FilenameUtils
import org.geotools.geojson.feature.FeatureJSON
import org.locationtech.geomesa.tools.Command
import org.locationtech.geomesa.tools.export.ExportCommand.ExportParams
import org.locationtech.geomesa.tools.export.formats.FeatureExporter.{ByteCounter, ByteCounterExporter}
import org.locationtech.geomesa.tools.utils.Prompt
import org.locationtech.geomesa.utils.io.{PathUtils, WithClose}
import org.locationtech.jts.geom.{Coordinate, Geometry}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._
import scala.io.Source

class LeafletMapExporter(os: OutputStream, counter: ByteCounter)
    extends ByteCounterExporter(counter) with LazyLogging {

  private val json = new FeatureJSON()
  private val writer = new OutputStreamWriter(os, StandardCharsets.UTF_8)
  private val coordMap = scala.collection.mutable.Map.empty[Coordinate, Int].withDefaultValue(0)

  private var first = true
  private var featureInfo = ""

  override def start(sft: SimpleFeatureType): Unit = {
    featureInfo = LeafletMapExporter.getFeatureInfo(sft)
    writer.write(LeafletMapExporter.IndexHead)
    writer.write("""var points = {"type":"FeatureCollection","features":[""")
  }

  override def export(features: Iterator[SimpleFeature]): Option[Long] = {
    var count = 0L
    if (first && features.hasNext) {
      first = false
      write(features.next)
      count += 1L
    }
    while (features.hasNext) {
      writer.write(',')
      write(features.next)
      count += 1L
    }
    writer.flush()
    Some(count)
  }

  private def write(feature: SimpleFeature): Unit = {
    json.writeFeature(feature, writer)
    val geom = feature.getDefaultGeometry.asInstanceOf[Geometry]
    if (geom != null) {
      geom.getCoordinates.foreach(c => coordMap(c) += 1)
    }
  }

  override def close(): Unit  = {
    // Finish writing GeoJson
    writer.write("]};\n\n")
    writer.write(featureInfo)
    // Write Heatmap Data
    writer.write("var heat = L.heatLayer([\n")
    if (coordMap.nonEmpty) {
      val max = coordMap.maxBy(_._2)._2
      val iter = coordMap.iterator
      iter.take(1).foreach { case (coord, weight) =>
        writer.write(s"        [${coord.y}, ${coord.x}, ${weight / max}]")
      }
      iter.foreach { case (coord, weight) =>
        writer.write(s",\n        [${coord.y}, ${coord.x}, ${weight / max}]")
      }
    }
    writer.write("\n    ], { radius: 25 });\n\n")
    writer.write(LeafletMapExporter.IndexTail)
    writer.close()

    if (first) {
      Command.user.warn("No features were exported - the map will not render correctly")
    }
  }
}

object LeafletMapExporter {

  private lazy val Template: Array[String] = {
    WithClose(getClass.getClassLoader.getResourceAsStream("leaflet/index.html")) { is =>
      val indexArray = Source.fromInputStream(is).mkString.split("\\|codegen\\|")
      require(indexArray.length == 2, "Malformed index.html, unable to render map")
      indexArray
    }
  }

  lazy val IndexHead: String = Template.head
  lazy val IndexTail: String = Template.last

  val MaxFeatures = 10000

  /**
    * Configure parameters for a leaflet export
    *
    * @param params params
    */
  def configure(params: ExportParams): Boolean = {
    val large = "The Leaflet map may exhibit performance issues when displaying large numbers of features. For a " +
        "more robust solution, consider using GeoServer."
    if (params.maxFeatures == null) {
      Command.user.warn(large)
      Command.user.warn(s"Limiting max features to ${LeafletMapExporter.MaxFeatures}. To override, " +
          "please use --max-features")
      params.maxFeatures = LeafletMapExporter.MaxFeatures
    } else if (params.maxFeatures > LeafletMapExporter.MaxFeatures) {
      if (params.force) {
        Command.user.warn(large)
      } else if (!Prompt.confirm(s"$large Would you like to continue anyway (y/n)? ")) {
        return false
      }
    }

    if (params.gzip != null) {
      Command.user.warn("Ignoring gzip parameter for Leaflet export")
      params.gzip = null
    }

    if (params.file == null) {
      params.file = sys.props("user.dir")
    }
    if (PathUtils.isRemote(params.file)) {
      if (params.file.endsWith("/")) {
        params.file = s"${params.file}index.html"
      } else if (FilenameUtils.indexOfExtension(params.file) == -1) {
        params.file = s"${params.file}/index.html"
      }
    } else {
      val file = new File(params.file)
      val destination = if (file.isDirectory || (!file.exists && file.getName.indexOf(".") == -1)) {
        new File(file, "index.html")
      } else {
        file
      }
      params.file = destination.getAbsolutePath
    }

    true
  }

  def getFeatureInfo(sft: SimpleFeatureType): String = {
    val str = new StringBuilder()
    str.append("    function onEachFeature(feature, layer) {\n")
    if (sft == null) {
      str.append("\n    };\n\n").toString
    } else {
      val attributes = sft.getAttributeDescriptors.map(_.getLocalName). collect {
        case name if name != "geom" => s""""$name: " + feature.properties.$name"""
      }
      str.append("""        layer.bindPopup("ID: " + feature.id + "<br>" + """)
      str.append(""""GEOM: " + feature.geometry.type + "[" + feature.geometry.coordinates + "]<br>" + """)
      str.append(attributes.mkString(""" + "<br>" + """))
    }
    str.append(");\n    }\n\n")
    str.toString()
  }
}
