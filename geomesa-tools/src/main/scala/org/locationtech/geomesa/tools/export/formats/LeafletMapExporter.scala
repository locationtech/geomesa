/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.export.formats

import java.io._

import com.beust.jcommander.ParameterException
import com.typesafe.scalalogging.LazyLogging
import org.geotools.geojson.feature.FeatureJSON
import org.locationtech.geomesa.tools.Command.user
import org.locationtech.geomesa.tools.export.ExportCommand
import org.locationtech.geomesa.tools.export.ExportCommand.ExportParams
import org.locationtech.geomesa.tools.export.formats.LeafletMapExporter.SimpleCoordinate
import org.locationtech.geomesa.tools.utils.Prompt
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.jts.geom.Geometry
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._
import scala.io.Source

class LeafletMapExporter(params: ExportParams) extends FeatureExporter with LazyLogging {

  Option(params.maxFeatures) match {
    case Some(limit) =>
      if (limit > LeafletMapExporter.MaxFeatures) {
        val warn = "The Leaflet map may exhibit performance issues displaying large numbers of features. " +
            "Instead, consider using GeoServer for map rendering. Would you like to continue anyway (y/n)? "
        if (!Prompt.confirm(warn)) {
          throw new ParameterException("Terminating execution")
        }
      }

    case None =>
      user.debug(s"Limiting max features to ${LeafletMapExporter.MaxFeatures}")
      params.maxFeatures = LeafletMapExporter.MaxFeatures
  }

  if (params.gzip != null) {
    user.warn("Ignoring gzip parameter for Leaflet export")
  }

  LeafletMapExporter.setDestination(params)

  private val json = new FeatureJSON()
  private val coordMap = scala.collection.mutable.Map.empty[SimpleCoordinate, Int]

  private var first = true
  private var featureInfo = ""
  private var totalCount = 0L

  private val indexWriter = ExportCommand.createWriter(params)

  override def start(sft: SimpleFeatureType): Unit = {
    featureInfo = LeafletMapExporter.getFeatureInfo(sft)
    indexWriter.write(LeafletMapExporter.IndexHead)
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
      LeafletMapExporter.storeFeature(feature, coordMap)
      count += 1L
    }
    indexWriter.flush()
    totalCount += count
    Some(count)
  }

  override def close(): Unit  = {
    // Finish writing GeoJson
    indexWriter.write("]};\n\n")
    indexWriter.write(featureInfo)
    // Write Heatmap Data
    val values = LeafletMapExporter.normalizeValues(coordMap)
    first = true
    indexWriter.write("""var heat = L.heatLayer([""" + "\n")
    values.foreach { case (coord, weight) =>
      if (first) {
        first = false
      } else {
        indexWriter.write(",\n")
      }
      indexWriter.write(s"""        [${coord.y}, ${coord.x}, $weight]""")
    }
    indexWriter.write("\n    ], {radius: 25});\n\n")
    indexWriter.write(LeafletMapExporter.IndexTail)
    indexWriter.close()

    if (totalCount < 1) {
      user.warn("No features were exported - the map will not render correctly")
    }
    user.info(s"Leaflet html exported to: ${new File(params.file).getAbsolutePath}")
  }
}

object LeafletMapExporter {

  lazy private val Template: Array[String] = {
    WithClose(getClass.getClassLoader.getResourceAsStream("leaflet/index.html")) { is =>
      val indexArray = Source.fromInputStream(is).mkString.split("\\|codegen\\|")
      require(indexArray.length == 2, "Malformed index.html, unable to render map")
      indexArray
    }
  }

  lazy val IndexHead: String = Template.head
  lazy val IndexTail: String  = Template.last

  val MaxFeatures = 10000

  /**
    * Sets the 'file' in params to an output html file. Handles both files and directories that could exist or not
    *
    * @param params parameters
    * @return
    */
  def setDestination(params: ExportParams): Unit = {
    val file = new File(Option(params.file).getOrElse(sys.props("user.dir")))
    val destination = if (file.isDirectory || (!file.exists && file.getName.indexOf(".") == -1)) {
      new File(file, "index.html")
    } else {
      file
    }
    if (!destination.exists()) {
      // this will throw an exception if we don't have permissions
      Option(destination.getParentFile).foreach(_.mkdirs())
      destination.createNewFile()
    }
    params.file = destination.getAbsolutePath
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

  def storeFeature(feature: SimpleFeature, coords: scala.collection.mutable.Map[SimpleCoordinate, Int]): Unit = {
    val geom = feature.getDefaultGeometry.asInstanceOf[Geometry]
    if (geom != null) {
      geom.getCoordinates.foreach { c =>
        val simple = SimpleCoordinate(c.x, c.y)
        coords.put(simple, 1).foreach(count => coords.put(simple, count + 1))
      }
    }
  }

  def normalizeValues(coords: scala.collection.mutable.Map[SimpleCoordinate, Int]): Map[SimpleCoordinate, Float] = {
    if (coords.nonEmpty) {
      val max: Float = coords.maxBy(_._2)._2
      coords.map(c => (c._1, c._2 / max)).toMap
    } else {
      Map.empty[SimpleCoordinate, Float]
    }
  }

  case class SimpleCoordinate(x: Double, y: Double)
}
