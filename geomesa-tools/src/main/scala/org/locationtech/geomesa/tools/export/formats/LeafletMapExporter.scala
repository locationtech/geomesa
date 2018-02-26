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
import org.locationtech.geomesa.tools.export.ExportCommand
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.io.Source

class LeafletMapExporter(indexFile: File) extends FeatureExporter with LazyLogging {

  private val json = new FeatureJSON()
  private val coordMap = mutable.Map[SimpleCoordinate[Double], Int]()

  private var first = true
  private var sft: SimpleFeatureType = _

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

  val indexWriter: Writer = ExportCommand.getWriter(indexFile, null)

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
      storeFeature(feature)
      count += 1L
    }
    indexWriter.flush()
    Some(count)
  }

  override def close(): Unit  = {
    // Finish writing GeoJson
    indexWriter.write("]};\n\n")
    indexWriter.write(getFeatureInfo)
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
  }

  private def getFeatureInfo: String = {
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

  def storeFeature(feature: SimpleFeature): Unit = {
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
    val max: Float = coordMap.maxBy(_._2)._2
    coordMap.map(c => (c._1, c._2 / max)).toMap
  }

  case class SimpleCoordinate[@specialized(Double) T](x: T, y: T) {
    def render(weight: Float): String = {
      s"""        [${y.toString}, ${x.toString}, ${weight.toString}]"""
    }
  }
}
