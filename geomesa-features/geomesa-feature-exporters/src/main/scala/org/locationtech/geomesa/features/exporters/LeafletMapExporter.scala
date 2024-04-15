/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.exporters

import com.google.gson.stream.JsonWriter
import com.typesafe.scalalogging.LazyLogging
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.locationtech.geomesa.features.serialization.GeoJsonSerializer
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.jts.geom.{Coordinate, Geometry}

import java.io.{OutputStream, OutputStreamWriter}
import java.nio.charset.StandardCharsets
import scala.io.Source

class LeafletMapExporter(out: OutputStream) extends FeatureExporter with LazyLogging {

  private val coordMap = scala.collection.mutable.Map.empty[Coordinate, Int].withDefaultValue(0)

  private var writer: OutputStreamWriter = _
  private var jsonWriter: JsonWriter = _
  private var jsonSerializer: GeoJsonSerializer = _

  private var featureInfo = ""

  override def start(sft: SimpleFeatureType): Unit = {
    featureInfo = LeafletMapExporter.getFeatureInfo(sft)
    writer = new OutputStreamWriter(out, StandardCharsets.UTF_8)
    jsonWriter = GeoJsonSerializer.writer(writer)
    writer.write(LeafletMapExporter.IndexHead)
    writer.write("var points = ")
    writer.flush()
    jsonSerializer = new GeoJsonSerializer(sft)
    jsonSerializer.startFeatureCollection(jsonWriter)
  }

  override def export(features: Iterator[SimpleFeature]): Option[Long] = {
    var count = 0L
    while (features.hasNext) {
      val feature = features.next
      jsonSerializer.write(jsonWriter, feature)
      val geom = feature.getDefaultGeometry.asInstanceOf[Geometry]
      if (geom != null) {
        geom.getCoordinates.foreach(c => coordMap(c) += 1)
      }
      count += 1L
    }
    jsonWriter.flush()
    Some(count)
  }

  override def close(): Unit  = {
    // finish writing GeoJson
    if (writer != null) {
      try {
        jsonSerializer.endFeatureCollection(jsonWriter)
        jsonWriter.flush()

        writer.write(";\n\n")
        writer.write(featureInfo)
        // Write Heatmap Data
        writer.write("var heat = L.heatLayer([\n")
        if (coordMap.isEmpty) {
          logger.warn("No features were exported - the map will not render correctly")
        } else {
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
      } finally {
        jsonWriter.close() // also closes writer and output stream
      }
    }
  }
}

object LeafletMapExporter {

  import scala.collection.JavaConverters._

  val MaxFeatures = 10000

  private lazy val Template: Array[String] = {
    WithClose(getClass.getClassLoader.getResourceAsStream("leaflet/index.html")) { is =>
      val indexArray = Source.fromInputStream(is).mkString.split("\\|codegen\\|")
      require(indexArray.length == 2, "Malformed index.html, unable to render map")
      indexArray
    }
  }

  private lazy val IndexHead: String = Template.head
  private lazy val IndexTail: String = Template.last

  private def getFeatureInfo(sft: SimpleFeatureType): String = {
    val str = new StringBuilder()
    str.append("    function onEachFeature(feature, layer) {\n")
    if (sft == null) {
      str.append("\n    };\n\n").toString
    } else {
      val attributes = sft.getAttributeDescriptors.asScala.map(_.getLocalName). collect {
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
