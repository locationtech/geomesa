/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.export

import java.io.{StringReader, StringWriter}

import org.geotools.data.DataUtilities
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.data.simple.SimpleFeatureCollection
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.tools.export.formats.DelimitedExporter
import org.locationtech.geomesa.tools.ingest.AutoIngestDelimited
import org.locationtech.geomesa.tools.utils.DataFormats
import org.locationtech.geomesa.tools.utils.DataFormats._
import org.locationtech.geomesa.utils.geotools.{GeoToolsDateFormat, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.text.WKTUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class DelimitedExportImportTest extends Specification {

  val dt1 = GeoToolsDateFormat.parseDateTime("2016-01-01T00:00:00.000Z").toDate
  val dt2 = GeoToolsDateFormat.parseDateTime("2016-01-02T00:00:00.000Z").toDate
  val pt1 = WKTUtils.read("POINT(1 0)")
  val pt2 = WKTUtils.read("POINT(0 2)")

  def export(features: SimpleFeatureCollection, format: DataFormat): String = {
    val writer = new StringWriter()
    val export = new DelimitedExporter(writer, format, None, true)
    export.export(features)
    export.close()
    writer.toString
  }

  "Delimited export import" should {

    "export and import simple schemas" >> {

      val sft = SimpleFeatureTypes.createType("tools", "name:String,dtg:Date,*geom:Point:srid=4326")
      val features = List(
        new ScalaSimpleFeature("1", sft, Array("name1", dt1, pt1)),
        new ScalaSimpleFeature("2", sft, Array("name2", dt2, pt2))
      )
      val fc = new ListFeatureCollection(sft, features)

      "in tsv" >> {
        val format = DataFormats.Tsv
        val results = export(fc, format)

        val reader = AutoIngestDelimited.getCsvFormat(format).parse(new StringReader(results))

        try {
          val (newSft, newFeatures) = AutoIngestDelimited.createSimpleFeatures("tools", reader.iterator())
          SimpleFeatureTypes.encodeType(newSft) mustEqual SimpleFeatureTypes.encodeType(sft)
          newFeatures.map(DataUtilities.encodeFeature).toList mustEqual features.map(DataUtilities.encodeFeature)
        } finally {
          reader.close()
        }
      }

      "in csv" >> {
        val format = DataFormats.Csv
        val results = export(fc, format)

        val reader = AutoIngestDelimited.getCsvFormat(format).parse(new StringReader(results))

        try {
          val (newSft, newFeatures) = AutoIngestDelimited.createSimpleFeatures("tools", reader.iterator())
          SimpleFeatureTypes.encodeType(newSft) mustEqual SimpleFeatureTypes.encodeType(sft)
          newFeatures.map(DataUtilities.encodeFeature).toList mustEqual features.map(DataUtilities.encodeFeature)
        } finally {
          reader.close()
        }
      }
    }

    "export and import lists and maps" >> {

      val sft = SimpleFeatureTypes.createType("tools",
        "name:String,fingers:List[Int],toes:Map[String,Int],dtg:Date,*geom:Point:srid=4326")
      val features = List(
        new ScalaSimpleFeature("1", sft, Array("name1", List(1, 2).asJava, Map("one" -> 1, "1" -> 0).asJava, dt1, pt1)),
        new ScalaSimpleFeature("2", sft, Array("name2", List(2, 1).asJava, Map("two" -> 2, "2" -> 0).asJava, dt1, pt1))
      )
      val fc = new ListFeatureCollection(sft, features)

      "in tsv" >> {
        val format = DataFormats.Tsv
        val results = export(fc, format)

        val reader = AutoIngestDelimited.getCsvFormat(format).parse(new StringReader(results))

        val (newSft, newFeatures) = try {
          val (newSft, newFeatures) = AutoIngestDelimited.createSimpleFeatures("tools", reader.iterator())
          (newSft, newFeatures.toList)
        } finally {
          reader.close()
        }

        SimpleFeatureTypes.encodeType(newSft) mustEqual SimpleFeatureTypes.encodeType(sft)
        forall(0 until sft.getAttributeCount) { i =>
          newFeatures.map(_.getAttribute(i)) mustEqual features.map(_.getAttribute(i))
        }
      }

      "in csv" >> {
        val format = DataFormats.Csv
        val results = export(fc, format)

        val reader = AutoIngestDelimited.getCsvFormat(format).parse(new StringReader(results))

        val (newSft, newFeatures) = try {
          val (newSft, newFeatures) = AutoIngestDelimited.createSimpleFeatures("tools", reader.iterator())
          (newSft, newFeatures.toList)
        } finally {
          reader.close()
        }

        SimpleFeatureTypes.encodeType(newSft) mustEqual SimpleFeatureTypes.encodeType(sft)
        forall(0 until sft.getAttributeCount) { i =>
          newFeatures.map(_.getAttribute(i)) mustEqual features.map(_.getAttribute(i))
        }
      }
    }
  }
}
