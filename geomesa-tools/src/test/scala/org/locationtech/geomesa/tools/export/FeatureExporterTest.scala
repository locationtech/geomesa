/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.export

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, StringWriter}
import java.util.Date
import java.util.zip.Deflater

import org.geotools.data.DataUtilities
import org.geotools.factory.Hints
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeatureFactory
import org.locationtech.geomesa.features.avro.AvroDataFileReader
import org.locationtech.geomesa.tools.export.formats.{AvroExporter, DelimitedExporter, ShapefileExporter}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.feature.simple.SimpleFeature
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class FeatureExporterTest extends Specification {

  sequential

  def createFeatures(sftName: String, numFeatures: Int = 1): Seq[SimpleFeature] = {
    val sft = SimpleFeatureTypes.createType(sftName, "name:String,geom:Point:srid=4326,dtg:Date")

    val attributes = Array("myname", "POINT(45.0 49.0)", new Date(0))

    Seq.tabulate(numFeatures) { i =>
      val feature = ScalaSimpleFeatureFactory.buildFeature(sft, attributes, s"fid-$i")
      feature.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
      feature
    }
  }

  "DelimitedExport" should {
    "properly export to CSV" >> {
      val features = createFeatures("DelimitedExportTest")
      val writer = new StringWriter()
      val export = DelimitedExporter.csv(writer, withHeader = true, includeIds = true)
      export.start(features.head.getFeatureType)
      export.export(features.iterator)
      export.close()

      val result = writer.toString.split("\r\n")
      result must haveLength(2)
      val (header, data) = (result(0), result(1))

      header mustEqual "id,name:String,*geom:Point:srid=4326,dtg:Date"
      data mustEqual "fid-0,myname,POINT (45 49),1970-01-01T00:00:00.000Z"
    }

    "properly export to CSV with options" >> {
      // simulate a projecting read
      val sft = SimpleFeatureTypes.createType("DelimitedExportTest", "name:String,dtg:Date")
      val features = createFeatures("DelimitedExportTest").map(DataUtilities.reType(sft, _))
      val writer = new StringWriter()
      val export = DelimitedExporter.csv(writer, withHeader = false, includeIds = false)
      export.start(sft)
      export.export(features.iterator)
      export.close()

      val result = writer.toString.split("\r\n")
      result must haveLength(1)
      result(0) mustEqual "myname,1970-01-01T00:00:00.000Z"
    }
  }

  "Shapefile Export" should {

    val sft = SimpleFeatureTypes.createType("Shp", "name:String,geom:Point:srid=4326,dtg:Date")

    "transform 'geom' to 'the_geom' when asking for just 'geom'" >> {
      ShapefileExporter.replaceGeom(sft, Seq("geom")) mustEqual Seq("the_geom=geom")
    }

    "transform 'geom' in the attributes string when another attribute follows" >> {
      ShapefileExporter.replaceGeom(sft, Seq("geom", "name")) mustEqual Seq("the_geom=geom","name")
    }

    "transform 'geom' in the attributes string when it follows another attribute" >> {
      ShapefileExporter.replaceGeom(sft, Seq("name", "geom")) mustEqual Seq("name","the_geom=geom")
    }

    "transform 'geom' in the attributes string when it is between two attributes" >> {
      ShapefileExporter.replaceGeom(sft, Seq("name", "geom", "dtg")) mustEqual Seq("name","the_geom=geom","dtg")
    }

    "NOT transform 'the_geom' in the attributes string" >> {
      ShapefileExporter.replaceGeom(sft, Seq("the_geom")) mustEqual Seq("the_geom")
    }

    "NOT transform an incorrect transform in the query" >> {
      ShapefileExporter.replaceGeom(sft, Seq("name", "geom=the_geom", "dtg")) mustEqual
          Seq("name", "geom=the_geom", "dtg", "the_geom=geom")
    }
  }

  "Avro Export" should {

    "properly export to avro" >> {
      val features = createFeatures("AvroExportTest", 10)
      val os = new ByteArrayOutputStream()
      val export = new AvroExporter(Deflater.NO_COMPRESSION, os)
      export.start(features.head.getFeatureType)
      export.export(features.iterator)
      export.close()

      val result = new AvroDataFileReader(new ByteArrayInputStream(os.toByteArray))
      SimpleFeatureTypes.encodeType(result.getSft) mustEqual SimpleFeatureTypes.encodeType(features.head.getFeatureType)

      val exported = result.toList
      exported must haveLength(10)
      exported.map(_.getID) must containTheSameElementsAs(Seq.tabulate(10)("fid-" + _))
      forall(exported) { feature =>
        feature.getAttribute(0) mustEqual "myname"
        feature.getAttribute(1) mustEqual WKTUtils.read("POINT(45 49)")
        feature.getAttribute(2) mustEqual new Date(0)
      }
    }

    "compress output" >> {
      val features = createFeatures("AvroExportTest", 10)

      val uncompressed :: compressed :: Nil = List(Deflater.NO_COMPRESSION, Deflater.DEFAULT_COMPRESSION).map { c =>
        val os = new ByteArrayOutputStream()
        val export = new AvroExporter(c, os)
        export.start(features.head.getFeatureType)
        export.export(features.iterator)
        export.close()
        os.toByteArray
      }

      forall(Seq(uncompressed, compressed)) { bytes =>
        val result = new AvroDataFileReader(new ByteArrayInputStream(bytes))
        SimpleFeatureTypes.encodeType(result.getSft) mustEqual SimpleFeatureTypes.encodeType(features.head.getFeatureType)

        val exported = result.toList
        exported must haveLength(10)
        exported.map(_.getID) must containTheSameElementsAs(Seq.tabulate(10)("fid-" + _))
        forall(exported) { feature =>
          feature.getAttribute(0) mustEqual "myname"
          feature.getAttribute(1) mustEqual WKTUtils.read("POINT(45 49)")
          feature.getAttribute(2) mustEqual new Date(0)
        }
      }

      compressed.length must beLessThan(uncompressed.length)
    }
  }
}
