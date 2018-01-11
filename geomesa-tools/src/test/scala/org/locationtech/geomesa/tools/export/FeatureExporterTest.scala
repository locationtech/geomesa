/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.export

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, StringWriter}
import java.util.Date
import java.util.zip.Deflater

import org.geotools.data.Query
import org.geotools.factory.Hints
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeatureFactory
import org.locationtech.geomesa.features.avro.AvroDataFileReader
import org.locationtech.geomesa.tools.export.ExportCommand.ExportAttributes
import org.locationtech.geomesa.tools.export.formats.{AvroExporter, DelimitedExporter, ShapefileExporter}
import org.locationtech.geomesa.tools.utils.DataFormats
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class FeatureExporterTest extends Specification {

  sequential

  def getSftAndFeatures(sftName: String, numFeatures: Int = 1): (SimpleFeatureType, Seq[SimpleFeature]) = {
    val sft = SimpleFeatureTypes.createType(sftName, "name:String,geom:Point:srid=4326,dtg:Date")

    val attributes = Array("myname", "POINT(45.0 49.0)", new Date(0))

    val features = (1 to numFeatures).map { i =>
      val feature = ScalaSimpleFeatureFactory.buildFeature(sft, attributes, s"fid-$i")
      feature.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
      feature
    }
    (sft, features)
  }

  "DelimitedExport" >> {
    val sftName = "DelimitedExportTest"
    val (sft, features) = getSftAndFeatures(sftName)

    "should properly export to CSV" >> {
      val query = new Query(sftName, Filter.INCLUDE)
      val writer = new StringWriter()
      val export = new DelimitedExporter(writer, DataFormats.Csv, None, true)
      export.start(sft)
      export.export(features.iterator)
      export.close()

      val result = writer.toString.split("\r\n")
      result must haveLength(2)
      val (header, data) = (result(0), result(1))

      header mustEqual "id,name:String,*geom:Point:srid=4326,dtg:Date"
      data mustEqual "fid-1,myname,POINT (45 49),1970-01-01T00:00:00.000Z"
    }

    "should properly export to CSV with options" >> {
      val query = new Query(sftName, Filter.INCLUDE)
      val writer = new StringWriter()
      val attributes = Some(ExportAttributes(Seq("name", "dtg"), fid = false))
      val export = new DelimitedExporter(writer, DataFormats.Csv, attributes, false)
      export.start(sft)
      export.export(features.iterator)
      export.close()

      val result = writer.toString.split("\r\n")
      result must haveLength(1)
      result(0) mustEqual "myname,1970-01-01T00:00:00.000Z"
    }
  }

  "Shapefile Export" >> {
    val sftName = "ShapefileExportTest"
    val (sft, _) = getSftAndFeatures(sftName)

    def checkReplacedAttributes(attr: Seq[String], expected: Seq[String]) = {
      ShapefileExporter.replaceGeom(sft, attr) mustEqual expected
    }

    "should transform 'geom' to 'the_geom' when asking for just 'geom'" >> {
      checkReplacedAttributes(Seq("geom"), Seq("the_geom=geom"))
    }

    "should transform 'geom' in the attributes string when another attribute follows" >> {
      checkReplacedAttributes(Seq("geom", "name"), Seq("the_geom=geom","name"))
    }

    "should transform 'geom' in the attributes string when it follows another attribute" >> {
      checkReplacedAttributes(Seq("name", "geom"), Seq("name","the_geom=geom"))
    }

    "should transform 'geom' in the attributes string when it is between two attributes" >> {
      checkReplacedAttributes(Seq("name", "geom", "dtg"), Seq("name","the_geom=geom","dtg"))
    }

    "should NOT transform 'the_geom' in the attributes string" >> {
      checkReplacedAttributes(Seq("the_geom"), Seq("the_geom"))
    }

    "should NOT transform an incorrect transform in the query" >> {
      checkReplacedAttributes(Seq("name", "geom=the_geom", "dtg"), Seq("name", "geom=the_geom", "dtg", "the_geom=geom"))
    }
  }

  "Avro Export" >> {
    val sftName = "AvroExportTest"
    val (sft, features) = getSftAndFeatures(sftName, 10)

    "should properly export to avro" >> {
      val os = new ByteArrayOutputStream()
      val export = new AvroExporter(os, Deflater.NO_COMPRESSION)
      export.start(sft)
      export.export(features.iterator)
      export.close()

      val result = new AvroDataFileReader(new ByteArrayInputStream(os.toByteArray))
      SimpleFeatureTypes.encodeType(result.getSft) mustEqual SimpleFeatureTypes.encodeType(sft)

      val exported = result.toList
      exported must haveLength(10)
      exported.map(_.getID) must containTheSameElementsAs((1 to 10).map("fid-" + _))
      forall(exported) { feature =>
        feature.getAttribute(0) mustEqual "myname"
        feature.getAttribute(1) mustEqual WKTUtils.read("POINT(45 49)")
        feature.getAttribute(2) mustEqual new Date(0)
      }
    }

    "should compress output" >> {

      val uncompressed :: compressed :: Nil = List(Deflater.NO_COMPRESSION, Deflater.DEFAULT_COMPRESSION).map { c =>
        val os = new ByteArrayOutputStream()
        val export = new AvroExporter(os, c)
        export.start(sft)
        export.export(features.iterator)
        export.close()
        os.toByteArray
      }

      forall(Seq(uncompressed, compressed)) { bytes =>
        val result = new AvroDataFileReader(new ByteArrayInputStream(bytes))
        SimpleFeatureTypes.encodeType(result.getSft) mustEqual SimpleFeatureTypes.encodeType(sft)

        val exported = result.toList
        exported must haveLength(10)
        exported.map(_.getID) must containTheSameElementsAs((1 to 10).map("fid-" + _))
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
