/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.tools.export

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.charset.StandardCharsets
import java.util.Date
import java.util.zip.Deflater

import org.geotools.data.DataUtilities
import org.geotools.util.factory.Hints
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeatureFactory
import org.locationtech.geomesa.features.avro.AvroDataFileReader
import org.locationtech.geomesa.tools.export.formats.{AvroExporter, DelimitedExporter}
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
      val os = new ByteArrayOutputStream()
      val export = DelimitedExporter.csv(os, null, withHeader = true, includeIds = true)
      export.start(features.head.getFeatureType)
      export.export(features.iterator)
      export.close()

      val result = new String(os.toByteArray, StandardCharsets.UTF_8).split("\r\n")
      result must haveLength(2)
      val (header, data) = (result(0), result(1))

      header mustEqual "id,name:String,*geom:Point:srid=4326,dtg:Date"
      data mustEqual "fid-0,myname,POINT (45 49),1970-01-01T00:00:00.000Z"
    }

    "properly export to CSV with options" >> {
      // simulate a projecting read
      val sft = SimpleFeatureTypes.createType("DelimitedExportTest", "name:String,dtg:Date")
      val features = createFeatures("DelimitedExportTest").map(DataUtilities.reType(sft, _))
      val os = new ByteArrayOutputStream()
      val export = DelimitedExporter.csv(os, null, withHeader = false, includeIds = false)
      export.start(sft)
      export.export(features.iterator)
      export.close()

      val result = new String(os.toByteArray, StandardCharsets.UTF_8).split("\r\n")
      result must haveLength(1)
      result(0) mustEqual "myname,1970-01-01T00:00:00.000Z"
    }
  }

  "Avro Export" should {

    "properly export to avro" >> {
      val features = createFeatures("AvroExportTest", 10)
      val os = new ByteArrayOutputStream()
      val export = new AvroExporter(os, null, Some(Deflater.NO_COMPRESSION))
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
        val export = new AvroExporter(os, null, Option(c))
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
