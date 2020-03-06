/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.tools.export

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.charset.StandardCharsets
import java.util.Date
import java.util.zip.Deflater

import org.geotools.data.Query
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithFeatureType
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.features.avro.AvroDataFileReader
import org.locationtech.geomesa.tools.export.formats.{AvroExporter, DelimitedExporter}
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.filter.Filter
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class FeatureExporterTest extends TestWithFeatureType {

  override val spec = "name:String,geom:Point:srid=4326,dtg:Date"

  lazy val features =
    Seq.tabulate(10)(i => ScalaSimpleFeature.create(sft, s"fid-$i", "myname", "POINT(45.0 49.0)", new Date(0)))

  step {
    addFeatures(features)
  }

  "DelimitedExporter" >> {

    "should properly export to CSV" >> {
      val query = new Query(sftName, Filter.INCLUDE)
      val features = ds.getFeatureSource(sftName).getFeatures(query)

      val os = new ByteArrayOutputStream()
      val export = DelimitedExporter.csv(os, null, withHeader = true) // includeIds = true
      export.start(features.getSchema)
      export.export(SelfClosingIterator(features.features()))
      export.close()

      val result = new String(os.toByteArray, StandardCharsets.UTF_8).split("\r\n").toSeq
      result must haveLength(11)
      result.head mustEqual "id,name:String,*geom:Point:srid=4326,dtg:Date"
      result.tail.map(_.split(',').head) must containTheSameElementsAs(this.features.map(_.getID))
      foreach(result.tail.map(_.substring(5)))(_ mustEqual ",myname,POINT (45 49),1970-01-01T00:00:00.000Z")
    }

    "should handle projections" >> {
      val query = new Query(sftName, Filter.INCLUDE, Array("geom", "dtg"))
      val features = ds.getFeatureSource(sftName).getFeatures(query)

      val os = new ByteArrayOutputStream()
      val export = DelimitedExporter.csv(os, null, withHeader = true) // includeIds = true
      export.start(features.getSchema)
      export.export(SelfClosingIterator(features.features()))
      export.close()

      val result = new String(os.toByteArray, StandardCharsets.UTF_8).split("\r\n").toSeq
      result must haveLength(11)
      result.head mustEqual "id,*geom:Point:srid=4326,dtg:Date"
      result.tail.map(_.split(',').head) must containTheSameElementsAs(this.features.map(_.getID))
      foreach(result.tail.map(_.substring(5)))(_ mustEqual ",POINT (45 49),1970-01-01T00:00:00.000Z")
    }

    "should handle transforms" >> {
      val query = new Query(sftName, Filter.INCLUDE, Array("derived=strConcat(name, '-test')", "geom", "dtg"))
      val features = ds.getFeatureSource(sftName).getFeatures(query)

      val os = new ByteArrayOutputStream()
      val export = DelimitedExporter.csv(os, null, withHeader = true) // includeIds = true
      export.start(features.getSchema)
      export.export(SelfClosingIterator(features.features()))
      export.close()

      val result = new String(os.toByteArray, StandardCharsets.UTF_8).split("\r\n").toSeq
      result must haveLength(11)
      result.head mustEqual "id,derived:String,*geom:Point:srid=4326,dtg:Date"
      result.tail.map(_.split(',').head) must containTheSameElementsAs(this.features.map(_.getID))
      foreach(result.tail.map(_.substring(5)))(_ mustEqual ",myname-test,POINT (45 49),1970-01-01T00:00:00.000Z")
    }

    "should handle escapes" >> {
      val query = new Query(sftName, Filter.INCLUDE, Array("geom", "dtg", "derived=strConcat(name, ',test')"))
      val features = ds.getFeatureSource(sftName).getFeatures(query)

      val os = new ByteArrayOutputStream()
      val export = DelimitedExporter.csv(os, null, withHeader = true) // includeIds = true
      export.start(features.getSchema)
      export.export(SelfClosingIterator(features.features()))
      export.close()

      val result = new String(os.toByteArray, StandardCharsets.UTF_8).split("\r\n").toSeq
      result must haveLength(11)
      result.head mustEqual "id,*geom:Point:srid=4326,dtg:Date,derived:String"
      result.tail.map(_.split(',').head) must containTheSameElementsAs(this.features.map(_.getID))
      foreach(result.tail.map(_.substring(5)))(_ mustEqual ",POINT (45 49),1970-01-01T00:00:00.000Z,\"myname,test\"")
    }
  }

  "Avro Export" >> {

    "should handle transforms" >> {
      val query = new Query(sftName, Filter.INCLUDE, Array("geom", "dtg", "derived=strConcat(name, '-test')"))
      val featureCollection = ds.getFeatureSource(sftName).getFeatures(query)

      val os = new ByteArrayOutputStream()
      val export = new AvroExporter(os, null, Some(Deflater.NO_COMPRESSION))
      export.start(featureCollection.getSchema)
      export.export(SelfClosingIterator(featureCollection.features()))
      export.close()

      val result = new AvroDataFileReader(new ByteArrayInputStream(os.toByteArray))
      SimpleFeatureTypes.encodeType(result.getSft) mustEqual SimpleFeatureTypes.encodeType(featureCollection.getSchema)

      val features = result.toList
      features must haveLength(10)
      features.map(_.getID) must containTheSameElementsAs(this.features.map(_.getID))
      forall(features) { feature =>
        feature.getAttribute(0) mustEqual WKTUtils.read("POINT(45 49)")
        feature.getAttribute(1) mustEqual new Date(0)
        feature.getAttribute(2) mustEqual "myname-test" // derived variable gets bumped to the end
      }
    }
  }
}
