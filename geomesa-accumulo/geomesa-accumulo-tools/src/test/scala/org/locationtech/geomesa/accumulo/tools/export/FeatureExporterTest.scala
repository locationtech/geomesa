/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.tools.export

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, StringWriter}
import java.util.Date
import java.util.zip.Deflater

import org.apache.accumulo.core.client.mock.MockInstance
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.geotools.data.{DataStoreFinder, Query}
import org.geotools.factory.Hints
import org.geotools.feature.DefaultFeatureCollection
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, AccumuloDataStoreParams}
import org.locationtech.geomesa.features.ScalaSimpleFeatureFactory
import org.locationtech.geomesa.features.avro.AvroDataFileReader
import org.locationtech.geomesa.tools.export.formats.{AvroExporter, DelimitedExporter}
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.filter.Filter
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class FeatureExporterTest extends Specification {

  sequential

  def getDataStoreAndSft(sftName: String, numFeatures: Int = 1) = {
    import scala.collection.JavaConversions._

    val sft = SimpleFeatureTypes.createType(sftName, "name:String,geom:Point:srid=4326,dtg:Date")

    val featureCollection = new DefaultFeatureCollection(sft.getTypeName, sft)

    val attributes = Array("myname", "POINT(45.0 49.0)", new Date(0))
    (1 to numFeatures).foreach { i =>
      val feature = ScalaSimpleFeatureFactory.buildFeature(sft, attributes, s"fid-$i")
      feature.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
      featureCollection.add(feature)
    }

    val connector = new MockInstance().getConnector("", new PasswordToken(""))

    val ds = DataStoreFinder.getDataStore(Map(AccumuloDataStoreParams.ConnectorParam.key -> connector,
      AccumuloDataStoreParams.CatalogParam.key -> sftName, AccumuloDataStoreParams.CachingParam.key -> false))
    ds.createSchema(sft)
    ds.asInstanceOf[AccumuloDataStore].getFeatureSource(sftName).addFeatures(featureCollection)
    (ds, sft)
  }

  "DelimitedExporter" >> {
    val sftName = "DelimitedExportTest"
    val (ds, sft) = getDataStoreAndSft(sftName)

    "should properly export to CSV" >> {
      val query = new Query(sftName, Filter.INCLUDE)
      val features = ds.getFeatureSource(sftName).getFeatures(query)

      val writer = new StringWriter()
      val export = DelimitedExporter.csv(writer, withHeader = true, includeIds = true)
      export.start(features.getSchema)
      export.export(SelfClosingIterator(features.features()))
      export.close()

      val result = writer.toString.split("\r\n")
      val (header, data) = (result(0), result(1))

      header mustEqual "id,name:String,*geom:Point:srid=4326,dtg:Date"
      data mustEqual "fid-1,myname,POINT (45 49),1970-01-01T00:00:00.000Z"
    }

    "should handle projections" >> {
      val query = new Query(sftName, Filter.INCLUDE, Array("geom", "dtg"))
      val features = ds.getFeatureSource(sftName).getFeatures(query)

      val writer = new StringWriter()
      val export = DelimitedExporter.csv(writer, withHeader = true, includeIds = true)
      export.start(features.getSchema)
      export.export(SelfClosingIterator(features.features()))
      export.close()

      val result = writer.toString.split("\r\n")
      val (header, data) = (result(0), result(1))

      header mustEqual "id,*geom:Point:srid=4326,dtg:Date"
      data mustEqual "fid-1,POINT (45 49),1970-01-01T00:00:00.000Z"
    }

    "should handle transforms" >> {
      val query = new Query(sftName, Filter.INCLUDE, Array("derived=strConcat(name, '-test')", "geom", "dtg"))
      val features = ds.getFeatureSource(sftName).getFeatures(query)

      val writer = new StringWriter()
      val export = DelimitedExporter.csv(writer, withHeader = true, includeIds = true)
      export.start(features.getSchema)
      export.export(SelfClosingIterator(features.features()))
      export.close()

      val result = writer.toString.split("\r\n")
      val (header, data) = (result(0), result(1))

      header mustEqual "id,derived:String,*geom:Point:srid=4326,dtg:Date"
      data mustEqual "fid-1,myname-test,POINT (45 49),1970-01-01T00:00:00.000Z"
    }

    "should handle escapes" >> {
      val query = new Query(sftName, Filter.INCLUDE, Array("geom", "dtg", "derived=strConcat(name, ',test')"))
      val features = ds.getFeatureSource(sftName).getFeatures(query)

      val writer = new StringWriter()
      val export = DelimitedExporter.csv(writer, withHeader = true, includeIds = true)
      export.start(features.getSchema)
      export.export(SelfClosingIterator(features.features()))
      export.close()

      val result = writer.toString.split("\r\n")
      val (header, data) = (result(0), result(1))

      header mustEqual "id,*geom:Point:srid=4326,dtg:Date,derived:String"
      data mustEqual "fid-1,POINT (45 49),1970-01-01T00:00:00.000Z,\"myname,test\""
    }
  }

  "Avro Export" >> {
    val sftName = "AvroExportTest"
    val (ds, sft) = getDataStoreAndSft(sftName, 10)

    "should handle transforms" >> {
      val query = new Query(sftName, Filter.INCLUDE, Array("geom", "dtg", "derived=strConcat(name, '-test')"))
      val featureCollection = ds.getFeatureSource(sftName).getFeatures(query)

      val os = new ByteArrayOutputStream()
      val export = new AvroExporter(Deflater.NO_COMPRESSION, os)
      export.start(featureCollection.getSchema)
      export.export(SelfClosingIterator(featureCollection.features()))
      export.close()

      val result = new AvroDataFileReader(new ByteArrayInputStream(os.toByteArray))
      SimpleFeatureTypes.encodeType(result.getSft) mustEqual SimpleFeatureTypes.encodeType(featureCollection.getSchema)

      val features = result.toList
      features must haveLength(10)
      features.map(_.getID) must containTheSameElementsAs((1 to 10).map("fid-" + _))
      forall(features) { feature =>
        features.head.getAttribute(0) mustEqual WKTUtils.read("POINT(45 49)")
        features.head.getAttribute(1) mustEqual new Date(0)
        features.head.getAttribute(2) mustEqual "myname-test" // derived variable gets bumped to the end
      }
    }
  }
}
