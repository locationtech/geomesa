/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/


package org.locationtech.geomesa.parquet


import java.nio.file.Files
import java.time.Instant

import org.locationtech.jts.geom.{Coordinate, Point}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.hadoop.ParquetReader
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.geotools.factory.CommonFactoryFinder
import org.geotools.geometry.jts.JTSFactoryFinder
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.fs.storage.common.jobs.StorageConfiguration
import org.locationtech.geomesa.parquet.ParquetFileSystemStorage.ParquetCompressionOpt
import org.locationtech.geomesa.parquet.jobs.SimpleFeatureReadSupport
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.specs2.specification.AllExpectations

import scala.collection.mutable

@RunWith(classOf[JUnitRunner])
class ParquetReadWriteTest extends Specification with AllExpectations {

  sequential

  def transformConf(tsft: SimpleFeatureType) = {
    val c = new Configuration()
    StorageConfiguration.setSft(c, tsft)
    c
  }

  "SimpleFeatureParquetWriter" should {

    val f = Files.createTempFile("geomesa", ".parquet")
    val gf = JTSFactoryFinder.getGeometryFactory
    val sft = SimpleFeatureTypes.createType("test", "name:String,age:Int,dtg:Date,*position:Point:srid=4326")

    val sftConf = {
      val c = new Configuration()
      StorageConfiguration.setSft(c, sft)
      // Use GZIP in tests but snappy in prod due to license issues
      c.set(ParquetCompressionOpt, CompressionCodecName.GZIP.toString)
      c
    }

    "write parquet files" >> {


      val writer = SimpleFeatureParquetWriter.builder(new Path(f.toUri), sftConf).build()

      val d1 = java.util.Date.from(Instant.parse("2017-01-01T00:00:00Z"))
      val d2 = java.util.Date.from(Instant.parse("2017-01-02T00:00:00Z"))
      val d3 = java.util.Date.from(Instant.parse("2017-01-03T00:00:00Z"))

      val sf = new ScalaSimpleFeature(sft, "1", Array("first", Integer.valueOf(100),  d1, gf.createPoint(new Coordinate(25.236263, 27.436734))))
      val sf2 = new ScalaSimpleFeature(sft, "2", Array(null, Integer.valueOf(200),    d2, gf.createPoint(new Coordinate(67.2363, 55.236))))
      val sf3 = new ScalaSimpleFeature(sft, "3", Array("third", Integer.valueOf(300), d3, gf.createPoint(new Coordinate(73.0, 73.0))))
      writer.write(sf)
      writer.write(sf2)
      writer.write(sf3)
      writer.close()
      Files.size(f) must be greaterThan 0
    }

    "read parquet files" >> {
      val reader = ParquetReader.builder[SimpleFeature](new SimpleFeatureReadSupport, new Path(f.toUri))
        .withFilter(FilterCompat.NOOP)
        .withConf(sftConf)
        .build()

      val sf = reader.read()
      sf.getAttributeCount mustEqual 4
      sf.getID must be equalTo "1"
      sf.getAttribute("name") must be equalTo "first"
      sf.getDefaultGeometry.asInstanceOf[Point].getX mustEqual 25.236263
      sf.getDefaultGeometry.asInstanceOf[Point].getY mustEqual 27.436734

      val sf2 = reader.read()
      sf2.getAttributeCount mustEqual 4
      sf2.getID must be equalTo "2"
      sf2.getAttribute("name") must beNull
      sf2.getDefaultGeometry.asInstanceOf[Point].getX mustEqual 67.2363
      sf2.getDefaultGeometry.asInstanceOf[Point].getY mustEqual 55.236

      val sf3 = reader.read()
      sf3.getAttributeCount mustEqual 4
      sf3.getID must be equalTo "3"
      sf3.getAttribute("name") must be equalTo "third"
      sf3.getDefaultGeometry.asInstanceOf[Point].getX mustEqual 73.0
      sf3.getDefaultGeometry.asInstanceOf[Point].getY mustEqual 73.0
    }

    "only read transform columns" >> {
      val tsft = SimpleFeatureTypes.createType("test", "name:String,dtg:Date,*position:Point:srid=4326")
      val reader = ParquetReader.builder[SimpleFeature](new SimpleFeatureReadSupport, new Path(f.toUri))
        .withFilter(FilterCompat.NOOP)
        .withConf(transformConf(tsft))
        .build()
      val sf = reader.read()
      sf.getAttributeCount mustEqual 3
      sf.getID must be equalTo "1"
      sf.getAttribute("name") must be equalTo "first"
      sf.getDefaultGeometry.asInstanceOf[Point].getX mustEqual 25.236263
      sf.getDefaultGeometry.asInstanceOf[Point].getY mustEqual 27.436734

      val sf2 = reader.read()
      sf2.getAttributeCount mustEqual 3
      sf2.getID must be equalTo "2"
      sf2.getAttribute("name") must beNull
      sf2.getDefaultGeometry.asInstanceOf[Point].getX mustEqual 67.2363
      sf2.getDefaultGeometry.asInstanceOf[Point].getY mustEqual 55.236

    }

    "perform filtering" >> {
      val nameAndGeom = SimpleFeatureTypes.createType("test", "name:String,*position:Point:srid=4326")

      val ff = CommonFactoryFinder.getFilterFactory2
      val geoFilter = ff.equals(ff.property("name"), ff.literal("first"))

      def getFeatures(geoFilter: org.opengis.filter.Filter, tsft: SimpleFeatureType): Seq[SimpleFeature] = {
        val pFilter = FilterCompat.get(new FilterConverter(tsft).convert(geoFilter)._1.get)

        val reader = ParquetReader.builder[SimpleFeature](new SimpleFeatureReadSupport, new Path(f.toUri))
          .withFilter(pFilter)
          .withConf(transformConf(tsft))
          .build()

        val res = mutable.ListBuffer.empty[SimpleFeature]
        var sf: SimpleFeature = reader.read()
        while( sf != null) {
          res += sf
          sf = reader.read()
        }
        res
      }

      "equals" >> {
        val res = getFeatures(ff.equals(ff.property("name"), ff.literal("first")), nameAndGeom)
        res.size mustEqual 1
        val sf = res.head

        sf.getAttributeCount mustEqual 2
        sf.getID must be equalTo "1"
        sf.getAttribute("name") must be equalTo "first"
        sf.getDefaultGeometry.asInstanceOf[Point].getX mustEqual 25.236263
        sf.getDefaultGeometry.asInstanceOf[Point].getY mustEqual 27.436734

      }

      "not equals" >> {
        val res = getFeatures(ff.notEqual(ff.property("name"), ff.literal("first")), nameAndGeom)
        res.size mustEqual 2

        val two = res.head
        two.getAttributeCount mustEqual 2
        two.getID must be equalTo "2"
        two.getAttribute("name") must beNull
        two.getDefaultGeometry.asInstanceOf[Point].getX mustEqual 67.2363
        two.getDefaultGeometry.asInstanceOf[Point].getX mustEqual 67.2363

        val three = res.last
        three.getAttributeCount mustEqual 2
        three.getID must be equalTo "3"
        three.getAttribute("name") must be equalTo "third"
        three.getDefaultGeometry.asInstanceOf[Point].getX mustEqual 73.0
        three.getDefaultGeometry.asInstanceOf[Point].getX mustEqual 73.0
      }

      "query with a bbox" >> {
        import scala.collection.JavaConversions._

        "small bbox" >> {
          val env = gf.buildGeometry(List(gf.createPoint(new Coordinate(25.236263, 27.436734)))).getEnvelopeInternal
          val res = getFeatures(ff.bbox("position", env.getMinX - .1, env.getMinY - .1, env.getMaxX + .1, env.getMaxY + .1, "EPSG:4326"), nameAndGeom)
          res.size mustEqual 1
          res.head.getAttribute("name") mustEqual "first"
        }

        "two points" >> {
          val env = gf.buildGeometry(List(
            gf.createPoint(new Coordinate(25.236263, 27.436734)),
            gf.createPoint(new Coordinate(67.2363, 55.236))
          )).getEnvelopeInternal
          val res = getFeatures(ff.bbox("position", env.getMinX - .1, env.getMinY - .1, env.getMaxX + .1, env.getMaxY + .1, "EPSG:4326"), nameAndGeom)
          res.size mustEqual 2
          res.head.getAttribute("name") mustEqual "first"

          res.last.getID mustEqual "2"
        }

        "3 points" >> {
          val res = getFeatures(ff.bbox("position", -30, -30, 80, 80, "EPSG:4326"), nameAndGeom)
          res.size mustEqual 3
          res.head.getAttribute("name") mustEqual "first"
          res.last.getID mustEqual "3"
        }
      }

      "query with time" >> {

        "day1" >> {
          val res = getFeatures(ff.between(ff.property("dtg"), ff.literal("2016-12-131T12:00:00Z"), ff.literal("2017-01-01T00:00:00Z")), sft)
          res.size mustEqual 1
          res.head.getAttribute("name") mustEqual "first"
        }

        "day 2 and 3" >> {
          val res = getFeatures(ff.between(ff.property("dtg"), ff.literal("2017-01-01T12:00:00Z"), ff.literal("2017-01-31T00:00:00Z")), sft)
          res.size mustEqual 2
          res.head.getAttribute("name") must beNull
          res.last.getAttribute("name") mustEqual "third"
        }
      }
    }

    step {
       Files.deleteIfExists(f)
    }

  }
}
