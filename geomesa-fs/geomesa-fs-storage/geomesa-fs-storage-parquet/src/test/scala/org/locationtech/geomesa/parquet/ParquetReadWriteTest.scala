/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/


package org.locationtech.geomesa.parquet


import java.nio.file.Files

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.hadoop.ParquetReader
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.geotools.data.DataUtilities
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.fs.storage.common.jobs.StorageConfiguration
import org.locationtech.geomesa.parquet.ParquetFileSystemStorage.ParquetCompressionOpt
import org.locationtech.geomesa.parquet.io.SimpleFeatureReadSupport
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.specs2.specification.AllExpectations

import scala.collection.mutable.ArrayBuffer

@RunWith(classOf[JUnitRunner])
class ParquetReadWriteTest extends Specification with AllExpectations {

  import scala.collection.JavaConverters._

  sequential

  def transformConf(tsft: SimpleFeatureType): Configuration = {
    val c = new Configuration()
    StorageConfiguration.setSft(c, tsft)
    c
  }

  lazy val f = Files.createTempFile("geomesa", ".parquet")

  val sft = SimpleFeatureTypes.createType("test", "name:String,age:Int,dtg:Date,*position:Point:srid=4326")
  val nameAndGeom = SimpleFeatureTypes.createType("test", "name:String,*position:Point:srid=4326")

  val sftConf = {
    val c = new Configuration()
    StorageConfiguration.setSft(c, sft)
    // Use GZIP in tests but snappy in prod due to license issues
    c.set(ParquetCompressionOpt, CompressionCodecName.GZIP.toString)
    c
  }

  val features = Seq(
    ScalaSimpleFeature.create(sft, "1", "first", 100, "2017-01-01T00:00:00Z", "POINT (25.236263 27.436734)"),
    ScalaSimpleFeature.create(sft, "2", null,    200, "2017-01-02T00:00:00Z", "POINT (67.2363 55.236)"),
    ScalaSimpleFeature.create(sft, "3", "third", 300, "2017-01-03T00:00:00Z", "POINT (73.0 73.0)")
  )

  def readFile(filter: FilterCompat.Filter = FilterCompat.NOOP, conf: Configuration = sftConf): Seq[SimpleFeature] = {
    val builder = ParquetReader.builder[SimpleFeature](new SimpleFeatureReadSupport, new Path(f.toUri))
    val result = ArrayBuffer.empty[SimpleFeature]
    WithClose(builder.withFilter(filter).withConf(conf).build()) { reader =>
      var sf = reader.read()
      while (sf != null) {
        result += sf
        sf = reader.read()
      }
    }
    result
  }

  def readFile(geoFilter: org.opengis.filter.Filter, tsft: SimpleFeatureType): Seq[SimpleFeature] = {
    val pFilter = FilterConverter.convert(tsft, geoFilter)._1.map(FilterCompat.get).getOrElse {
      ko(s"Couldn't extract a filter from ${ECQL.toCQL(geoFilter)}")
      FilterCompat.NOOP
    }
    readFile(pFilter, transformConf(tsft))
  }

  "SimpleFeatureParquetWriter" should {

    "write parquet files" >> {
      WithClose(SimpleFeatureParquetWriter.builder(new Path(f.toUri), sftConf).build()) { writer =>
        features.foreach(writer.write)
      }
      Files.size(f) must beGreaterThan(0L)
    }

    "read parquet files" >> {
      val result = readFile(FilterCompat.NOOP, sftConf)
      result mustEqual features
    }

    "only read transform columns" >> {
      val tsft = SimpleFeatureTypes.createType("test", "name:String,dtg:Date,*position:Point:srid=4326")
      val result = readFile(FilterCompat.NOOP, transformConf(tsft))
      foreach(result)(_.getFeatureType mustEqual tsft)
      result.map(_.getAttributes.asScala) mustEqual features.map(DataUtilities.reType(tsft, _).getAttributes.asScala)
    }

    "perform filtering on attribute equals" >> {
      val result = readFile(ECQL.toFilter("name = 'first'"), nameAndGeom)
      result must haveSize(1)
      result.head.getFeatureType mustEqual nameAndGeom
      result.head.getID mustEqual "1"
      result.head.getAttributes.asScala mustEqual DataUtilities.reType(nameAndGeom, features.head).getAttributes.asScala
    }

    "perform filtering on attribute not equals" >> {
      val result = readFile(ECQL.toFilter("name <> 'first'"), nameAndGeom)
      result must haveSize(2)
      foreach(result)(_.getFeatureType mustEqual nameAndGeom)
      result.map(_.getID) mustEqual Seq("2", "3")
      result.map(_.getAttributes.asScala) mustEqual features.drop(1).map(DataUtilities.reType(nameAndGeom, _).getAttributes.asScala)
    }

    "perform filtering on small bbox" >> {
      val result = readFile(ECQL.toFilter("bbox(position, 25.136263, 27.336734, 25.336263, 27.536734)"), nameAndGeom)
      result must haveSize(1)
      result.head.getFeatureType mustEqual nameAndGeom
      result.head.getID mustEqual "1"
      result.head.getAttributes.asScala mustEqual DataUtilities.reType(nameAndGeom, features.head).getAttributes.asScala
    }

    "perform filtering on medium bbox" >> {
      val result = readFile(ECQL.toFilter("bbox(position, 25.136263, 27.336734, 67.3363, 55.336)"), nameAndGeom)
      result must haveSize(2)
      foreach(result)(_.getFeatureType mustEqual nameAndGeom)
      result.map(_.getID) mustEqual Seq("1", "2")
      result.map(_.getAttributes.asScala) mustEqual features.take(2).map(DataUtilities.reType(nameAndGeom, _).getAttributes.asScala)
    }

    "perform filtering on large bbox" >> {
      val result = readFile(ECQL.toFilter("bbox(position, -30, -30, 80, 80)"), nameAndGeom)
      result must haveSize(3)
      foreach(result)(_.getFeatureType mustEqual nameAndGeom)
      result.map(_.getID) mustEqual Seq("1", "2", "3")
      result.map(_.getAttributes.asScala) mustEqual features.map(DataUtilities.reType(nameAndGeom, _).getAttributes.asScala)
    }

    "perform filtering on two week duration" >> {
      val result = readFile(ECQL.toFilter("dtg BETWEEN '2016-12-13T12:00:00Z' AND '2017-01-01T00:00:00Z'"), sft)
      result mustEqual features.take(1)
    }

    "perform filtering on one month duration" >> {
      val result = readFile(ECQL.toFilter("dtg BETWEEN '2017-01-01T12:00:00Z' AND '2017-01-31T00:00:00Z'"), sft)
      result mustEqual features.drop(1)
    }
  }

  step {
    Files.deleteIfExists(f)
  }
}
