/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/


package org.locationtech.geomesa.parquet


import com.fasterxml.jackson.databind.ObjectMapper
import com.networknt.schema.{JsonSchemaFactory, SpecVersion, ValidationMessage}
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.hadoop.ParquetReader
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.data.DataUtilities
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.filter.FilterHelper
import org.locationtech.geomesa.fs.storage.common.jobs.StorageConfiguration
import org.locationtech.geomesa.fs.storage.parquet.ParquetFileSystemStorage.{ParquetCompressionOpt, validateParquetFile}
import org.locationtech.geomesa.fs.storage.parquet.io.SimpleFeatureParquetSchema.GeoParquetSchemaKey
import org.locationtech.geomesa.fs.storage.parquet.io.SimpleFeatureReadSupport
import org.locationtech.geomesa.fs.storage.parquet.{FilterConverter, SimpleFeatureParquetWriter}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.index.BucketIndex
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.geomesa.utils.text.WKTUtils
import org.locationtech.jts.geom.{Coordinate, Envelope, Geometry, GeometryFactory}
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.specs2.specification.AllExpectations

import java.io.{File, RandomAccessFile}
import java.nio.file.{Files, Paths}
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

@RunWith(classOf[JUnitRunner])
class ParquetReadWriteTest extends Specification with AllExpectations with LazyLogging {

  import scala.collection.JavaConverters._

  sequential

  def transformConf(tsft: SimpleFeatureType): Configuration = {
    val c = new Configuration()
    StorageConfiguration.setSft(c, tsft)
    c
  }

  lazy val f = Files.createTempFile("geomesa", ".parquet")

  val sft = SimpleFeatureTypes.createType("test", "name:String,age:Int,dtg:Date,*position:Point:srid=4326,poly:Polygon")
  val nameAndGeom = SimpleFeatureTypes.createType("test", "name:String,*position:Point:srid=4326")

  val sftConf = {
    val c = new Configuration()
    StorageConfiguration.setSft(c, sft)
    // Use GZIP in tests but snappy in prod due to license issues
    c.set(ParquetCompressionOpt, CompressionCodecName.GZIP.toString)
    c
  }

  val points = {
    val gf = new GeometryFactory
    Seq(
      gf.createPoint(new Coordinate(25.236263, 27.436734)),
      gf.createPoint(new Coordinate(67.2363, 55.236)),
      gf.createPoint(new Coordinate(73.0, 73.0)),
    )
  }

  val polygons = {
    val gf = new GeometryFactory
    Seq(
      gf.createPolygon(Array(
        new Coordinate(0, 0),
        new Coordinate(0, 1),
        new Coordinate(1, 1),
        new Coordinate(1, 0),
        new Coordinate(0, 0),
      )),
      gf.createPolygon(Array(
        new Coordinate(10, 10),
        new Coordinate(10, 15),
        new Coordinate(15, 15),
        new Coordinate(15, 10),
        new Coordinate(10, 10),
      )),
      gf.createPolygon(Array(
        new Coordinate(30, 30),
        new Coordinate(30, 35),
        new Coordinate(35, 35),
        new Coordinate(35, 30),
        new Coordinate(30, 30),
      )),
    )
  }

  val pointsBboxString = {
    val bbox = new Envelope
    points.indices.foreach(i => bbox.expandToInclude(points(i).getEnvelopeInternal))
    s"[${bbox.getMinX}, ${bbox.getMinY}, ${bbox.getMaxX}, ${bbox.getMaxY}]"
  }

  val polygonsBboxString = {
    val bbox = new Envelope
    polygons.indices.foreach(i => bbox.expandToInclude(polygons(i).getEnvelopeInternal))
    s"[${bbox.getMinX}, ${bbox.getMinY}, ${bbox.getMaxX}, ${bbox.getMaxY}]"
  }

  val features = {
    Seq(
      ScalaSimpleFeature.create(sft, "1", "first", 100, "2017-01-01T00:00:00Z", WKTUtils.write(points.head), WKTUtils.write(polygons.head)),
      ScalaSimpleFeature.create(sft, "2", null,    200, "2017-01-02T00:00:00Z", WKTUtils.write(points(1)), WKTUtils.write(polygons(1))),
      ScalaSimpleFeature.create(sft, "3", "third", 300, "2017-01-03T00:00:00Z", WKTUtils.write(points(2)), WKTUtils.write(polygons(2)))
    )
  }

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
    result.toSeq
  }

  def readFile(geoFilter: org.geotools.api.filter.Filter, tsft: SimpleFeatureType): Seq[SimpleFeature] = {
    val pFilter = FilterConverter.convert(tsft, geoFilter)(2)._1.map(FilterCompat.get).getOrElse(FilterCompat.NOOP)
    val conf = transformConf(tsft)

    val geomAttributeName = tsft.getGeometryDescriptor.getName.toString
    val geoms = FilterHelper.extractGeometries(geoFilter, geomAttributeName).values

    val builder = ParquetReader.builder[SimpleFeature](new SimpleFeatureReadSupport, new Path(f.toUri))
    val result = ArrayBuffer.empty[SimpleFeature]
    val index = new BucketIndex[SimpleFeature]

    WithClose(builder.withFilter(pFilter).withConf(conf).build()) { reader =>
      var sf = reader.read()
      while (sf != null) {
        result += sf
        index.insert(sf.getAttribute(geomAttributeName).asInstanceOf[Geometry], sf.getID, sf)
        sf = reader.read()
      }
    }

    if (geoms.nonEmpty) {
      index.query(geoms.head.getEnvelopeInternal).toSeq
    } else {
      result.toSeq
    }
  }

  // Helper method that validates the file metadata against the GeoParquet metadata json schema
  def validateMetadata(metadataString: String): mutable.Set[ValidationMessage] = {
    val schema = {
      // https://geoparquet.org/releases/v1.0.0/schema.json
      val schemaFile = new File(getClass.getClassLoader.getResource("geoparquet-metadata-schema.json").toURI)
      val schemaReader = scala.io.Source.fromFile(schemaFile)
      val schemaString = schemaReader.mkString
      schemaReader.close()
      JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V4).getSchema(schemaString)
    }
    val metadata = new ObjectMapper().readTree(metadataString)

    schema.validate(metadata).asScala
  }

  "SimpleFeatureParquetWriter" should {

    "fail if a corrupt parquet file is written" >> {
      val filePath = new Path(f.toUri)
      WithClose(SimpleFeatureParquetWriter.builder(filePath, sftConf).build()) { writer =>
        features.foreach(writer.write)
      }

      // Corrupt the file by writing an invalid byte somewhere
      val randomAccessFile = new RandomAccessFile(f.toFile, "rw")
      logger.debug(s"File length: ${randomAccessFile.length()}")
      Files.size(f) must beGreaterThan(50L)
      randomAccessFile.seek(50)
      randomAccessFile.writeByte(999)
      randomAccessFile.close()

      // Validate the file
      validateParquetFile(filePath) must throwA[RuntimeException].like {
        case e => e.getMessage mustEqual s"Unable to validate '${filePath}': File may be corrupted"
      }
    }

    "write geoparquet files" >> {
      val writer = SimpleFeatureParquetWriter.builder(new Path(f.toUri), sftConf).build()
      WithClose(writer) { writer =>
        features.foreach(writer.write)
      }
      
      Files.size(f) must beGreaterThan(0L)

      // Check that the GeoParquet metadata is valid json
      val metadata = writer.getFooter.getFileMetaData.getKeyValueMetaData.get(GeoParquetSchemaKey)
      validateMetadata(metadata) must beEmpty

      // Check that the GeoParquet metadata contains the correct bounding box for each geometry
      metadata.contains(pointsBboxString) must beTrue
      metadata.contains(polygonsBboxString) must beTrue
    }

    "write parquet files with no geometries" >> {
      val f = Files.createTempFile("geomesa", ".parquet")
      val sft = SimpleFeatureTypes.createType("test", "name:String,age:Int,dtg:Date")
      val sftConf = {
        val c = new Configuration()
        StorageConfiguration.setSft(c, sft)
        // Use GZIP in tests but snappy in prod due to license issues
        c.set(ParquetCompressionOpt, CompressionCodecName.GZIP.toString)
        c
      }

      val features = {
        Seq(
          ScalaSimpleFeature.create(sft, "1", "first", 100, "2017-01-01T00:00:00Z"),
          ScalaSimpleFeature.create(sft, "2", null, 200, "2017-01-02T00:00:00Z"),
          ScalaSimpleFeature.create(sft, "3", "third", 300, "2017-01-03T00:00:00Z")
        )
      }

      val writer = SimpleFeatureParquetWriter.builder(new Path(f.toUri), sftConf).build()
      WithClose(writer) { writer =>
        features.foreach(writer.write)
      }

      Files.size(f) must beGreaterThan(0L)

      val metadata = writer.getFooter.getFileMetaData.getKeyValueMetaData.get(GeoParquetSchemaKey)
      metadata must beNull
    }

    "read geoparquet files" >> {
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

    val crcFilePath = Paths.get(s"${f.getParent}/.${f.getFileName}.crc")
    Files.deleteIfExists(crcFilePath)
  }
}
