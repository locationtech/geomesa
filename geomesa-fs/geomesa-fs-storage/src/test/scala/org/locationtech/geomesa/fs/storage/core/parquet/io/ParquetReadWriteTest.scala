/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core.parquet.io

import com.google.gson.{JsonElement, JsonParser}
import com.typesafe.scalalogging.LazyLogging
import org.apache.parquet.conf.{ParquetConfiguration, PlainParquetConfiguration}
import org.apache.parquet.filter2.compat.FilterCompat
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.data.DataUtilities
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.fs.storage.core.FileSystemStorage.FileValidationObserver
import org.locationtech.geomesa.fs.storage.core.fs.LocalObjectStore
import org.locationtech.geomesa.fs.storage.core.parquet.ParquetFilterConverter
import org.locationtech.geomesa.fs.storage.core.parquet.schema.SimpleFeatureParquetSchema
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.AttributeOptions
import org.locationtech.geomesa.utils.io.WithClose
import org.specs2.matcher.MatchResult
import org.specs2.mutable.SpecificationWithJUnit

import java.io.RandomAccessFile
import java.nio.file.Files
import scala.collection.mutable.ArrayBuffer

class ParquetReadWriteTest extends SpecificationWithJUnit with LazyLogging {

  import scala.collection.JavaConverters._

  sequential

  def transformConf(tsft: SimpleFeatureType): ParquetConfiguration = {
    val c = new PlainParquetConfiguration()
    SimpleFeatureParquetSchema.setSft(c, tsft)
    c
  }

  // TODO don't use a single file here...
  lazy val f = Files.createTempFile("geomesa", ".parquet")

  val sft = SimpleFeatureTypes.createImmutableType("test", "name:String,age:Int,dtg:Date,*position:Point:srid=4326")
  val nameAndGeom = SimpleFeatureTypes.createImmutableType("test", "name:String,*position:Point:srid=4326")

  val sftConf = {
    val c = new PlainParquetConfiguration()
    SimpleFeatureParquetSchema.setSft(c, sft)
    c
  }

  val features = Seq(
    ScalaSimpleFeature.create(sft, "1", "first", 100, "2017-01-01T00:00:00Z", "POINT (25.236263 27.436734)"),
    ScalaSimpleFeature.create(sft, "2", null,    200, "2017-01-02T00:00:00Z", "POINT (67.2363 55.236)"),
    ScalaSimpleFeature.create(sft, "3", "third", 300, "2017-01-03T00:00:00Z", "POINT (73.0 73.0)")
  )

  def readFile(filter: FilterCompat.Filter = FilterCompat.NOOP, conf: ParquetConfiguration = sftConf): Seq[SimpleFeature] = {
    val builder = ParquetFileSystemReader.builder(LocalObjectStore, f.toUri)
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
    val pFilter = ParquetFilterConverter.convert(tsft, geoFilter)._1.map(FilterCompat.get).getOrElse {
      ko(s"Couldn't extract a filter from ${ECQL.toCQL(geoFilter)}")
      FilterCompat.NOOP
    }
    readFile(pFilter, transformConf(tsft))
  }

  "SimpleFeatureParquetWriter" should {

    "fail if a corrupt parquet file is written" >> {
      WithClose(ParquetFileSystemWriter(sft, Map.empty, LocalObjectStore, s"file://${f.toString}")) { writer =>
        features.foreach(writer.write)
      }

      // corrupt the file by writing invalid bytes somewhere
      val randomAccessFile = new RandomAccessFile(f.toFile, "rw")
      logger.debug(s"File length: ${randomAccessFile.length()}")
      Files.size(f) must beGreaterThan(50L)
      randomAccessFile.seek(40)
      randomAccessFile.writeBytes("abcdefghij")
      randomAccessFile.close()

      // Validate the file
      FileValidationObserver(f.toString).close() must throwA[RuntimeException].like {
        case e => e.getMessage must contain("corrupted")
      }
    }

    "write parquet files" >> {
      WithClose(ParquetFileSystemWriter(sft, Map.empty, LocalObjectStore, s"file://${f.toString}")) { writer =>
        features.foreach(writer.write)
      }
      Files.size(f) must beGreaterThan(0L)
    }

    "read parquet files" >> {
      val result = readFile(FilterCompat.NOOP, sftConf)
      result mustEqual features
    }

    "only read transform columns" >> {
      val tsft = SimpleFeatureTypes.createImmutableType("test", "name:String,dtg:Date,*position:Point:srid=4326")
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

    "read and write structural json" >> {
      val avro =
        """{
          |  "type": "record",
          |  "name": "props",
          |  "fields": [
          |    { "name": "name", "type": ["null", "string"], "default": null },
          |    { "name": "age", "type": "int" },
          |    { "name": "tags", "type": { "type": "array", "items": "string" } },
          |    { "name": "scores", "type": { "type": "map", "values": "long" } },
          |    { "name": "nested", "type": ["null", {
          |        "type": "record",
          |        "name": "nested",
          |        "fields": [ { "name": "flag", "type": "boolean" } ]
          |    }], "default": null }
          |  ]
          |}""".stripMargin

      val jsonSft = SimpleFeatureTypes.createType("json-test", "props:String:json=true,*position:Point:srid=4326")
      jsonSft.getDescriptor("props").getUserData.put(AttributeOptions.OptJsonSchema, avro)

      val jsonValues = Seq(
        """{"name":"alice","age":30,"tags":["a","b"],"scores":{"x":1,"y":2},"nested":{"flag":true}}""",
        // omitted optional fields (name, nested), empty array and map
        """{"age":7,"tags":[],"scores":{}}""",
        // explicit nulls for optional fields
        """{"name":null,"age":99,"tags":["z"],"scores":{"k":42},"nested":null}""",
        null // null json value -> null attribute
      )

      val jsonFeatures = jsonValues.zipWithIndex.map { case (json, i) =>
        ScalaSimpleFeature.create(jsonSft, (i + 1).toString, json, s"POINT (${i} ${i})")
      }

      // objects drop explicit nulls for optional fields on the structural round-trip
      def normalize(json: String): JsonElement = {
        val tree = JsonParser.parseString(json).getAsJsonObject
        val nullKeys = tree.entrySet().asScala.collect { case e if e.getValue.isJsonNull => e.getKey }.toSeq
        nullKeys.foreach(tree.remove)
        tree
      }

      checkStructuralJsonRoundTrip(jsonSft, jsonFeatures, normalize)
    }

    "read and write structural json with a top-level array of records" >> {
      val avro =
        """{
          |  "type": "array",
          |  "items": {
          |    "type": "record",
          |    "name": "item",
          |    "fields": [
          |      { "name": "id", "type": "int" },
          |      { "name": "label", "type": ["null", "string"], "default": null }
          |    ]
          |  }
          |}""".stripMargin

      val jsonSft = SimpleFeatureTypes.createType("json-array-test", "props:String:json=true,*position:Point:srid=4326")
      jsonSft.getDescriptor("props").getUserData.put(AttributeOptions.OptJsonSchema, avro)

      val jsonValues = Seq(
        """[{"id":1,"label":"a"},{"id":2,"label":"b"}]""",
        "[]", // empty array
        // omitted optional field on the record
        """[{"id":42}]""",
        null // null json value -> null attribute
      )

      val jsonFeatures = jsonValues.zipWithIndex.map { case (json, i) =>
        ScalaSimpleFeature.create(jsonSft, (i + 1).toString, json, s"POINT (${i} ${i})")
      }

      checkStructuralJsonRoundTrip(jsonSft, jsonFeatures, JsonParser.parseString)
    }

    "read and write structural json with a top-level map of records" >> {
      val avro =
        """{
          |  "type": "map",
          |  "values": {
          |    "type": "record",
          |    "name": "value",
          |    "fields": [
          |      { "name": "count", "type": "long" },
          |      { "name": "label", "type": ["null", "string"], "default": null }
          |    ]
          |  }
          |}""".stripMargin

      val jsonSft = SimpleFeatureTypes.createType("json-map-test", "props:String:json=true,*position:Point:srid=4326")
      jsonSft.getDescriptor("props").getUserData.put(AttributeOptions.OptJsonSchema, avro)

      val jsonValues = Seq(
        """{"x":{"count":1,"label":"a"},"y":{"count":2,"label":"b"}}""",
        "{}", // empty map
        // omitted optional field on the record
        """{"k":{"count":42}}""",
        null // null json value -> null attribute
      )

      val jsonFeatures = jsonValues.zipWithIndex.map { case (json, i) =>
        ScalaSimpleFeature.create(jsonSft, (i + 1).toString, json, s"POINT (${i} ${i})")
      }

      checkStructuralJsonRoundTrip(jsonSft, jsonFeatures, JsonParser.parseString)
    }

    "read and write structural json with decimals" >> {
      // precision <= 9 stores as int32, <= 18 as int64, > 18 as fixed-length bytes - cover all three,
      // including negatives to exercise the sign-extended fixed-length padding
      val avro =
        """{
          |  "type": "record",
          |  "name": "props",
          |  "fields": [
          |    { "name": "small", "type": { "type": "bytes", "logicalType": "decimal", "precision": 9, "scale": 2 } },
          |    { "name": "medium", "type": { "type": "bytes", "logicalType": "decimal", "precision": 18, "scale": 4 } },
          |    { "name": "large", "type": { "type": "bytes", "logicalType": "decimal", "precision": 38, "scale": 6 } }
          |  ]
          |}""".stripMargin

      val jsonSft = SimpleFeatureTypes.createType("json-decimal-test", "props:String:json=true,*position:Point:srid=4326")
      jsonSft.getDescriptor("props").getUserData.put(AttributeOptions.OptJsonSchema, avro)

      val jsonValues = Seq(
        """{"small":123.45,"medium":1234.5678,"large":123456789012345.678901}""",
        """{"small":-1.20,"medium":-0.0001,"large":-98765432109876.543210}""",
        null // null json value -> null attribute
      )

      val jsonFeatures = jsonValues.zipWithIndex.map { case (json, i) =>
        ScalaSimpleFeature.create(jsonSft, (i + 1).toString, json, s"POINT (${i} ${i})")
      }

      // gson compares numeric primitives by value, so trailing zeros from rescaling don't matter
      checkStructuralJsonRoundTrip(jsonSft, jsonFeatures, JsonParser.parseString)
    }

    "reject a top-level scalar structural json schema" >> {
      val jsonSft = SimpleFeatureTypes.createType("json-scalar-test", "props:String:json=true,*position:Point:srid=4326")
      jsonSft.getDescriptor("props").getUserData.put(AttributeOptions.OptJsonSchema, """"string"""")
      SimpleFeatureParquetSchema(jsonSft, Map.empty[String, String]) must throwAn[IllegalArgumentException]
    }
  }

  // writes the features to a parquet file, reads them back, and asserts the json attribute round-trips.
  // `expect` maps an original json string to the tree it should equal after the round-trip.
  private def checkStructuralJsonRoundTrip(
      sft: SimpleFeatureType,
      features: Seq[SimpleFeature],
      expect: String => JsonElement): MatchResult[Any] = {
    val jsonFile = Files.createTempFile("geomesa-json", ".parquet")
    try {
      WithClose(ParquetFileSystemWriter(sft, Map.empty, LocalObjectStore, jsonFile.toUri.toString)) { writer =>
        features.foreach(writer.write)
      }

      val readConf = new PlainParquetConfiguration()
      SimpleFeatureParquetSchema.setSft(readConf, sft)
      val result = {
        val builder = ParquetFileSystemReader.builder(LocalObjectStore, jsonFile.toUri)
        val buffer = ArrayBuffer.empty[SimpleFeature]
        WithClose(builder.withFilter(FilterCompat.NOOP).withConf(readConf).build()) { reader =>
          var sf = reader.read()
          while (sf != null) {
            buffer += ScalaSimpleFeature.copy(sf)
            sf = reader.read()
          }
        }
        buffer.toSeq
      }

      result must haveSize(features.size)
      val byId = result.map(f => f.getID -> f).toMap
      foreach(features) { expected =>
        val actual = byId.get(expected.getID)
        actual must beSome
        val expectedJson = expected.getAttribute("props").asInstanceOf[String]
        val actualJson = actual.get.getAttribute("props").asInstanceOf[String]
        if (expectedJson == null) {
          actualJson must beNull
        } else {
          // compare parsed trees so key ordering / whitespace don't matter
          JsonParser.parseString(actualJson) mustEqual expect(expectedJson)
        }
      }
    } finally {
      Files.deleteIfExists(jsonFile)
    }
  }

  step {
    Files.deleteIfExists(f)
  }
}
