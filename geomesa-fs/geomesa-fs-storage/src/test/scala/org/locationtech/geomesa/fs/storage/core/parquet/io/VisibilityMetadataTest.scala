/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core.parquet.io

import com.google.gson.JsonParser
import org.apache.accumulo.access.{AccessEvaluator, AccessExpression, Authorizations}
import org.apache.iceberg.MetadataColumns
import org.apache.iceberg.mapping.{MappingUtil, NameMappingParser}
import org.apache.parquet.conf.PlainParquetConfiguration
import org.apache.parquet.example.data.simple.SimpleGroupFactory
import org.apache.parquet.hadoop.{ParquetFileReader, ParquetFileWriter}
import org.apache.parquet.hadoop.example.ExampleParquetWriter
import org.apache.parquet.io.{LocalInputFile, LocalOutputFile}
import org.apache.parquet.schema.MessageTypeParser
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.fs.storage.core.fs.LocalObjectStore
import org.locationtech.geomesa.fs.storage.core.iceberg.SimpleFeatureIcebergSchema
import org.locationtech.geomesa.fs.storage.core.parquet.schema.VisibilityMetadata
import org.locationtech.geomesa.fs.storage.core.schema.SimpleFeatureSchema
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.specs2.mutable.SpecificationWithJUnit

import java.nio.file.{Files, Paths, StandardCopyOption}

class VisibilityMetadataTest extends SpecificationWithJUnit {
  import scala.collection.JavaConverters._

  private val sft = SimpleFeatureTypes.createType("rollup", "name:String")
  private val conf = Map(SimpleFeatureSchema.IcebergNameMappingKey ->
      """[{"field-id":1,"names":["__fid__"]},{"field-id":2,"names":["__vis__"]},{"field-id":3,"names":["name"]}]""")

  "Visibility footer rollup" should {
    "write visibility and GeoParquet metadata together for spatial parity fixtures" >> {
      val spatialSft = SimpleFeatureTypes.createType("spatial-rollup", "name:String,*geom:Point:srid=4326")
      val schema = SimpleFeatureIcebergSchema.create(spatialSft)
      val spatialConf = Map(SimpleFeatureSchema.IcebergNameMappingKey -> NameMappingParser.toJson(MappingUtil.create(schema)))
      val file = Files.createTempFile("geomesa-spatial-visibility", ".parquet")
      val points = Seq("POINT (-1 -1)", "POINT (1 1)", "POINT (2 2)", "POINT (20 20)", "POINT (0 0)", "POINT (5 5)")
      val expressions = Seq("A", "A&B", "C|D", "A", "", null)
      try {
        WithClose(ParquetFileSystemWriter(spatialSft, spatialConf, LocalObjectStore, file.toUri.toString)) { writer =>
          points.zip(expressions).zipWithIndex.foreach { case ((point, vis), i) =>
            val feature = ScalaSimpleFeature.create(spatialSft, i.toString, s"row-$i", point)
            if (vis != null) feature.getUserData.put("geomesa.feature.visibility", vis)
            writer.write(feature)
          }
        }
        sys.props.get("geomesa.rollup.fixture").foreach { path =>
          Files.copy(file, Paths.get(path + ".spatial"), StandardCopyOption.REPLACE_EXISTING)
        }
        WithClose(ParquetFileReader.open(new LocalInputFile(file))) { reader =>
          val metadata = reader.getFooter.getFileMetaData.getKeyValueMetaData
          metadata.containsKey("geo") must beTrue
          metadata.containsKey(VisibilityMetadata.MetadataKey) must beTrue
          reader.getFooter.getBlocks.asScala.map(_.getRowCount).sum mustEqual 6L
        }
      } finally {
        Files.deleteIfExists(file)
      }
    }

    "describe exactly the rows in each physical file and preserve existing metadata" >> {
      foreach(Seq(Seq("A", "A&B", "C|D", "A", "", null), Seq("A", "A&B", "C|D", "A"), Seq("B"))) { expressions =>
        val file = Files.createTempFile("geomesa-rollup", ".parquet")
        try {
          WithClose(ParquetFileSystemWriter(sft, conf, LocalObjectStore, file.toUri.toString)) { writer =>
            expressions.zipWithIndex.foreach { case (vis, i) =>
              val feature = ScalaSimpleFeature.create(sft, i.toString, s"row-$i")
              if (vis != null) feature.getUserData.put("geomesa.feature.visibility", vis)
              writer.write(feature)
            }
          }
          // Optional fixture export for the Java reader's cross-module interoperability tests.
          if (expressions.size == 6) {
            sys.props.get("geomesa.rollup.fixture").foreach { path =>
              Files.copy(file, Paths.get(path), StandardCopyOption.REPLACE_EXISTING)
              val deleteSchema = MessageTypeParser.parseMessageType(
                s"""message deletes {
                   |  required binary file_path (STRING) = ${MetadataColumns.DELETE_FILE_PATH.fieldId()};
                   |  required int64 pos = ${MetadataColumns.DELETE_FILE_POS.fieldId()};
                   |}""".stripMargin)
              val deleteWriter = ExampleParquetWriter.builder(new LocalOutputFile(Paths.get(path + ".deletes")))
                  .withConf(new PlainParquetConfiguration()).withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                  .withType(deleteSchema).build()
              WithClose(deleteWriter) { writer =>
                writer.write(new SimpleGroupFactory(deleteSchema).newGroup()
                    .append("file_path", "memory:///mixed.parquet").append("pos", 0L))
              }
            }
          }
          if (expressions.size == 4) {
            sys.props.get("geomesa.rollup.fixture").foreach { path =>
              Files.copy(file, Paths.get(path + ".restricted"), StandardCopyOption.REPLACE_EXISTING)
            }
          }
          WithClose(ParquetFileReader.open(new LocalInputFile(file))) { reader =>
            val footer = reader.getFooter
            val metadata = footer.getFileMetaData.getKeyValueMetaData
            val summary = JsonParser.parseString(metadata.get(VisibilityMetadata.MetadataKey)).getAsJsonObject
            summary.get("rowCount").getAsLong mustEqual expressions.size.toLong
            summary.get("fieldId").getAsInt mustEqual 2
            summary.get("version").getAsInt mustEqual 1
            summary.get("scope").getAsString mustEqual "file"
            metadata.size() must beGreaterThan(1)
            val operands = expressions.filter(v => v != null && v.nonEmpty).distinct.map(v => s"($v)")
            summary.get("allOf").getAsString mustEqual AccessExpression.of(operands.mkString("&"), true).getExpression
            summary.get("anyOf").getAsString mustEqual
                (if (expressions.exists(v => v == null || v.isEmpty)) ""
                 else AccessExpression.of(operands.mkString("|"), true).getExpression)
          }
        } finally {
          Files.deleteIfExists(file)
        }
      }
    }

    "represent unrestricted files with an empty conjunction" >> {
      val observer = new VisibilityMetadata(2)
      Seq(null, "", "").foreach(observer.observe)
      val summary = JsonParser.parseString(observer.metadata.get).getAsJsonObject
      summary.get("rowCount").getAsLong mustEqual 3L
      summary.get("allOf").getAsString mustEqual ""
      summary.get("anyOf").getAsString mustEqual ""
    }

    "omit both proofs on malformed expressions even when unrestricted rows are present" >> {
      foreach(Seq("A&", " A ")) { expression =>
        val observer = new VisibilityMetadata(2)
        Seq("A", expression, "", null).foreach(observer.observe)
        observer.metadata must beNone
      }
    }

    "finish writing and preserve data when visibility is too deeply nested to summarize" >> {
      val file = Files.createTempFile("geomesa-deep-visibility", ".parquet")
      val expression = "(" * 10000 + "A" + ")" * 10000
      try {
        WithClose(ParquetFileSystemWriter(sft, conf, LocalObjectStore, file.toUri.toString)) { writer =>
          val feature = ScalaSimpleFeature.create(sft, "1", "deep")
          feature.getUserData.put("geomesa.feature.visibility", expression)
          writer.write(feature)
        }
        WithClose(ParquetFileReader.open(new LocalInputFile(file))) { reader =>
          reader.getFooter.getBlocks.asScala.map(_.getRowCount).sum mustEqual 1L
          reader.getFooter.getFileMetaData.getKeyValueMetaData.containsKey(VisibilityMetadata.MetadataKey) must beFalse
        }
        WithClose(ParquetFileSystemReader.builder(LocalObjectStore, file.toUri).build()) { reader =>
          reader.read().getUserData.get("geomesa.feature.visibility") mustEqual expression
        }
      } finally {
        Files.deleteIfExists(file)
      }
    }

    "bound structural nesting without rejecting parentheses in quoted labels" >> {
      val atLimit = new VisibilityMetadata(2)
      atLimit.observe("(" * (VisibilityMetadata.MaxDepth - 1) + "A" + ")" * (VisibilityMetadata.MaxDepth - 1))
      atLimit.metadata must beSome
      val overLimit = new VisibilityMetadata(2)
      overLimit.observe("(" * VisibilityMetadata.MaxDepth + "A" + ")" * VisibilityMetadata.MaxDepth)
      overLimit.metadata must beNone
      val quoted = new VisibilityMetadata(2)
      quoted.observe(AccessExpression.quote("A\"" + "(" * 1000 + "A\\"))
      quoted.metadata must beSome
    }

    "preserve all-row and any-row semantics including quoted and escaped labels" >> {
      val labels = Seq("A", "B", "C", "A B", "A\"B", "A\\B", "A&B", "\u20ac")
      val expressions = Seq("A|(B&C)", "B|A") ++ labels.drop(3).map(AccessExpression.quote)
      val observer = new VisibilityMetadata(2)
      expressions.foreach(observer.observe)
      val summary = JsonParser.parseString(observer.metadata.get).getAsJsonObject
      foreach(0 until (1 << labels.size)) { mask =>
        val auths = labels.zipWithIndex.collect { case (label, i) if (mask & (1 << i)) != 0 => label }
        val evaluator = AccessEvaluator.of(Authorizations.of(auths.asJava))
        evaluator.canAccess(summary.get("allOf").getAsString) mustEqual expressions.forall(evaluator.canAccess)
        evaluator.canAccess(summary.get("anyOf").getAsString) mustEqual expressions.exists(evaluator.canAccess)
      }
    }

    "retain summaries at the distinct expression bound even with repeated rows" >> {
      val observer = new VisibilityMetadata(2)
      val expressions = (0 until VisibilityMetadata.MaxExpressions).map(i => s"A$i")
      expressions.foreach(observer.observe)
      expressions.foreach(observer.observe)
      val summary = JsonParser.parseString(observer.metadata.get).getAsJsonObject
      summary.get("rowCount").getAsLong mustEqual 2L * VisibilityMetadata.MaxExpressions
      summary.get("allOf").getAsString mustEqual
          AccessExpression.of(expressions.map(e => s"($e)").mkString("&"), true).getExpression
      observer.observe("B")
      observer.metadata must beNone
    }

    "count repeated expression bytes only once" >> {
      val observer = new VisibilityMetadata(2)
      val expression = "A" * 1000
      (0 until 1000).foreach(_ => observer.observe(expression))
      val summary = JsonParser.parseString(observer.metadata.get).getAsJsonObject
      summary.get("rowCount").getAsLong mustEqual 1000L
      summary.get("allOf").getAsString mustEqual expression
      summary.get("anyOf").getAsString mustEqual expression
    }

    "discard complete summaries when distinct expressions exceed the bound" >> {
      val observer = new VisibilityMetadata(2)
      (0 to VisibilityMetadata.MaxExpressions).foreach(i => observer.observe(s"A$i"))
      observer.observe(null)
      observer.metadata must beNone
    }

    "bound encoded metadata including JSON escaping and UTF-8" >> {
      foreach(Seq("A" * VisibilityMetadata.MaxBytes, "\u0001" * 12000, "\u20ac" * 23000)) { expression =>
        val observer = new VisibilityMetadata(2)
        observer.observe(expression)
        observer.metadata must beNone
      }
    }

    "accept a zero-based physical field identity" >> {
      val observer = new VisibilityMetadata(0)
      observer.observe("A")
      val summary = JsonParser.parseString(observer.metadata.get).getAsJsonObject
      summary.get("fieldId").getAsInt mustEqual 0
      summary.get("allOf").getAsString mustEqual "A"
      summary.get("anyOf").getAsString mustEqual "A"
    }

    "omit summaries without a physical field identity" >> {
      new VisibilityMetadata(-1).metadata must beNone
    }
  }
}
