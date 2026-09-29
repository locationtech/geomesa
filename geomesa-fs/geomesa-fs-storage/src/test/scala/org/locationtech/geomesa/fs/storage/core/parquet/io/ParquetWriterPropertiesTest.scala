/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core.parquet.io

import org.apache.parquet.column.Encoding
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.metadata.{ColumnChunkMetaData, CompressionCodecName}
import org.apache.parquet.io.LocalInputFile
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.fs.storage.core.fs.LocalObjectStore
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.specs2.mutable.SpecificationWithJUnit

import java.nio.file.Files

class ParquetWriterPropertiesTest extends SpecificationWithJUnit {

  import scala.collection.JavaConverters._

  private val sft = SimpleFeatureTypes.createType("props", "category:String,label:String,text:String,age:Int,dtg:Date,*geom:Point:srid=4326")

  // category and label have few distinct values, so are dictionary encoded by default; text is large enough to span pages
  private val features = Seq.tabulate(20000) { i =>
    val text = s"text-$i-" + Integer.toHexString(i * 2654435761L.toInt) * 8
    ScalaSimpleFeature.create(sft, s"$i", s"category${i % 10}", s"label${i % 20}", text, i, "2017-01-01T00:00:00Z", "POINT (1 1)")
  }

  private case class Written(columns: Map[String, ColumnChunkMetaData], pages: Map[String, Int])

  private def write(conf: Map[String, String]): Written = {
    val file = Files.createTempFile("geomesa-props", ".parquet")
    try {
      WithClose(ParquetFileSystemWriter(sft, conf, LocalObjectStore, s"file://$file"))(w => features.foreach(w.write))
      WithClose(ParquetFileReader.open(new LocalInputFile(file))) { reader =>
        val blocks = reader.getFooter.getBlocks.asScala
        blocks must haveLength(1)
        val columns = blocks.head.getColumns.asScala.map(c => c.getPath.toDotString -> c).toMap
        val pages = columns.map { case (path, c) => path -> Option(reader.readOffsetIndex(c)).fold(0)(_.getPageCount) }
        Written(columns, pages)
      }
    } finally {
      Files.deleteIfExists(file)
    }
  }

  private def dictionary(c: ColumnChunkMetaData): Boolean = c.getEncodings.asScala.exists(_.usesDictionary())

  "ParquetFileSystemWriter properties" should {

    "use parquet's defaults when not configured" >> {
      val written = write(Map.empty)
      dictionary(written.columns("category")) must beTrue
      dictionary(written.columns("label")) must beTrue
      written.columns("category").getCodec mustEqual CompressionCodecName.ZSTD
      written.columns("age").getStatistics.isEmpty must beFalse
    }

    "disable dictionary encoding globally and per column" >> {
      val global = write(Map("parquet.enable.dictionary" -> "false"))
      dictionary(global.columns("category")) must beFalse
      dictionary(global.columns("label")) must beFalse

      val column = write(Map("parquet.enable.dictionary#category" -> "false"))
      dictionary(column.columns("category")) must beFalse
      column.columns("category").getEncodings.asScala must not(contain(Encoding.RLE_DICTIONARY))
      dictionary(column.columns("label")) must beTrue
    }

    "set the page size" >> {
      val default = write(Map.empty)
      val small = write(Map("parquet.page.size" -> "4k"))
      small.pages("text") must beGreaterThan(default.pages("text"))
    }

    "set the page row count limit" >> {
      write(Map("parquet.page.row.count.limit" -> "1000")).pages("age") must beGreaterThanOrEqualTo(20)
    }

    "set compression per column" >> {
      val written = write(Map("parquet.compression#label" -> "SNAPPY"))
      written.columns("label").getCodec mustEqual CompressionCodecName.SNAPPY
      written.columns("category").getCodec mustEqual CompressionCodecName.ZSTD
    }

    "disable statistics per column" >> {
      val written = write(Map("parquet.column.statistics.enabled#age" -> "false"))
      written.columns("age").getStatistics.isEmpty must beTrue
      written.columns("category").getStatistics.isEmpty must beFalse
    }

    "ignore invalid values" >> {
      val written = write(Map("parquet.enable.dictionary" -> "no", "parquet.page.size" -> "lots", "parquet.compression#label" -> "foo"))
      dictionary(written.columns("category")) must beTrue
      written.columns("label").getCodec mustEqual CompressionCodecName.ZSTD
    }
  }
}
