/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core.parquet.io

import org.apache.parquet.column.values.bloomfilter.BloomFilter
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.io.LocalInputFile
import org.apache.parquet.io.api.Binary
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.fs.storage.core.fs.LocalObjectStore
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.specs2.mutable.SpecificationWithJUnit

import java.nio.file.Files

class ParquetBloomFilterTest extends SpecificationWithJUnit {

  import scala.collection.JavaConverters._

  private val sft = SimpleFeatureTypes.createType("blooms", "key:String,code:String,category:String,age:Int,dtg:Date,*geom:Point:srid=4326")

  private val features = Seq.tabulate(5000) { i =>
    ScalaSimpleFeature.create(sft, s"$i", f"key-$i%05d", s"code$i", s"category${i % 10}", i,
      f"2017-01-${i % 28 + 1}%02dT00:00:00Z", s"POINT (${i % 180} ${i % 90})")
  }

  /**
   * Writes the test features and returns the bloom filter of each column that has one, by column path
   */
  private def blooms(conf: Map[String, String]): Map[String, BloomFilter] = {
    val file = Files.createTempFile("geomesa-blooms", ".parquet")
    try {
      WithClose(ParquetFileSystemWriter(sft, conf, LocalObjectStore, s"file://$file")) { writer =>
        features.foreach(writer.write)
      }
      WithClose(ParquetFileReader.open(new LocalInputFile(file))) { reader =>
        val blocks = reader.getFooter.getBlocks.asScala
        blocks must haveLength(1)
        blocks.head.getColumns.asScala.flatMap { col =>
          Option(reader.readBloomFilter(col)).map(col.getPath.toDotString -> _)
        }.toMap
      }
    } finally {
      Files.deleteIfExists(file)
    }
  }

  private def contains(bloom: BloomFilter, value: String): Boolean =
    bloom.findHash(bloom.hash(Binary.fromString(value)))

  "ParquetFileSystemWriter bloom filters" should {

    "not write bloom filters by default" >> {
      blooms(Map.empty) must beEmpty
    }

    "write bloom filters only for the configured columns" >> {
      val result = blooms(Map("parquet.bloom.filter.enabled#key" -> "true", "parquet.bloom.filter.enabled#code" -> "true"))
      result.keySet mustEqual Set("key", "code")
      forall(Seq("key-00000", "key-04999"))(v => contains(result("key"), v) must beTrue)
      contains(result("code"), "code4999") must beTrue
      // the default fpp is 1%, so allow some slack
      val absent = Seq.tabulate(1000)(i => f"absent-$i%05d").count(contains(result("key"), _))
      absent must beLessThan(50)
    }

    "enable bloom filters globally and disable per column" >> {
      val result = blooms(Map("parquet.bloom.filter.enabled" -> "true", "parquet.bloom.filter.enabled#code" -> "false"))
      result.keySet must contain("key")
      result.keySet must not(contain("code"))
    }

    "not write bloom filters for columns that are entirely dictionary encoded" >> {
      // parquet skips these, as the dictionary already answers membership
      blooms(Map("parquet.bloom.filter.enabled#category" -> "true")) must beEmpty
    }

    "size bloom filters from the expected ndv, and the max bytes otherwise" >> {
      val default = blooms(Map("parquet.bloom.filter.enabled#key" -> "true"))("key")
      val ndv = blooms(Map("parquet.bloom.filter.enabled#key" -> "true", "parquet.bloom.filter.expected.ndv#key" -> "5000"))("key")
      val capped = blooms(Map("parquet.bloom.filter.enabled#key" -> "true", "parquet.bloom.filter.max.bytes" -> "64k"))("key")
      default.getBitsetSize mustEqual 1024 * 1024
      ndv.getBitsetSize must beLessThan(16 * 1024)
      capped.getBitsetSize mustEqual 64 * 1024
      contains(ndv, "key-00500") must beTrue
    }

    "size bloom filters adaptively" >> {
      val conf = Map("parquet.bloom.filter.enabled#key" -> "true", "parquet.bloom.filter.adaptive.enabled" -> "true")
      // candidates halve from the max bytes, so the smallest of the default 5 is 1MB / 16
      val adaptive = blooms(conf)("key")
      val more = blooms(conf ++ Map("parquet.bloom.filter.candidates.number#key" -> "8"))("key")
      adaptive.getBitsetSize mustEqual 64 * 1024
      more.getBitsetSize must beLessThan(64 * 1024)
      contains(adaptive, "key-00500") must beTrue
    }

    "ignore invalid values" >> {
      val result = blooms(Map(
        "parquet.bloom.filter.enabled#key" -> "true",
        "parquet.bloom.filter.enabled#code" -> "yes",
        "parquet.bloom.filter.expected.ndv#key" -> "lots",
        "parquet.bloom.filter.fpp#key" -> "2",
      ))
      result.keySet mustEqual Set("key")
      result("key").getBitsetSize mustEqual 1024 * 1024
    }

    "translate iceberg table properties" >> {
      val props = Map(
        "write.parquet.bloom-filter-enabled.column.key" -> "true",
        "write.parquet.bloom-filter-ndv.column.key" -> "1000",
        "write.parquet.bloom-filter-fpp.column.key" -> "0.05",
        "write.parquet.bloom-filter-max-bytes" -> "65536",
        "write.parquet.compression-codec" -> "zstd",
        "geomesa.sft.name" -> "blooms",
      )
      ParquetFileSystemWriter.icebergBloomFilterConf(props.asJava) mustEqual Map(
        "parquet.bloom.filter.enabled#key" -> "true",
        "parquet.bloom.filter.expected.ndv#key" -> "1000",
        "parquet.bloom.filter.fpp#key" -> "0.05",
        "parquet.bloom.filter.max.bytes" -> "65536",
      )
    }
  }
}
