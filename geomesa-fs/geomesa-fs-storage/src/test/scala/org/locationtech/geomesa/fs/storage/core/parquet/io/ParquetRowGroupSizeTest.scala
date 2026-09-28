/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core.parquet.io

import org.apache.parquet.conf.PlainParquetConfiguration
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.io.LocalInputFile
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.fs.storage.core.FileSystemStorage.{ParquetRowGroupSizeDefault, ParquetRowGroupSizeOpt}
import org.locationtech.geomesa.fs.storage.core.fs.LocalObjectStore
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.specs2.mutable.SpecificationWithJUnit

import java.nio.file.{Files, Path}

class ParquetRowGroupSizeTest extends SpecificationWithJUnit {

  import scala.collection.JavaConverters._

  private val sft = SimpleFeatureTypes.createType("rowgroups", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")

  // enough varied data (~5MB uncompressed) to span several small row groups, but one default-sized one
  private val features = Seq.tabulate(20000) { i =>
    val name = s"feature-$i-" + Integer.toHexString(i * 2654435761L.toInt) * 12
    ScalaSimpleFeature.create(sft, s"$i", name, i, f"2017-01-${i % 28 + 1}%02dT00:00:00Z", s"POINT (${i % 180} ${i % 90})")
  }

  private def rowGroups(conf: Map[String, String]): Int = {
    val file = Files.createTempFile("geomesa-rowgroups", ".parquet")
    try {
      WithClose(ParquetFileSystemWriter(sft, conf, LocalObjectStore, s"file://$file")) { writer =>
        features.foreach(writer.write)
      }
      WithClose(ParquetFileReader.open(new LocalInputFile(file)))(_.getFooter.getBlocks.size)
    } finally {
      Files.deleteIfExists(file)
    }
  }

  private def configured(value: String): Long =
    ParquetFileSystemWriter.rowGroupSize(new PlainParquetConfiguration(Map(ParquetRowGroupSizeOpt -> value).asJava))

  "ParquetFileSystemWriter row group size" should {

    "default to 8MB" >> {
      ParquetFileSystemWriter.rowGroupSize(new PlainParquetConfiguration()) mustEqual ParquetRowGroupSizeDefault
      ParquetRowGroupSizeDefault mustEqual 8L * 1024 * 1024
    }

    "accept bytes and sizes with suffixes" >> {
      configured("1048576") mustEqual 1024L * 1024
      configured("128MB") mustEqual 128L * 1024 * 1024
      configured(" 64k ") mustEqual 64L * 1024
    }

    "fall back to the default for invalid values" >> {
      configured("lots") mustEqual ParquetRowGroupSizeDefault
      configured("") mustEqual ParquetRowGroupSizeDefault
    }

    "write fewer, larger row groups when configured larger" >> {
      val small = rowGroups(Map(ParquetRowGroupSizeOpt -> "256k"))
      val default = rowGroups(Map.empty)
      small must beGreaterThan(1)
      default mustEqual 1
    }
  }
}
