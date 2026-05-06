/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.parquet.iceberg

import org.apache.commons.io.FileUtils
import org.apache.iceberg.transforms.Transform
import org.apache.iceberg.types.Types
import org.calrissian.mango.types.LexiTypeEncoders
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.fs.storage.core.metadata.FileBasedMetadataCatalog
import org.locationtech.geomesa.fs.storage.core.{FileSystemContext, FileSystemStorage, Partition}
import org.locationtech.geomesa.fs.storage.parquet.ParquetFileSystemStorageFactory
import org.locationtech.geomesa.fs.storage.parquet.iceberg.IcebergMapper
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.specs2.mutable.SpecificationWithJUnit

import java.io.File
import java.nio.file.Files
import java.util.Date

class IcebergMapperTest extends SpecificationWithJUnit {

  import scala.collection.JavaConverters._

  val sft = SimpleFeatureTypes.createType("test", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")
  val sf = ScalaSimpleFeature.create(sft, "", "goodbye", "11", "2026-05-06T11:12:13", "POINT (0 0)")
  val dtg = sf.getAttribute("dtg").asInstanceOf[Date].getTime * 1000 // in microseconds

  "IcebergMapper" should {
    "map string attributes" in {
      withStorage(Seq("attribute:attribute=name")) { storage =>
        val partition = storage.metadata.schemes.head.getPartition(sf)
        partition.value mustEqual "goodbye"
        val mapper = new IcebergMapper(storage)
        val fields = mapper.spec.fields().asScala
        fields must haveLength(1)
        fields.head.name() mustEqual "name"
        fields.head.transform().isIdentity must beTrue
        fields.head.transform().asInstanceOf[Transform[String, String]].bind(Types.StringType.get()).apply("goodbye") mustEqual "goodbye"
        mapper.partitionValues(Partition(Set(partition))) mustEqual Seq("goodbye")
      }
    }
    "map string attributes with width" in {
      withStorage(Seq("attribute:attribute=name:width=4")) { storage =>
        val partition = storage.metadata.schemes.head.getPartition(sf)
        partition.value mustEqual "good"
        val mapper = new IcebergMapper(storage)
        val fields = mapper.spec.fields().asScala
        fields must haveLength(1)
        fields.head.name() must startWith("name")
        fields.head.transform().toString mustEqual "truncate[4]"
        fields.head.transform().asInstanceOf[Transform[String, String]].bind(Types.StringType.get()).apply("goodbye") mustEqual "good"
        mapper.partitionValues(Partition(Set(partition))) mustEqual Seq("good")
      }
    }
    "map int attributes" in {
      withStorage(Seq("attribute:attribute=age")) { storage =>
        val partition = storage.metadata.schemes.head.getPartition(sf)
        partition.value mustEqual LexiTypeEncoders.integerEncoder().encode(11)
        val mapper = new IcebergMapper(storage)
        val fields = mapper.spec.fields().asScala
        fields must haveLength(1)
        fields.head.name() mustEqual "age"
        fields.head.transform().isIdentity must beTrue
        fields.head.transform().asInstanceOf[Transform[Int, Int]].bind(Types.IntegerType.get()).apply(11) mustEqual 11
        mapper.partitionValues(Partition(Set(partition))) mustEqual Seq("11")
      }
    }
    "map int attributes with divisor" in {
      withStorage(Seq("attribute:attribute=age:divisor=10")) { storage =>
        val partition = storage.metadata.schemes.head.getPartition(sf)
        partition.value mustEqual LexiTypeEncoders.integerEncoder().encode(10)
        val mapper = new IcebergMapper(storage)
        val fields = mapper.spec.fields().asScala
        fields must haveLength(1)
        fields.head.name() must startWith("age")
        fields.head.transform().toString mustEqual "truncate[10]"
        fields.head.transform().asInstanceOf[Transform[Int, Int]].bind(Types.IntegerType.get()).apply(11) mustEqual 10
        mapper.partitionValues(Partition(Set(partition))) mustEqual Seq("10")
      }
    }
    "map hour scheme" in {
      withStorage(Seq("hours")) { storage =>
        val partition = storage.metadata.schemes.head.getPartition(sf)
        val expected = LexiTypeEncoders.integerEncoder().decode(partition.value)
        val mapper = new IcebergMapper(storage)
        val fields = mapper.spec.fields().asScala
        fields must haveLength(1)
        fields.head.name() mustEqual "dtg_hour"
        fields.head.transform().asInstanceOf[Transform[Long, Int]].bind(Types.TimestampType.withoutZone()).apply(dtg) mustEqual expected
        mapper.partitionValues(Partition(Set(partition))) mustEqual Seq(expected.toString)
      }
    }
    "map day scheme" in {
      withStorage(Seq("days")) { storage =>
        val partition = storage.metadata.schemes.head.getPartition(sf)
        val expected = LexiTypeEncoders.integerEncoder().decode(partition.value)
        val mapper = new IcebergMapper(storage)
        val fields = mapper.spec.fields().asScala
        fields must haveLength(1)
        fields.head.name() mustEqual "dtg_day"
        fields.head.transform().asInstanceOf[Transform[Long, Int]].bind(Types.TimestampType.withoutZone()).apply(dtg) mustEqual expected
        mapper.partitionValues(Partition(Set(partition))) mustEqual Seq(expected.toString)
      }
    }
    "map month scheme" in {
      withStorage(Seq("months")) { storage =>
        val partition = storage.metadata.schemes.head.getPartition(sf)
        val expected = LexiTypeEncoders.integerEncoder().decode(partition.value)
        val mapper = new IcebergMapper(storage)
        val fields = mapper.spec.fields().asScala
        fields must haveLength(1)
        fields.head.name() mustEqual "dtg_month"
        fields.head.transform().asInstanceOf[Transform[Long, Int]].bind(Types.TimestampType.withoutZone()).apply(dtg) mustEqual expected
        mapper.partitionValues(Partition(Set(partition))) mustEqual Seq(expected.toString)
      }
    }
    "map year scheme" in {
      withStorage(Seq("years")) { storage =>
        val partition = storage.metadata.schemes.head.getPartition(sf)
        val expected = LexiTypeEncoders.integerEncoder().decode(partition.value)
        val mapper = new IcebergMapper(storage)
        val fields = mapper.spec.fields().asScala
        fields must haveLength(1)
        fields.head.name() mustEqual "dtg_year"
        fields.head.transform().asInstanceOf[Transform[Long, Int]].bind(Types.TimestampType.withoutZone()).apply(dtg) mustEqual expected
        mapper.partitionValues(Partition(Set(partition))) mustEqual Seq(expected.toString)
      }
    }
    "not map unsupported schemas" in {
      foreach(Seq("hours:step=2", "z2:bits=2")) { unsupported =>
        withStorage(Seq(unsupported)) { storage =>
          val mapper = new IcebergMapper(storage)
          mapper.spec.fields().asScala must beEmpty
        }
      }
    }
  }


  def withStorage[R](schemes: Seq[String])(code: FileSystemStorage => R): R = {
    val file = Files.createTempDirectory("gm-parquet-test").toUri
    try {
      val context = FileSystemContext.create(file, Map("fs.metadta.type" -> "file"))
      val metadata = new FileBasedMetadataCatalog(context).create(sft, schemes)
      WithClose(new ParquetFileSystemStorageFactory().apply(context, metadata))(code.apply)
    } finally {
      FileUtils.deleteDirectory(new File(file))
    }
  }
}
