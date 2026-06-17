/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core.iceberg

import org.apache.commons.io.FileUtils
import org.apache.iceberg.expressions.Expressions
import org.apache.iceberg.transforms.Transform
import org.apache.iceberg.types.Types
import org.calrissian.mango.types.LexiTypeEncoders
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.fs.storage.core.StorageMetadata.Z2Encoder
import org.locationtech.geomesa.fs.storage.core.metadata.FileBasedMetadataCatalog
import org.locationtech.geomesa.fs.storage.core.parquet.ParquetFileSystemStorageFactory
import org.locationtech.geomesa.fs.storage.core.{FileSystemContext, FileSystemStorage, Partition}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.jts.geom.Point
import org.specs2.mutable.SpecificationWithJUnit

import java.io.File
import java.nio.file.Files
import java.util.Date

class IcebergMapperTest extends SpecificationWithJUnit {

  import scala.collection.JavaConverters._

  val sft = SimpleFeatureTypes.createType("test", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")
  val sf = ScalaSimpleFeature.create(sft, "", "goodbye", "11", "2026-05-06T11:12:13", "POINT (10 10)")
  val dtg = sf.getAttribute("dtg").asInstanceOf[Date].getTime * 1000 // in microseconds

  // note: we compare expressions as strings as column refs don't implement 'equals'

  "IcebergMapper" should {
    "map string attributes" in {
      withStorage(Seq("attribute:attribute=name")) { storage =>
        val partition = storage.metadata.schemes.head.getPartition(sf)
        partition.value mustEqual "goodbye"
        val mapper = IcebergMapper(storage.metadata.sft, storage.metadata.schemes.toSeq.sortBy(_.name), storage.context)
        val fields = mapper.spec.fields().asScala
        fields must haveLength(1)
        fields.head.name() mustEqual "name"
        fields.head.transform().isIdentity must beTrue
        fields.head.transform().asInstanceOf[Transform[String, String]].bind(Types.StringType.get()).apply("goodbye") mustEqual "goodbye"
        mapper.expression(Partition(Set(partition))).toString mustEqual Expressions.equal("name", "goodbye").toString
      }
    }
    "map string attributes with width" in {
      withStorage(Seq("attribute:attribute=name:width=4")) { storage =>
        val partition = storage.metadata.schemes.head.getPartition(sf)
        partition.value mustEqual "good"
        val mapper = IcebergMapper(storage.metadata.sft, storage.metadata.schemes.toSeq.sortBy(_.name), storage.context)
        val fields = mapper.spec.fields().asScala
        fields must haveLength(1)
        fields.head.name() must startWith("name")
        fields.head.transform().toString mustEqual "truncate[4]"
        fields.head.transform().asInstanceOf[Transform[String, String]].bind(Types.StringType.get()).apply("goodbye") mustEqual "good"
        mapper.expression(Partition(Set(partition))).toString mustEqual
          Expressions.equal(Expressions.truncate[String]("name", 4), "good").toString
      }
    }
    "map int attributes" in {
      withStorage(Seq("attribute:attribute=age")) { storage =>
        val partition = storage.metadata.schemes.head.getPartition(sf)
        partition.value mustEqual LexiTypeEncoders.integerEncoder().encode(11)
        val mapper = IcebergMapper(storage.metadata.sft, storage.metadata.schemes.toSeq.sortBy(_.name), storage.context)
        val fields = mapper.spec.fields().asScala
        fields must haveLength(1)
        fields.head.name() mustEqual "age"
        fields.head.transform().isIdentity must beTrue
        fields.head.transform().asInstanceOf[Transform[Int, Int]].bind(Types.IntegerType.get()).apply(11) mustEqual 11
        mapper.expression(Partition(Set(partition))).toString mustEqual Expressions.equal("age", 11).toString
      }
    }
    "map int attributes with divisor" in {
      withStorage(Seq("attribute:attribute=age:divisor=10")) { storage =>
        val partition = storage.metadata.schemes.head.getPartition(sf)
        partition.value mustEqual LexiTypeEncoders.integerEncoder().encode(10)
        val mapper = IcebergMapper(storage.metadata.sft, storage.metadata.schemes.toSeq.sortBy(_.name), storage.context)
        val fields = mapper.spec.fields().asScala
        fields must haveLength(1)
        fields.head.name() must startWith("age")
        fields.head.transform().toString mustEqual "truncate[10]"
        fields.head.transform().asInstanceOf[Transform[Int, Int]].bind(Types.IntegerType.get()).apply(11) mustEqual 10
        mapper.expression(Partition(Set(partition))).toString mustEqual
          Expressions.equal(Expressions.truncate[Integer]("age", 10), Int.box(10)).toString
      }
    }
    "map hour scheme" in {
      withStorage(Seq("hours")) { storage =>
        val partition = storage.metadata.schemes.head.getPartition(sf)
        val expected = LexiTypeEncoders.integerEncoder().decode(partition.value)
        val mapper = IcebergMapper(storage.metadata.sft, storage.metadata.schemes.toSeq.sortBy(_.name), storage.context)
        val fields = mapper.spec.fields().asScala
        fields must haveLength(1)
        fields.head.name() mustEqual "dtg_hour"
        fields.head.transform().asInstanceOf[Transform[Long, Int]].bind(Types.TimestampType.withoutZone()).apply(dtg) mustEqual expected
        mapper.expression(Partition(Set(partition))).toString mustEqual
          Expressions.equal(Expressions.hour[Integer]("dtg"), expected).toString
      }
    }
    "map day scheme" in {
      withStorage(Seq("days")) { storage =>
        val partition = storage.metadata.schemes.head.getPartition(sf)
        val expected = LexiTypeEncoders.integerEncoder().decode(partition.value)
        val mapper = IcebergMapper(storage.metadata.sft, storage.metadata.schemes.toSeq.sortBy(_.name), storage.context)
        val fields = mapper.spec.fields().asScala
        fields must haveLength(1)
        fields.head.name() mustEqual "dtg_day"
        fields.head.transform().asInstanceOf[Transform[Long, Int]].bind(Types.TimestampType.withoutZone()).apply(dtg) mustEqual expected
        mapper.expression(Partition(Set(partition))).toString mustEqual
          Expressions.equal(Expressions.day[Integer]("dtg"), expected).toString
      }
    }
    "map month scheme" in {
      withStorage(Seq("months")) { storage =>
        val partition = storage.metadata.schemes.head.getPartition(sf)
        val expected = LexiTypeEncoders.integerEncoder().decode(partition.value)
        val mapper = IcebergMapper(storage.metadata.sft, storage.metadata.schemes.toSeq.sortBy(_.name), storage.context)
        val fields = mapper.spec.fields().asScala
        fields must haveLength(1)
        fields.head.name() mustEqual "dtg_month"
        fields.head.transform().asInstanceOf[Transform[Long, Int]].bind(Types.TimestampType.withoutZone()).apply(dtg) mustEqual expected
        mapper.expression(Partition(Set(partition))).toString mustEqual
          Expressions.equal(Expressions.month[Integer]("dtg"), expected).toString
      }
    }
    "map year scheme" in {
      withStorage(Seq("years")) { storage =>
        val partition = storage.metadata.schemes.head.getPartition(sf)
        val expected = LexiTypeEncoders.integerEncoder().decode(partition.value)
        val mapper = IcebergMapper(storage.metadata.sft, storage.metadata.schemes.toSeq.sortBy(_.name), storage.context)
        val fields = mapper.spec.fields().asScala
        fields must haveLength(1)
        fields.head.name() mustEqual "dtg_year"
        fields.head.transform().asInstanceOf[Transform[Long, Int]].bind(Types.TimestampType.withoutZone()).apply(dtg) mustEqual expected
        mapper.expression(Partition(Set(partition))).toString mustEqual
          Expressions.equal(Expressions.year[Integer]("dtg"), expected).toString
      }
    }
    "map z2 4/8-bit scheme" in {
      val fullZValue = Z2Encoder.encode(sf.getDefaultGeometry.asInstanceOf[Point])
      foreach(Seq(4, 8)) { bits =>
        withStorage(Seq(s"z2:bits=$bits")) { storage =>
          val partition = storage.metadata.schemes.head.getPartition(sf)
          val mapper = IcebergMapper(storage.metadata.sft, storage.metadata.schemes.toSeq.sortBy(_.name), storage.context)
          val fields = mapper.spec.fields().asScala
          fields must haveLength(1)
          fields.head.name() mustEqual "__geom_z2___trunc"
          mapper.expression(Partition(Set(partition))).toString mustEqual
            Expressions.equal("__geom_z2__", fullZValue.take(bits / 4)).toString
        }
      }
    }
    "not map unsupported schemas" in {
      foreach(Seq("hours:step=2", "weekly")) { unsupported =>
        withStorage(Seq(unsupported)) { storage =>
          IcebergMapper(storage.metadata.sft, storage.metadata.schemes.toSeq.sortBy(_.name), storage.context) must
            throwAn[UnsupportedOperationException]
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
