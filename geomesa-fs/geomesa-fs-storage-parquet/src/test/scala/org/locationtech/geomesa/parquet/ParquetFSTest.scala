/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/


package org.locationtech.geomesa.parquet

import java.nio.file.Files
import java.time.temporal.ChronoUnit

import com.vividsolutions.jts.geom.{Coordinate, Point}
import org.apache.commons.io.FileUtils
import org.geotools.data.Query
import org.geotools.factory.CommonFactoryFinder
import org.geotools.filter.text.ecql.ECQL
import org.geotools.geometry.jts.JTSFactoryFinder
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.fs.storage.common.{CompositeScheme, DateTimeScheme, PartitionScheme, Z2Scheme}
import org.locationtech.geomesa.index.planning.QueryPlanner
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.opengis.feature.simple.SimpleFeature
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.specs2.specification.AllExpectations

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class ParquetFSTest extends Specification with AllExpectations {

  sequential

  val gf = JTSFactoryFinder.getGeometryFactory
  val sft = SimpleFeatureTypes.createType("test", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")
  val ff = CommonFactoryFinder.getFilterFactory2

  val tempDir = Files.createTempDirectory("geomesa")

  val fsStorage = new ParquetFileSystemStorageFactory().build(Map(
    "fs.path" -> tempDir.toFile.getPath,
    "parquet.compression" -> "gzip"
  ))

  val sf1 = new ScalaSimpleFeature(sft, "1", Array("first", Integer.valueOf(100), new java.util.Date, gf.createPoint(new Coordinate(25.236263, 27.436734))))
  val sf2 = new ScalaSimpleFeature(sft, "2", Array(null, Integer.valueOf(200), new java.util.Date, gf.createPoint(new Coordinate(67.2363, 55.236))))
  val sf3 = new ScalaSimpleFeature(sft, "3", Array("third", Integer.valueOf(300), new java.util.Date, gf.createPoint(new Coordinate(73.0, 73.0))))

  "ParquetFileSystemStorage" should {
    "create an fs" >> {
      val scheme = new CompositeScheme(Seq(
        new DateTimeScheme("yyy/DDD/HH", ChronoUnit.HOURS, 1, "dtg", false),
        new Z2Scheme(10, "geom", false)
      ))
      PartitionScheme.addToSft(sft, scheme)
      fsStorage.createNewFeatureType(sft, scheme)

      fsStorage.listFeatureTypes().size mustEqual 1
      fsStorage.listFeatureTypes().head.getTypeName mustEqual "test"
    }

    "write and read features" >> {
      val partitionSchema = fsStorage.getPartitionScheme(sft.getTypeName)
      val partitions = List(sf1, sf2, sf3).map(partitionSchema.getPartitionName)
      List[SimpleFeature](sf1, sf2, sf3)
        .zip(partitions)
        .groupBy(_._2)
        .foreach { case (partition, features) =>
          val writer = fsStorage.getWriter(sft.getTypeName, partition)
          features.map(_._1).foreach(writer.write)
          writer.close()
        }

      WithClose(fsStorage.getPartitionReader(sft, new Query("test", ECQL.toFilter("name = 'first'")), partitions(0))) { reader =>
        val features = reader.toList
        features must haveSize(1)
        features.head.getAttribute("name") mustEqual "first"
        features.head.getAttribute("dtg") must not(beNull)
        features.head.getDefaultGeometry.asInstanceOf[Point].getX mustEqual 25.236263
        features.head.getDefaultGeometry.asInstanceOf[Point].getY mustEqual 27.436734
      }

      WithClose(fsStorage.getPartitionReader(sft, new Query("test", ECQL.toFilter("name = 'third'")), partitions(2))) { reader =>
        val features = reader.toList
        features must haveSize(1)
        features.head.getAttribute("name") mustEqual "third"
        features.head.getAttribute("dtg") must not(beNull)
        features.head.getDefaultGeometry.asInstanceOf[Point].getX mustEqual 73.0
        features.head.getDefaultGeometry.asInstanceOf[Point].getY mustEqual 73.0
      }

      val transform = new Query("test", ECQL.toFilter("name = 'third'"), Array("dtg", "geom"))
      QueryPlanner.setQueryTransforms(transform, sft)

      WithClose(fsStorage.getPartitionReader(sft, transform, partitions(2))) { reader =>
        val features = reader.toList
        features must haveSize(1)
        features.head.getFeatureType.getAttributeDescriptors.map(_.getLocalName) mustEqual Seq("dtg", "geom")
        features.head.getAttribute("name") must beNull
        features.head.getAttribute("dtg") must not(beNull)
        features.head.getDefaultGeometry.asInstanceOf[Point].getX mustEqual 73.0
        features.head.getDefaultGeometry.asInstanceOf[Point].getY mustEqual 73.0
      }
    }
  }

  step {
    FileUtils.deleteDirectory(tempDir.toFile)
  }
}
