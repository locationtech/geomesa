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
import org.geotools.geometry.jts.JTSFactoryFinder
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.fs.storage.common.DateTimeZ2Scheme
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeature
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.specs2.specification.AllExpectations

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class ParquetFSTest extends Specification with AllExpectations {

  sequential

  "ParquetFileSystemStorage" should {

    val gf = JTSFactoryFinder.getGeometryFactory
    val sft = SimpleFeatureTypes.createType("test", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")
    val ff = CommonFactoryFinder.getFilterFactory2

    val tempDir = Files.createTempDirectory("geomesa")


    "create an fs" >> {
      val parquetFactory = new ParquetFileSystemStorageFactory

      val fsStorage = parquetFactory.build(Map(
        "fs.path" -> tempDir.toFile.getPath
      ))

      val scheme = new DateTimeZ2Scheme("yyyy/DDD/HH", ChronoUnit.HOURS, 1, 10, sft, "dtg", "geom")
      fsStorage.createNewFeatureType(sft, scheme)

      fsStorage.listFeatureTypes().size mustEqual 1
      fsStorage.listFeatureTypes().head.getTypeName mustEqual "test"

      val sf1 = new ScalaSimpleFeature("1", sft, Array("first", Integer.valueOf(100), new java.util.Date, gf.createPoint(new Coordinate(25.236263, 27.436734))))
      val sf2 = new ScalaSimpleFeature("2", sft, Array(null, Integer.valueOf(200), new java.util.Date, gf.createPoint(new Coordinate(67.2363, 55.236))))
      val sf3 = new ScalaSimpleFeature("3", sft, Array("third", Integer.valueOf(300), new java.util.Date, gf.createPoint(new Coordinate(73.0, 73.0))))

      val partitionSchema = fsStorage.getPartitionScheme(sft.getTypeName)
      val partitions = List(sf1, sf2, sf3).map(partitionSchema.getPartitionName)
      List[SimpleFeature](sf1, sf2, sf3)
        .zip(partitions)
        .groupBy(_._2)
        .foreach { case (partition, features) => val writer = fsStorage.getWriter(sft.getTypeName, fsStorage.getPartition(partition))
          features.map(_._1).foreach(writer.write)
          writer.close()
        }

      val reader3 = fsStorage.getPartitionReader(new Query("test", ff.equals(ff.property("name"), ff.literal("third"))), fsStorage.getPartition(partitions(2)))
      val features3 = reader3.toList
      features3.size mustEqual 1
      features3.head.getDefaultGeometry.asInstanceOf[Point].getX mustEqual 73.0
      reader3.close()

      val reader1 = fsStorage.getPartitionReader(new Query("test", ff.equals(ff.property("name"), ff.literal("first"))), fsStorage.getPartition(partitions(0)))
      val features1 = reader1.toList
      features1.size mustEqual 1
      features1.head.getDefaultGeometry.asInstanceOf[Point].getX mustEqual 25.236263
      reader1.close()

      success
    }

    step {
      FileUtils.deleteDirectory(tempDir.toFile)
    }

  }
}
