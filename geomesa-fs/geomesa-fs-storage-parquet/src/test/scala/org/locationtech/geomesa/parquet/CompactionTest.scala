/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.parquet

import java.nio.file.Files
import java.text.SimpleDateFormat

import com.vividsolutions.jts.geom.Coordinate
import org.apache.commons.io.FileUtils
import org.geotools.data.Query
import org.geotools.factory.CommonFactoryFinder
import org.geotools.geometry.jts.JTSFactoryFinder
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.fs.storage.common._
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeature
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.specs2.specification.AllExpectations

import scala.collection.JavaConversions._


@RunWith(classOf[JUnitRunner])
class CompactionTest extends Specification with AllExpectations {

  sequential

  "ParquetFileSystemStorage" should {
    val gf = JTSFactoryFinder.getGeometryFactory
    val sft = SimpleFeatureTypes.createType("test", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")
    val ff = CommonFactoryFinder.getFilterFactory2
    val tempDir = Files.createTempDirectory("geomesa")
    println(tempDir)

    "compact partitions" >> {
      val parquetFactory = new ParquetFileSystemStorageFactory

      val fsStorage = parquetFactory.build(Map(
        "fs.path" -> tempDir.toFile.getPath,
        "parquet.compression" -> "gzip"
      ))

      val scheme = CommonSchemeLoader.build("daily", sft)
      PartitionScheme.addToSft(sft, scheme)
      fsStorage.createNewFeatureType(sft, scheme)

      fsStorage.listFeatureTypes().size mustEqual 1
      fsStorage.listFeatureTypes().head.getTypeName mustEqual "test"

      val sdf = new SimpleDateFormat("yyyy-MM-dd")
      val dtg = sdf.parse("2017-01-01")
      val sf1 = new ScalaSimpleFeature(sft, "1", Array("first", Integer.valueOf(100), dtg, gf.createPoint(new Coordinate(10, 10))))
      val partition = scheme.getPartitionName(sf1)

      def write(sf: SimpleFeature) = {
        val writer = fsStorage.getWriter(sft.getTypeName, partition)
        writer.write(sf)
        writer.close()
      }

      // First simple feature goes in its own file
      write(sf1)
      fsStorage.getPaths(sft.getTypeName, partition) must haveSize(1)
      fsStorage.getPartitionReader(sft, Query.ALL, partition).toList must haveSize(1)

      // Second simple feature should be in a separate file
      val sf2 = new ScalaSimpleFeature(sft, "2", Array("second", Integer.valueOf(200), dtg, gf.createPoint(new Coordinate(10, 10))))
      write(sf2)
      fsStorage.getPaths(sft.getTypeName, partition) must haveSize(2)
      fsStorage.getPartitionReader(sft, Query.ALL, partition).toList must haveSize(2)

      // Third feature in a third file
      val sf3 = new ScalaSimpleFeature(sft, "3", Array("third", Integer.valueOf(300), dtg, gf.createPoint(new Coordinate(10, 10))))
      write(sf3)
      fsStorage.getPaths(sft.getTypeName, partition) must haveSize(3)
      fsStorage.getPartitionReader(sft, Query.ALL, partition).toList must haveSize(3)

      // Compact to create a single file
      fsStorage.compact(sft.getTypeName, partition)
      fsStorage.getPaths(sft.getTypeName, partition) must haveSize(1)
      fsStorage.getPartitionReader(sft, Query.ALL, partition).toList must haveSize(3)

    }

    step {
      FileUtils.deleteDirectory(tempDir.toFile)
    }
  }
}
