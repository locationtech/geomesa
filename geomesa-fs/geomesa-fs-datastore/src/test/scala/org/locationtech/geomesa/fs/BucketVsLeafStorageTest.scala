/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs

import java.nio.file.Files
import java.util.Date
import java.util.stream.Collectors

import org.apache.commons.io.FileUtils
import org.geotools.data.DataStoreFinder
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.data.simple.{SimpleFeatureIterator, SimpleFeatureStore}
import org.geotools.geometry.jts.JTSFactoryFinder
import org.geotools.util.Converters
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.fs.data.FileSystemDataStore
import org.locationtech.geomesa.utils.collection.{CloseableIterator, SelfClosingIterator}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.jts.geom.Coordinate
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class BucketVsLeafStorageTest extends Specification {

  import org.locationtech.geomesa.fs.storage.common.RichSimpleFeatureType

  sequential

  "DataStores" should {
    val tempDir = Files.createTempDirectory("geomesa")
    val gf = JTSFactoryFinder.getGeometryFactory
    def mkSft(name: String) =  SimpleFeatureTypes.createType(name, "attr:String,dtg:Date,*geom:Point:srid=4326")
    def ds = DataStoreFinder.getDataStore(Map(
        "fs.path" -> tempDir.toFile.getPath,
        "fs.encoding" -> "parquet",
        "fs.config" -> "parquet.compression=gzip"
      )).asInstanceOf[FileSystemDataStore]

    def date(str: String) = Converters.convert(str, classOf[Date])

    def features(sft: SimpleFeatureType) = List(
      new ScalaSimpleFeature(sft, "1", Array("first",  date("2016-01-01"), gf.createPoint(new Coordinate(-5, 5)))), // z2 = 2
      new ScalaSimpleFeature(sft, "2", Array("second", date("2016-01-02"), gf.createPoint(new Coordinate(5, 5)))),  // z2 = 3
      new ScalaSimpleFeature(sft, "3", Array("third",  date("2016-01-03"), gf.createPoint(new Coordinate(5, -5)))), // z2 = 1
      new ScalaSimpleFeature(sft, "3", Array("fourth", date("2016-01-04"), gf.createPoint(new Coordinate(-5, -5)))) // z2 = 0
    )

    def toList(sfi: SimpleFeatureIterator): List[SimpleFeature] = SelfClosingIterator(sfi).toList

    "store data in leaves" >> {

      "in one scheme" >> {
        val typeName = "leaf-one"
        val sft = mkSft(typeName)
        sft.setScheme("daily")
        ds.createSchema(sft)
        ds.getTypeNames.length mustEqual 1
        ds.getTypeNames.head mustEqual typeName
        ds.storage(typeName).metadata.leafStorage must beTrue
        ds.getFeatureSource(sft.getTypeName).asInstanceOf[SimpleFeatureStore]
          .addFeatures(new ListFeatureCollection(sft, features(sft).take(2)))

        val fp = tempDir.resolve("leaf-one")
        fp.toFile.exists must beTrue
        fp.resolve("2016/01").toFile.exists must beTrue

        Seq(
          "2016/01/01_W[0-9a-f]{32}\\.parquet",
          "2016/01/02_W[0-9a-f]{32}\\.parquet"
        ).map(fp.toString + "/" + _).forall { f =>
          Files.walk(fp).collect(Collectors.toList()).count(_.toString.matches(f)) mustEqual 1
        }
        toList(ds.getFeatureSource(sft.getTypeName).getFeatures.features).size mustEqual 2

        ds.getFeatureSource(sft.getTypeName).asInstanceOf[SimpleFeatureStore]
          .addFeatures(new ListFeatureCollection(sft, features(sft).drop(2)))

        Seq(
          "2016/01/01_W[0-9a-f]{32}\\.parquet",
          "2016/01/02_W[0-9a-f]{32}\\.parquet",
          "2016/01/03_W[0-9a-f]{32}\\.parquet",
          "2016/01/04_W[0-9a-f]{32}\\.parquet"
        ).map(fp.toString + "/" + _).forall { f =>
          Files.walk(fp).collect(Collectors.toList()).count(_.toString.matches(f)) mustEqual 1
        }
        CloseableIterator(ds.getFeatureSource(sft.getTypeName).getFeatures.features).toSeq.size mustEqual 4

        // For now adding new features will create new parquet files on disk
        // and for leaf file stroage we should get a one up sequence file
        ds.getFeatureSource(sft.getTypeName).asInstanceOf[SimpleFeatureStore]
          .addFeatures(new ListFeatureCollection(sft, features(sft)))
        Seq(
          "2016/01/01_W[0-9a-f]{32}\\.parquet",
          "2016/01/02_W[0-9a-f]{32}\\.parquet",
          "2016/01/03_W[0-9a-f]{32}\\.parquet",
          "2016/01/04_W[0-9a-f]{32}\\.parquet"
        ).map(fp.toString + "/" + _).forall { f =>
          Files.walk(fp).collect(Collectors.toList()).count(_.toString.matches(f)) mustEqual 2
        }
        CloseableIterator(ds.getFeatureSource(sft.getTypeName).getFeatures.features).toSeq.size mustEqual 8

      }

      "in two schemes" >> {
        val typeName = "leaf-two"
        val sft = mkSft(typeName)
        sft.setScheme("daily,z2-2bits")
        ds.createSchema(sft)
        ds.getTypeNames.toSeq must contain(typeName)
        ds.storage(typeName).metadata.leafStorage must beTrue
        ds.getFeatureSource(sft.getTypeName).asInstanceOf[SimpleFeatureStore]
          .addFeatures(new ListFeatureCollection(sft, features(sft)))

        val fp = tempDir.resolve("leaf-two")
        fp.toFile.exists must beTrue

        Seq(
          "2016/01/01/2_W[0-9a-f]{32}\\.parquet",
          "2016/01/02/3_W[0-9a-f]{32}\\.parquet",
          "2016/01/03/1_W[0-9a-f]{32}\\.parquet",
          "2016/01/04/0_W[0-9a-f]{32}\\.parquet"
        ).map(fp.toString + "/" + _).forall { f =>
          Files.walk(fp).collect(Collectors.toList()).count(x => x.toString.matches(f)) mustEqual 1
        }
        CloseableIterator(ds.getFeatureSource(sft.getTypeName).getFeatures.features).toList.size mustEqual 4
        // For now adding more features results in a next seq file
        ds.getFeatureSource(sft.getTypeName).asInstanceOf[SimpleFeatureStore]
          .addFeatures(new ListFeatureCollection(sft, features(sft)))

        Seq(
          "2016/01/01/2_W[0-9a-f]{32}\\.parquet",
          "2016/01/02/3_W[0-9a-f]{32}\\.parquet",
          "2016/01/03/1_W[0-9a-f]{32}\\.parquet",
          "2016/01/04/0_W[0-9a-f]{32}\\.parquet"
        ).map(fp.toString + "/" + _).forall { f =>
          Files.walk(fp).collect(Collectors.toList()).count(x => x.toString.matches(f)) mustEqual 2
        }
        CloseableIterator(ds.getFeatureSource(sft.getTypeName).getFeatures.features).toList.size mustEqual 8
      }
    }

    "store data in buckets" >> {
      "in one scheme" >> {
        val typeName = "bucket-one"
        val sft = mkSft(typeName)
        sft.setScheme("daily")
        sft.setLeafStorage(false)
        ds.createSchema(sft)
        ds.getTypeNames.toSeq must contain(typeName)
        ds.storage(typeName).metadata.leafStorage must beFalse
        ds.getFeatureSource(sft.getTypeName).asInstanceOf[SimpleFeatureStore]
          .addFeatures(new ListFeatureCollection(sft, features(sft).take(2)))

        val fp = tempDir.resolve("bucket-one")

        Seq(
          "2016/01/01/W[0-9a-f]{32}\\.parquet",
          "2016/01/02/W[0-9a-f]{32}\\.parquet"
        ).map(fp.toString + "/" + _).forall { f =>
          Files.walk(fp).collect(Collectors.toList()).count(_.toString.matches(f)) mustEqual 1
        }
        toList(ds.getFeatureSource(sft.getTypeName).getFeatures.features).size mustEqual 2

        ds.getFeatureSource(sft.getTypeName).asInstanceOf[SimpleFeatureStore]
          .addFeatures(new ListFeatureCollection(sft, features(sft).drop(2)))

        Seq(
          "2016/01/01/W[0-9a-f]{32}\\.parquet",
          "2016/01/02/W[0-9a-f]{32}\\.parquet",
          "2016/01/03/W[0-9a-f]{32}\\.parquet",
          "2016/01/04/W[0-9a-f]{32}\\.parquet"
        ).map(fp.toString + "/" + _).forall { f =>
          Files.walk(fp).collect(Collectors.toList()).count(_.toString.matches(f)) mustEqual 1
        }
        CloseableIterator(ds.getFeatureSource(sft.getTypeName).getFeatures.features).toSeq.size mustEqual 4

        // For now adding new features will create new parquet files on disk
        // and for leaf file stroage we should get a one up sequence file
        ds.getFeatureSource(sft.getTypeName).asInstanceOf[SimpleFeatureStore]
          .addFeatures(new ListFeatureCollection(sft, features(sft)))
        Seq(
          "2016/01/01/W[0-9a-f]{32}\\.parquet",
          "2016/01/02/W[0-9a-f]{32}\\.parquet",
          "2016/01/03/W[0-9a-f]{32}\\.parquet",
          "2016/01/04/W[0-9a-f]{32}\\.parquet",
          "2016/01/01/W[0-9a-f]{32}\\.parquet",
          "2016/01/02/W[0-9a-f]{32}\\.parquet",
          "2016/01/03/W[0-9a-f]{32}\\.parquet",
          "2016/01/04/W[0-9a-f]{32}\\.parquet"
        ).map(fp.toString + "/" + _).forall { f =>
          Files.walk(fp).collect(Collectors.toList()).count(_.toString.matches(f)) mustEqual 2
        }
        CloseableIterator(ds.getFeatureSource(sft.getTypeName).getFeatures.features).toSeq.size mustEqual 8
      }

      "with two schemes" >> {
        val typeName = "bucket-two"
        val sft = mkSft(typeName)
        sft.setScheme("daily,z2-2bits")
        sft.setLeafStorage(false)
        ds.createSchema(sft)
        ds.getTypeNames.toSeq must contain(typeName)
        ds.storage(typeName).metadata.leafStorage must beFalse
        ds.getFeatureSource(sft.getTypeName).asInstanceOf[SimpleFeatureStore]
          .addFeatures(new ListFeatureCollection(sft, features(sft)))

        val fp = tempDir.resolve(typeName)
        fp.toFile.exists must beTrue

        Seq(
          "2016/01/01/2/W[0-9a-f]{32}\\.parquet",
          "2016/01/02/3/W[0-9a-f]{32}\\.parquet",
          "2016/01/03/1/W[0-9a-f]{32}\\.parquet",
          "2016/01/04/0/W[0-9a-f]{32}\\.parquet"
        ).map(fp.toString + "/" + _).forall { f =>
          Files.walk(fp).collect(Collectors.toList()).count(_.toString.matches(f)) mustEqual 1
        }
        CloseableIterator(ds.getFeatureSource(sft.getTypeName).getFeatures.features).toList.size mustEqual 4
        // For now adding more features results in a next seq file
        ds.getFeatureSource(sft.getTypeName).asInstanceOf[SimpleFeatureStore]
          .addFeatures(new ListFeatureCollection(sft, features(sft)))

        forall(Seq(
          "2016/01/01/2/W[0-9a-f]{32}\\.parquet",
          "2016/01/02/3/W[0-9a-f]{32}\\.parquet",
          "2016/01/03/1/W[0-9a-f]{32}\\.parquet",
          "2016/01/04/0/W[0-9a-f]{32}\\.parquet",
          "2016/01/01/2/W[0-9a-f]{32}\\.parquet",
          "2016/01/02/3/W[0-9a-f]{32}\\.parquet",
          "2016/01/03/1/W[0-9a-f]{32}\\.parquet",
          "2016/01/04/0/W[0-9a-f]{32}\\.parquet"
        ).map(fp.toString + "/" + _)) { f =>
          Files.walk(fp).collect(Collectors.toList()).count(_.toString.matches(f)) mustEqual 2
        }
        CloseableIterator(ds.getFeatureSource(sft.getTypeName).getFeatures.features).toList.size mustEqual 8
      }

      "support deprecated leaf-storage config" >> {
        val sft = mkSft("bucket-three")
        sft.setScheme("daily", Map("leaf-storage" -> "false"))
        ds.createSchema(sft)
        ds.storage("bucket-three").metadata.leafStorage must beFalse
      }
    }

    step {
      FileUtils.deleteDirectory(tempDir.toFile)
    }

  }
}
