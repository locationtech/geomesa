/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs

import java.nio.file.Files
import java.time.temporal.ChronoUnit

import com.vividsolutions.jts.geom.Coordinate
import org.apache.commons.io.FileUtils
import org.geotools.data.DataStoreFinder
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.data.simple.{SimpleFeatureIterator, SimpleFeatureStore}
import org.geotools.geometry.jts.JTSFactoryFinder
import org.joda.time.format.ISODateTimeFormat
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.fs.storage.common.{CompositeScheme, DateTimeScheme, PartitionScheme, Z2Scheme}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class BucketVsLeafStorageTest extends Specification {

  sequential

  "DataStores" should {
    val tempDir = Files.createTempDirectory("geomesa")
    val gf = JTSFactoryFinder.getGeometryFactory
    def mkSft(name: String) =  SimpleFeatureTypes.createType(name, "attr:String,dtg:Date,*geom:Point:srid=4326")
    def ds = DataStoreFinder.getDataStore(Map(
        "fs.path" -> tempDir.toFile.getPath,
        "fs.encoding" -> "parquet",
        "parquet.compression" -> "gzip"
      )).asInstanceOf[FileSystemDataStore]

    def date(str: String) = ISODateTimeFormat.date().parseDateTime(str).toDate

    def features(sft: SimpleFeatureType) = List(
      new ScalaSimpleFeature("1", sft, Array("first",  date("2016-01-01"), gf.createPoint(new Coordinate(-5, 5)))), // z2 = 2
      new ScalaSimpleFeature("2", sft, Array("second", date("2016-01-02"), gf.createPoint(new Coordinate(5, 5)))),  // z2 = 3
      new ScalaSimpleFeature("3", sft, Array("third",  date("2016-01-03"), gf.createPoint(new Coordinate(5, -5)))), // z2 = 1
      new ScalaSimpleFeature("3", sft, Array("fourth", date("2016-01-04"), gf.createPoint(new Coordinate(-5, -5)))) // z2 = 0
    )

    def addFeatures(sft: SimpleFeatureType) =
      ds.getFeatureSource(sft.getTypeName).asInstanceOf[SimpleFeatureStore]
        .addFeatures(new ListFeatureCollection(sft, features(sft)))

    def toList(sfi: SimpleFeatureIterator): List[SimpleFeature] = {
      import org.locationtech.geomesa.utils.geotools.Conversions._
      sfi.toList
    }
    "store data in leaves" >> {

      "in one scheme" >> {
        val sft = mkSft("leaf-one")
        val scheme = new DateTimeScheme("yyyy/MM/dd", ChronoUnit.DAYS, 1, "dtg", true)
        PartitionScheme.addToSft(sft, scheme)
        ds.createSchema(sft)
        ds.getTypeNames.length mustEqual 1
        ds.getTypeNames.head mustEqual "leaf-one"
        ds.getFeatureSource(sft.getTypeName).asInstanceOf[SimpleFeatureStore]
          .addFeatures(new ListFeatureCollection(sft, features(sft).take(2)))

        val fp = tempDir.resolve("leaf-one")
        fp.toFile.exists must beTrue
        fp.resolve("2016/01").toFile.exists must beTrue

        Seq(
          "2016/01/01_0000.parquet",
          "2016/01/02_0000.parquet"
        ).forall { f =>
          val p = fp.resolve(f)
          p.toFile.exists() must beTrue
        }
        toList(ds.getFeatureSource(sft.getTypeName).getFeatures.features).size mustEqual 2

        ds.getFeatureSource(sft.getTypeName).asInstanceOf[SimpleFeatureStore]
          .addFeatures(new ListFeatureCollection(sft, features(sft).drop(2)))

        Seq(
          "2016/01/01_0000.parquet",
          "2016/01/02_0000.parquet",
          "2016/01/03_0000.parquet",
          "2016/01/04_0000.parquet"
        ).forall { f =>
          val p = fp.resolve(f)
          p.toFile.exists() must beTrue
        }
        CloseableIterator(ds.getFeatureSource(sft.getTypeName).getFeatures.features).toSeq.size mustEqual 4

        // For now adding new features will create new parquet files on disk
        // and for leaf file stroage we should get a one up sequence file
        ds.getFeatureSource(sft.getTypeName).asInstanceOf[SimpleFeatureStore]
          .addFeatures(new ListFeatureCollection(sft, features(sft)))
        Seq(
          "2016/01/01_0000.parquet",
          "2016/01/02_0000.parquet",
          "2016/01/03_0000.parquet",
          "2016/01/04_0000.parquet",
          "2016/01/01_0001.parquet",
          "2016/01/02_0001.parquet",
          "2016/01/03_0001.parquet",
          "2016/01/04_0001.parquet"
        ).forall { f =>
          val p = fp.resolve(f)
          p.toFile.exists() must beTrue
        }
        CloseableIterator(ds.getFeatureSource(sft.getTypeName).getFeatures.features).toSeq.size mustEqual 8

      }

      "in two schemes" >> {
        val sft = mkSft("leaf-two")
        val scheme = new CompositeScheme(Seq(
          new DateTimeScheme("yyyy/MM/dd", ChronoUnit.DAYS, 1, "dtg", true),
          new Z2Scheme(2, "geom", true))
        )
        PartitionScheme.addToSft(sft, scheme)
        ds.createSchema(sft)
        ds.getTypeNames.toSeq must contain("leaf-two")
        ds.getFeatureSource(sft.getTypeName).asInstanceOf[SimpleFeatureStore]
          .addFeatures(new ListFeatureCollection(sft, features(sft)))

        val fp = tempDir.resolve("leaf-two")
        fp.toFile.exists must beTrue

        Seq(
          "2016/01/01/2_0000.parquet",
          "2016/01/02/3_0000.parquet",
          "2016/01/03/1_0000.parquet",
          "2016/01/04/0_0000.parquet"
        ).forall { f =>
          val p = fp.resolve(f)
          p.toFile.exists() must beTrue
        }
        CloseableIterator(ds.getFeatureSource(sft.getTypeName).getFeatures.features).toList.size mustEqual 4
        // For now adding more features results in a next seq file
        ds.getFeatureSource(sft.getTypeName).asInstanceOf[SimpleFeatureStore]
          .addFeatures(new ListFeatureCollection(sft, features(sft)))

        Seq(
          "2016/01/01/2_0000.parquet",
          "2016/01/02/3_0000.parquet",
          "2016/01/03/1_0000.parquet",
          "2016/01/04/0_0000.parquet",
          "2016/01/01/2_0001.parquet",
          "2016/01/02/3_0001.parquet",
          "2016/01/03/1_0001.parquet",
          "2016/01/04/0_0001.parquet"
        ).forall { f =>
          val p = fp.resolve(f)
          p.toFile.exists() must beTrue
        }
        CloseableIterator(ds.getFeatureSource(sft.getTypeName).getFeatures.features).toList.size mustEqual 8
      }

    }

    "store data in buckets" >> {
      "in one scheme" >> {
        val sft = mkSft("bucket-one")
        val scheme = new DateTimeScheme("yyyy/MM/dd", ChronoUnit.DAYS, 1, "dtg", false)
        PartitionScheme.addToSft(sft, scheme)
        ds.createSchema(sft)
        ds.getTypeNames.toSeq must contain("bucket-one")
        ds.getFeatureSource(sft.getTypeName).asInstanceOf[SimpleFeatureStore]
          .addFeatures(new ListFeatureCollection(sft, features(sft).take(2)))

        val fp = tempDir.resolve("bucket-one")

        Seq(
          "2016/01/01/0000.parquet",
          "2016/01/02/0000.parquet"
        ).forall { f =>
          val p = fp.resolve(f)
          p.toFile.exists() must beTrue
        }
        toList(ds.getFeatureSource(sft.getTypeName).getFeatures.features).size mustEqual 2

        ds.getFeatureSource(sft.getTypeName).asInstanceOf[SimpleFeatureStore]
          .addFeatures(new ListFeatureCollection(sft, features(sft).drop(2)))

        Seq(
          "2016/01/01/0000.parquet",
          "2016/01/02/0000.parquet",
          "2016/01/03/0000.parquet",
          "2016/01/04/0000.parquet"
        ).forall { f =>
          val p = fp.resolve(f)
          p.toFile.exists() must beTrue
        }
        CloseableIterator(ds.getFeatureSource(sft.getTypeName).getFeatures.features).toSeq.size mustEqual 4

        // For now adding new features will create new parquet files on disk
        // and for leaf file stroage we should get a one up sequence file
        ds.getFeatureSource(sft.getTypeName).asInstanceOf[SimpleFeatureStore]
          .addFeatures(new ListFeatureCollection(sft, features(sft)))
        Seq(
          "2016/01/01/0000.parquet",
          "2016/01/02/0000.parquet",
          "2016/01/03/0000.parquet",
          "2016/01/04/0000.parquet",
          "2016/01/01/0001.parquet",
          "2016/01/02/0001.parquet",
          "2016/01/03/0001.parquet",
          "2016/01/04/0001.parquet"
        ).forall { f =>
          val p = fp.resolve(f)
          p.toFile.exists() must beTrue
        }
        CloseableIterator(ds.getFeatureSource(sft.getTypeName).getFeatures.features).toSeq.size mustEqual 8
      }

      "with two schemes" >> {
        val typeName = "bucket-two"
        val sft = mkSft(typeName)
        val scheme = new CompositeScheme(Seq(
          new DateTimeScheme("yyyy/MM/dd", ChronoUnit.DAYS, 1, "dtg", false),
          new Z2Scheme(2, "geom", false))
        )
        PartitionScheme.addToSft(sft, scheme)
        ds.createSchema(sft)
        ds.getTypeNames.toSeq must contain(typeName)
        ds.getFeatureSource(sft.getTypeName).asInstanceOf[SimpleFeatureStore]
          .addFeatures(new ListFeatureCollection(sft, features(sft)))

        val fp = tempDir.resolve(typeName)
        fp.toFile.exists must beTrue

        Seq(
          "2016/01/01/2/0000.parquet",
          "2016/01/02/3/0000.parquet",
          "2016/01/03/1/0000.parquet",
          "2016/01/04/0/0000.parquet"
        ).forall { f =>
          val p = fp.resolve(f)
          p.toFile.exists() must beTrue
        }
        CloseableIterator(ds.getFeatureSource(sft.getTypeName).getFeatures.features).toList.size mustEqual 4
        // For now adding more features results in a next seq file
        ds.getFeatureSource(sft.getTypeName).asInstanceOf[SimpleFeatureStore]
          .addFeatures(new ListFeatureCollection(sft, features(sft)))

        Seq(
          "2016/01/01/2/0000.parquet",
          "2016/01/02/3/0000.parquet",
          "2016/01/03/1/0000.parquet",
          "2016/01/04/0/0000.parquet",
          "2016/01/01/2/0001.parquet",
          "2016/01/02/3/0001.parquet",
          "2016/01/03/1/0001.parquet",
          "2016/01/04/0/0001.parquet"
        ).forall { f =>
          val p = fp.resolve(f)
          p.toFile.exists() must beTrue
        }
        CloseableIterator(ds.getFeatureSource(sft.getTypeName).getFeatures.features).toList.size mustEqual 8
      }

    }

    step {
      FileUtils.deleteDirectory(tempDir.toFile)
    }

  }
}
