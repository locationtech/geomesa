/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs

import java.io.File
import java.nio.file.Files
import java.time.temporal.ChronoUnit

import com.typesafe.config.ConfigFactory
import org.apache.commons.io.FileUtils
import org.geotools.data.{DataStoreFinder, Query, Transaction}
import org.geotools.filter.identity.FeatureIdImpl
import org.geotools.filter.text.ecql.ECQL
import org.geotools.geometry.jts.JTSFactoryFinder
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.fs.storage.common.{DateTimeScheme, PartitionScheme}
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.filter.Filter
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class FileSystemDataStoreTest extends Specification {

  sequential

  val gf = JTSFactoryFinder.getGeometryFactory
  val dir = Files.createTempDirectory("fsds-test").toFile

  val sft = SimpleFeatureTypes.createType("test", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")
  val partitionScheme = new DateTimeScheme(DateTimeScheme.Formats.Daily, ChronoUnit.DAYS, 1, "dtg", false)
  val sf = ScalaSimpleFeature.create(sft, "1", "test", 100, "2017-06-05T04:03:02.0001Z", "POINT(10 10)")

  "FileSystemDataStore" should {
    "create a DS" >> {
      val ds = DataStoreFinder.getDataStore(Map(
        "fs.path" -> dir.getPath,
        "fs.encoding" -> "parquet",
        "parquet.compression" -> "gzip"))
      PartitionScheme.addToSft(sft, partitionScheme)
      ds.createSchema(sft)

      val fw = ds.getFeatureWriterAppend("test", Transaction.AUTO_COMMIT)
      val s = fw.next()
      s.getIdentifier.asInstanceOf[FeatureIdImpl].setID(sf.getID)
      s.setAttributes(sf.getAttributes)
      fw.write()
      fw.close()

      // metadata
      new File(dir, "test/metadata.json").exists() must beTrue

      val conf = ConfigFactory.parseFile(new File(dir, "test/metadata.json"))
      conf.hasPath("partitions") must beTrue
      val p1 = conf.getConfig("partitions").getStringList("2017/06/05")
      p1.size() mustEqual 1
      p1.get(0).matches("W[0-9a-f]{32}\\.parquet") must beTrue

      // Metadata, schema, and partition file checks
      new File(dir, "test/2017/06/05").exists() must beTrue
      new File(dir, "test/2017/06/05").isDirectory must beTrue
      new File(dir, s"test/2017/06/05/${p1.get(0)}").exists() must beTrue
      new File(dir, s"test/2017/06/05/${p1.get(0)}").isFile must beTrue

      ds.getTypeNames must have size 1
      val fs = ds.getFeatureSource("test")
      fs must not(beNull)

      val features = SelfClosingIterator(fs.getFeatures(new Query("test")).features()).toList
      features must haveSize(1)
      features.head mustEqual sf
    }

    "create a second ds with the same path" >> {
      // Load a new datastore to read metadata and stuff
      val ds2 = DataStoreFinder.getDataStore(Map(
        "fs.path" -> dir.getPath,
        "fs.encoding" -> "parquet")).asInstanceOf[FileSystemDataStore]
      ds2.getTypeNames.toList must containTheSameElementsAs(Seq("test"))

      val features = SelfClosingIterator(ds2.getFeatureSource("test").getFeatures(Filter.INCLUDE).features()).toList

      features must haveSize(1)
      features.head mustEqual sf
    }

    "call create schema on existing type" >> {
      val ds2 = DataStoreFinder.getDataStore(Map(
        "fs.path" -> dir.getPath,
        "fs.encoding" -> "parquet")).asInstanceOf[FileSystemDataStore]
      val sameSft = SimpleFeatureTypes.createType("test", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")
      val partitionScheme = new DateTimeScheme(DateTimeScheme.Formats.Daily, ChronoUnit.DAYS, 1, "dtg", false)

      PartitionScheme.addToSft(sameSft, partitionScheme)

      ds2.createSchema(sameSft) must not(throwA[Throwable])
    }

    "support transforms" >> {
      val ds = DataStoreFinder.getDataStore(Map(
        "fs.path" -> dir.getPath,
        "fs.encoding" -> "parquet")).asInstanceOf[FileSystemDataStore]

      val filters = Seq(
        "INCLUDE",
        "name = 'test'",
        "bbox(geom, 5, 5, 15, 15)",
        "dtg DURING 2017-06-05T04:03:00.0000Z/2017-06-05T04:04:00.0000Z",
        "dtg > '2017-06-05T04:03:00.0000Z' AND dtg < '2017-06-05T04:04:00.0000Z'",
        "dtg DURING 2017-06-05T04:03:00.0000Z/2017-06-05T04:04:00.0000Z and bbox(geom, 5, 5, 15, 15)"
      ).map(ECQL.toFilter)
      val transforms = Seq(null, Array("name"), Array("dtg", "geom")) // note: geom is always returned

      foreach(filters) { filter =>
        foreach(transforms) { transform =>
          val query = new Query("test", filter, transform)
          val features = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList
          features must haveLength(1)
          val feature = features.head
          if (transform == null) {
            feature.getAttributeCount mustEqual 4
            feature.getAttributes mustEqual sf.getAttributes
          } else if (transform.contains("geom")) {
            feature.getAttributeCount mustEqual transform.length
            foreach(transform)(t => feature.getAttribute(t) mustEqual sf.getAttribute(t))
          } else {
            feature.getAttributeCount mustEqual transform.length + 1
            foreach(transform)(t => feature.getAttribute(t) mustEqual sf.getAttribute(t))
            feature.getAttribute("geom") mustEqual sf.getAttribute("geom")
          }
        }
      }
    }
  }

  step {
    FileUtils.deleteDirectory(dir)
  }
}
