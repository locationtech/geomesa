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
import com.vividsolutions.jts.geom.Coordinate
import org.apache.commons.io.FileUtils
import org.geotools.data.{DataStoreFinder, Query, Transaction}
import org.geotools.filter.identity.FeatureIdImpl
import org.geotools.geometry.jts.JTSFactoryFinder
import org.joda.time.format.ISODateTimeFormat
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.fs.storage.common.{DateTimeScheme, PartitionScheme}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.filter.Filter
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class FileSystemDataStoreTest extends Specification {

  sequential

  val dir = Files.createTempDirectory("fsds-test").toFile

  "FileSystemDataStore" should {
    "pass a test" >> {
      val gf = JTSFactoryFinder.getGeometryFactory
      val sft = SimpleFeatureTypes.createType("test", "name:String,age:Int,dtg:Date,*geom:Point:srid=4326")

      val sf = new ScalaSimpleFeature("1", sft, Array("test", Integer.valueOf(100),
        ISODateTimeFormat.dateTime().parseDateTime("2017-06-05T04:03:02.0001Z").toDate, gf.createPoint(new Coordinate(10, 10))))

      import scala.collection.JavaConversions._
      val ds = DataStoreFinder.getDataStore(Map(
        "fs.path" -> dir.getPath,
        "fs.encoding" -> "parquet",
        "parquet.compression" -> "gzip"))
      val partitionScheme = new DateTimeScheme(DateTimeScheme.Formats.Daily, ChronoUnit.DAYS, 1, sft, "dtg", false)
      PartitionScheme.addToSft(sft, partitionScheme)
      ds.createSchema(sft)

      val fw = ds.getFeatureWriterAppend("test", Transaction.AUTO_COMMIT)
      val s = fw.next()
      s.getIdentifier.asInstanceOf[FeatureIdImpl].setID(sf.getID)
      s.setAttributes(sf.getAttributes)
      fw.write()
      fw.close()

      // Metadata, schema, and partition file checks
      new File(dir, "test/schema.sft").exists() must beTrue
      new File(dir, "test/2017/06/05/0000.parquet").exists() must beTrue
      new File(dir, "test/2017/06/05/0000.parquet").isFile must beTrue

      // metadata
      new File(dir, "test/metadata").exists() must beTrue
      val conf = ConfigFactory.parseFile(new File(dir, "test/metadata"))
      conf.hasPath("partitions") must beTrue
      val p1 = conf.getConfig("partitions").getStringList("2017/06/05")
      p1.size() mustEqual 1
      p1.get(0) mustEqual "0000.parquet"

      ds.getTypeNames must have size 1
      val fs = ds.getFeatureSource("test")
      fs must not(beNull)
      import org.locationtech.geomesa.utils.geotools.Conversions._
      val q = new Query("test", Filter.INCLUDE)
      val features = fs.getFeatures(q).features().toList

      features.size mustEqual 1

      // TODO add another to ensure metadata works when reading writing
    }
  }

  step {
    FileUtils.deleteDirectory(dir)
  }
}
