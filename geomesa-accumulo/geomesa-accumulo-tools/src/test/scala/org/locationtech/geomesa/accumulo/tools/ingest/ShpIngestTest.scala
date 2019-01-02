/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.tools.ingest

import java.io.File
import java.nio.file.{Files, Path}
import java.util.Date

import org.geotools.data.shapefile.ShapefileDataStoreFactory
import org.geotools.data.store.ReprojectingFeatureCollection
import org.geotools.data.{DataStore, Transaction}
import org.geotools.factory.Hints
import org.geotools.referencing.CRS
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.tools.AccumuloRunner
import org.locationtech.geomesa.convert.{Modes, SimpleFeatureValidator}
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.collection.{CloseableIterator, SelfClosingIterator}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.{PathUtils, WithClose}
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.specs2.specification.BeforeAfterAll

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class ShpIngestTest extends Specification with BeforeAfterAll {

  sequential

  // note: shpfile always puts geom first
  val schema = SimpleFeatureTypes.createType("shpingest", "*geom:Point:srid=4326,age:Integer,dtg:Date")

  val features = Seq(
    ScalaSimpleFeature.create(schema, "1", "POINT(1.0 1.5)", 1, "2011-01-01T00:00:00.000Z"),
    ScalaSimpleFeature.create(schema, "2", "POINT(2.0 2.5)", 2, "2012-01-01T00:00:00.000Z")
  )

  val featuresWithNulls = features ++ Seq(
    ScalaSimpleFeature.create(schema, "3", "POINT(3.0 1.5)", 3, null),
    ScalaSimpleFeature.create(schema, "4", "POINT(4.0 2.5)", 4, "2013-01-01T00:00:00.000Z")
  )

  val baseArgs = Array[String]("ingest", "--zookeepers", "zoo", "--mock", "--instance", "mycloud", "--user", "myuser",
    "--password", "mypassword", "--catalog", "testshpingestcatalog", "--force")

  var dir: Path = _
  var shpFile: File = _
  var shpFileToReproject: File = _
  var shpFileToReproject2: File = _
  var shpFileWithNullDates: File = _

  "ShpIngest" should {

    "should properly ingest a shapefile" in {
      val args = baseArgs :+ shpFile.getAbsolutePath

      val command = AccumuloRunner.parseCommand(args).asInstanceOf[AccumuloIngestCommand]
      command.execute()

      val fs = command.withDataStore(_.getFeatureSource("shpingest"))

      SelfClosingIterator(fs.getFeatures.features).toList must haveLength(2)

      val bounds = fs.getBounds
      bounds.getMinX mustEqual 1.0
      bounds.getMaxX mustEqual 2.0
      bounds.getMinY mustEqual 1.5
      bounds.getMaxY mustEqual 2.5

      command.withDataStore { ds =>
        ds.stats.getAttributeBounds[Date](ds.getSchema(schema.getTypeName), "dtg").map(_.tuple) must
            beSome((features.head.getAttribute("dtg"), features.last.getAttribute("dtg"), 2L))
      }
    }

    "should support renaming the feature type" in {
      val args = baseArgs ++ Array("--feature-name", "changed", shpFile.getAbsolutePath)

      val command = AccumuloRunner.parseCommand(args).asInstanceOf[AccumuloIngestCommand]
      command.execute()

      val fs = command.withDataStore(_.getFeatureSource("changed"))

      SelfClosingIterator(fs.getFeatures.features).toList must haveLength(2)

      val bounds = fs.getBounds
      bounds.getMinX mustEqual 1.0
      bounds.getMaxX mustEqual 2.0
      bounds.getMinY mustEqual 1.5
      bounds.getMaxY mustEqual 2.5

      command.withDataStore { ds =>
        ds.stats.getAttributeBounds[Date](ds.getSchema("changed"), "dtg").map(_.tuple) must
            beSome((features.head.getAttribute("dtg"), features.last.getAttribute("dtg"), 2L))
      }
    }

    "reproject to 4326 automatically on ingest" in {
      val args = baseArgs :+ shpFileToReproject.getAbsolutePath

      val command = AccumuloRunner.parseCommand(args).asInstanceOf[AccumuloIngestCommand]
      command.execute()

      val fs = command.withDataStore(_.getFeatureSource("shpingest32631"))

      SelfClosingIterator(fs.getFeatures.features).toList must haveLength(2)

      val bounds = fs.getBounds
      bounds.getMinX must beCloseTo(1.0, 0.0001)
      bounds.getMaxX must beCloseTo(2.0, 0.0001)
      bounds.getMinY must beCloseTo(1.5, 0.0001)
      bounds.getMaxY must beCloseTo(2.5, 0.0001)

      command.withDataStore { ds =>
        ds.stats.getAttributeBounds[Date](ds.getSchema(schema.getTypeName), "dtg").map(_.tuple) must
          beSome((features.head.getAttribute("dtg"), features.last.getAttribute("dtg"), 2L))
      }
    }

    "reproject to 4326 automatically on ingest with flipped inputs" in {
      val args = baseArgs :+ shpFileToReproject2.getAbsolutePath

      val command = AccumuloRunner.parseCommand(args).asInstanceOf[AccumuloIngestCommand]
      command.execute()

      val fs = command.withDataStore(_.getFeatureSource("shpingest4269"))

      SelfClosingIterator(fs.getFeatures.features).toList must haveLength(2)

      val bounds = fs.getBounds
      bounds.getMinX must beCloseTo(1.0, 0.0001)
      bounds.getMaxX must beCloseTo(2.0, 0.0001)
      bounds.getMinY must beCloseTo(1.5, 0.0001)
      bounds.getMaxY must beCloseTo(2.5, 0.0001)

      command.withDataStore { ds =>
        ds.stats.getAttributeBounds[Date](ds.getSchema(schema.getTypeName), "dtg").map(_.tuple) must
          beSome((features.head.getAttribute("dtg"), features.last.getAttribute("dtg"), 2L))
      }
    }

    "skip records with null dates by default" in {
      val args = baseArgs :+ shpFileWithNullDates.getAbsolutePath

      val command = AccumuloRunner.parseCommand(args).asInstanceOf[AccumuloIngestCommand]
      command.execute()

      val fs = command.withDataStore(_.getFeatureSource("shpingestNullDates"))

      SelfClosingIterator(fs.getFeatures.features).toList must haveLength(3)

      val bounds = fs.getBounds
      bounds.getMinX mustEqual 1.0
      bounds.getMaxX mustEqual 4.0
      bounds.getMinY mustEqual 1.5
      bounds.getMaxY mustEqual 2.5

      command.withDataStore { ds =>
        ds.stats.getAttributeBounds[Date](ds.getSchema(schema.getTypeName), "dtg").map(_.tuple) must
          beSome((features.head.getAttribute("dtg"), features.last.getAttribute("dtg"), 2L))
      }
    }

    // In this test, the third record has a null date.  Raising an error stops the ingest.
    "index null dates via override" in {
      Modes.ErrorMode.systemProperty.threadLocalValue.set("raise-errors")

      val args = baseArgs ++ Array("--feature-name", "nullDates2", shpFileWithNullDates.getAbsolutePath)

      val command = AccumuloRunner.parseCommand(args).asInstanceOf[AccumuloIngestCommand]
      command.execute()
      System.clearProperty("converter.error.mode")

      val fs = command.withDataStore(_.getFeatureSource("nullDates2"))

      SelfClosingIterator(fs.getFeatures.features).toList must haveLength(2)

      val bounds = fs.getBounds
      bounds.getMinX mustEqual 1.0
      bounds.getMaxX mustEqual 2.0
      bounds.getMinY mustEqual 1.5
      bounds.getMaxY mustEqual 2.5

      try {
        command.withDataStore { ds =>
          ds.stats.getAttributeBounds[Date](ds.getSchema("nullDates2"), "dtg").map(_.tuple) must
            beSome((features.head.getAttribute("dtg"), features.last.getAttribute("dtg"), 2L))
        }
      } finally {
        Modes.ErrorMode.systemProperty.threadLocalValue.remove()
      }
    }

    // In this test, the third record has a null date.  The system property turns off the error check.
    "index null dates via override" in {
      SimpleFeatureValidator.property.threadLocalValue.set("has-geo")
      val args = baseArgs ++ Array("--feature-name", "nullDates3", shpFileWithNullDates.getAbsolutePath)

      val command = AccumuloRunner.parseCommand(args).asInstanceOf[AccumuloIngestCommand]
      command.execute()

      val fs = command.withDataStore(_.getFeatureSource("nullDates3"))

      SelfClosingIterator(fs.getFeatures.features).toList must haveLength(4)

      val bounds = fs.getBounds
      bounds.getMinX mustEqual 1.0
      bounds.getMaxX mustEqual 4.0
      bounds.getMinY mustEqual 1.5
      bounds.getMaxY mustEqual 2.5

      try {
        command.withDataStore { ds =>
          ds.stats.getAttributeBounds[Date](ds.getSchema("nullDates3"), "dtg").map(_.tuple) must
            beSome((features.head.getAttribute("dtg"), featuresWithNulls.last.getAttribute("dtg"), 3L))
        }
      } finally {
        SimpleFeatureValidator.property.threadLocalValue.remove()
      }
    }

    "index no data with null dates with batch parse mode and raise-errors via override" in {
      Modes.ErrorMode.systemProperty.threadLocalValue.set("raise-errors")
      Modes.ParseMode.systemProperty.threadLocalValue.set("batch")

      val args = baseArgs ++ Array("--feature-name", "nullDates4", shpFileWithNullDates.getAbsolutePath)

      val command = AccumuloRunner.parseCommand(args).asInstanceOf[AccumuloIngestCommand]
      command.execute()

      try {
        val fs = command.withDataStore(_.getFeatureSource("nullDates4"))
        SelfClosingIterator(fs.getFeatures.features).toList must haveLength(0)
      } finally {
        Modes.ErrorMode.systemProperty.threadLocalValue.remove()
        Modes.ParseMode.systemProperty.threadLocalValue.remove()
      }
    }
  }

  override def beforeAll(): Unit = {
    dir = Files.createTempDirectory("gm-shp-ingest-test")
    shpFile = new File(dir.toFile, "shpingest.shp")
    shpFileToReproject = new File(dir.toFile, "shpingest32631.shp")
    shpFileToReproject2 = new File(dir.toFile, "shpingest4269.shp")
    shpFileWithNullDates = new File(dir.toFile, "shpingestNullDates.shp")

    val dsf = new ShapefileDataStoreFactory

    val shpStore = dsf.createNewDataStore(Map("url" -> shpFile.toURI.toURL))
    val shpStoreToReproject: DataStore = dsf.createNewDataStore(Map("url" -> shpFileToReproject.toURI.toURL))
    val shpStoreToReproject2: DataStore = dsf.createNewDataStore(Map("url" -> shpFileToReproject2.toURI.toURL))
    val shpStoreWithNullDates = dsf.createNewDataStore(Map("url" -> shpFileWithNullDates.toURI.toURL))

    try {
      shpStore.createSchema(schema)
      WithClose(shpStore.getFeatureWriterAppend("shpingest", Transaction.AUTO_COMMIT)) { writer =>
        features.foreach { feature =>
          val toWrite = writer.next()
          toWrite.setAttributes(feature.getAttributes)
          toWrite.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
          toWrite.getUserData.put(Hints.PROVIDED_FID, feature.getID)
          writer.write()
        }
      }

      val initialFeatures = shpStore.getFeatureSource("shpingest").getFeatures
      val projectedFeatures = new ReprojectingFeatureCollection(initialFeatures, CRS.decode("EPSG:32631"))

      shpStoreToReproject.createSchema(projectedFeatures.getSchema)
      WithClose(shpStoreToReproject.getFeatureWriterAppend("shpingest32631", Transaction.AUTO_COMMIT)) { writer =>
        CloseableIterator(projectedFeatures.features()).foreach { feature =>
          val toWrite = writer.next()
          toWrite.setAttributes(feature.getAttributes)
          toWrite.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
          toWrite.getUserData.put(Hints.PROVIDED_FID, feature.getID)
          writer.write()
        }
      }

      val projectedFeatures2 = new ReprojectingFeatureCollection(initialFeatures, CRS.decode("EPSG:4269"))

      shpStoreToReproject2.createSchema(projectedFeatures2.getSchema)
      WithClose(shpStoreToReproject2.getFeatureWriterAppend("shpingest4269", Transaction.AUTO_COMMIT)) { writer =>
        CloseableIterator(projectedFeatures2.features()).foreach { feature =>
          val toWrite = writer.next()
          toWrite.setAttributes(feature.getAttributes)
          toWrite.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
          toWrite.getUserData.put(Hints.PROVIDED_FID, feature.getID)
          writer.write()
        }
      }

      shpStoreWithNullDates.createSchema(schema)
      WithClose(shpStoreWithNullDates.getFeatureWriterAppend("shpingestNullDates", Transaction.AUTO_COMMIT)) { writer =>
        featuresWithNulls.foreach { feature =>
          val toWrite = writer.next()
          toWrite.setAttributes(feature.getAttributes)
          toWrite.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
          toWrite.getUserData.put(Hints.PROVIDED_FID, feature.getID)
          writer.write()
        }
      }


    } finally {
      shpStore.dispose()
      shpStoreToReproject.dispose()
      shpStoreToReproject2.dispose()
      shpStoreWithNullDates.dispose()
    }
  }

  override def afterAll(): Unit = PathUtils.deleteRecursively(dir)
}
