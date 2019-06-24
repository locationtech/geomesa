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
import java.util.{Collections, Date}

import org.geotools.data.Transaction
import org.geotools.data.shapefile.ShapefileDataStore
import org.geotools.data.store.ReprojectingFeatureCollection
import org.geotools.util.factory.Hints
import org.geotools.referencing.CRS
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithDataStore
import org.locationtech.geomesa.convert.Modes
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.io.{PathUtils, WithClose, WithStore}
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ShpIngestTest extends TestWithDataStore {

  sequential

  // note: shpfile always puts geom first
  override val spec: String = "*geom:Point:srid=4326,age:Integer,dtg:Date"

  val features = Seq(
    ScalaSimpleFeature.create(sft, "1", "POINT(1.0 1.5)", 1, "2011-01-01T00:00:00.000Z"),
    ScalaSimpleFeature.create(sft, "2", "POINT(2.0 2.5)", 2, "2012-01-01T00:00:00.000Z")
  )

  val featuresWithNulls = features ++ Seq(
    ScalaSimpleFeature.create(sft, "3", "POINT(3.0 1.5)", 3, null),
    ScalaSimpleFeature.create(sft, "4", "POINT(4.0 2.5)", 4, "2013-01-01T00:00:00.000Z")
  )

  def connectedCommand(file: String): AccumuloIngestCommand = {
    val command = new AccumuloIngestCommand()
    command.params.user        = mockUser
    command.params.instance    = mockInstanceId
    command.params.zookeepers  = mockZookeepers
    command.params.password    = mockPassword
    command.params.catalog     = catalog
    command.params.mock        = true
    command.params.force       = true
    command.params.files       = Collections.singletonList(new File(dir.toFile, s"$file.shp").getAbsolutePath)
    command
  }

  val shpFile = "shpingest"
  val shpFileToReproject = "shpingest32631"
  val shpFileToReproject2 = "shpingest4269"
  val shpFileWithNullDates = "shpingestNullDates"

  var dir: Path = _

  step {
    dir = Files.createTempDirectory("gm-shp-ingest-test")

    def params(name: String) = Map("url" -> new File(dir.toFile, s"$name.shp").toURI.toURL)

    val initialFeatures = WithStore[ShapefileDataStore](params(shpFile)) { store =>
      store.createSchema(sft)
      WithClose(store.getFeatureWriterAppend(Transaction.AUTO_COMMIT)) { writer =>
        features.foreach { feature =>
          val toWrite = writer.next()
          toWrite.setAttributes(feature.getAttributes)
          toWrite.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
          toWrite.getUserData.put(Hints.PROVIDED_FID, feature.getID)
          writer.write()
        }
      }
      store.getFeatureSource().getFeatures
    }

    WithStore[ShapefileDataStore](params(shpFileToReproject)) { store =>
      val projectedFeatures = new ReprojectingFeatureCollection(initialFeatures, CRS.decode("EPSG:32631"))
      store.createSchema(projectedFeatures.getSchema)
      WithClose(store.getFeatureWriterAppend(Transaction.AUTO_COMMIT)) { writer =>
        SelfClosingIterator(projectedFeatures.features()).foreach { feature =>
          val toWrite = writer.next()
          toWrite.setAttributes(feature.getAttributes)
          toWrite.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
          toWrite.getUserData.put(Hints.PROVIDED_FID, feature.getID)
          writer.write()
        }
      }
    }

    WithStore[ShapefileDataStore](params(shpFileToReproject2)) { store =>
      val projectedFeatures = new ReprojectingFeatureCollection(initialFeatures, CRS.decode("EPSG:4269"))
      store.createSchema(projectedFeatures.getSchema)
      WithClose(store.getFeatureWriterAppend(Transaction.AUTO_COMMIT)) { writer =>
        SelfClosingIterator(projectedFeatures.features()).foreach { feature =>
          val toWrite = writer.next()
          toWrite.setAttributes(feature.getAttributes)
          toWrite.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
          toWrite.getUserData.put(Hints.PROVIDED_FID, feature.getID)
          writer.write()
        }
      }
    }

    WithStore[ShapefileDataStore](params(shpFileWithNullDates)) { store =>
      store.createSchema(sft)
      WithClose(store.getFeatureWriterAppend(Transaction.AUTO_COMMIT)) { writer =>
        featuresWithNulls.foreach { feature =>
          val toWrite = writer.next()
          toWrite.setAttributes(feature.getAttributes)
          toWrite.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
          toWrite.getUserData.put(Hints.PROVIDED_FID, feature.getID)
          writer.write()
        }
      }
    }
  }

  "ShpIngest" should {
    "should properly ingest a shapefile" in {
      connectedCommand(shpFile).execute()

      ds.stats.generateStats(ds.getSchema(shpFile)) // re-gen stats to invalidate cache

      val fs = ds.getFeatureSource(shpFile)

      SelfClosingIterator(fs.getFeatures.features).toList must haveLength(2)

      val bounds = fs.getBounds
      bounds.getMinX mustEqual 1.0
      bounds.getMaxX mustEqual 2.0
      bounds.getMinY mustEqual 1.5
      bounds.getMaxY mustEqual 2.5

      ds.stats.getAttributeBounds[Date](ds.getSchema(shpFile), "dtg").map(_.tuple) must
          beSome((features.head.getAttribute("dtg"), features.last.getAttribute("dtg"), 2L))
    }

    "should support renaming the feature type" in {
      val command = connectedCommand(shpFile)
      command.params.featureName = "changed"
      command.execute()

      ds.stats.generateStats(ds.getSchema("changed")) // re-gen stats to invalidate cache

      val fs = ds.getFeatureSource("changed")

      SelfClosingIterator(fs.getFeatures.features).toList must haveLength(2)

      val bounds = fs.getBounds
      bounds.getMinX mustEqual 1.0
      bounds.getMaxX mustEqual 2.0
      bounds.getMinY mustEqual 1.5
      bounds.getMaxY mustEqual 2.5

      ds.stats.getAttributeBounds[Date](ds.getSchema("changed"), "dtg").map(_.tuple) must
          beSome((features.head.getAttribute("dtg"), features.last.getAttribute("dtg"), 2L))
    }

    "reproject to 4326 automatically on ingest" in {
      connectedCommand(shpFileToReproject).execute()

      ds.stats.generateStats(ds.getSchema(shpFileToReproject)) // re-gen stats to invalidate cache

      val fs = ds.getFeatureSource(shpFileToReproject)

      SelfClosingIterator(fs.getFeatures.features).toList must haveLength(2)

      val bounds = fs.getBounds
      bounds.getMinX must beCloseTo(1.0, 0.0001)
      bounds.getMaxX must beCloseTo(2.0, 0.0001)
      bounds.getMinY must beCloseTo(1.5, 0.0001)
      bounds.getMaxY must beCloseTo(2.5, 0.0001)

      ds.stats.getAttributeBounds[Date](ds.getSchema(shpFileToReproject), "dtg").map(_.tuple) must
        beSome((features.head.getAttribute("dtg"), features.last.getAttribute("dtg"), 2L))
    }

    "reproject to 4326 automatically on ingest with flipped inputs" in {
      connectedCommand(shpFileToReproject2).execute()

      ds.stats.generateStats(ds.getSchema(shpFileToReproject2)) // re-gen stats to invalidate cache

      val fs = ds.getFeatureSource(shpFileToReproject2)

      SelfClosingIterator(fs.getFeatures.features).toList must haveLength(2)

      val bounds = fs.getBounds
      bounds.getMinX must beCloseTo(1.0, 0.0001)
      bounds.getMaxX must beCloseTo(2.0, 0.0001)
      bounds.getMinY must beCloseTo(1.5, 0.0001)
      bounds.getMaxY must beCloseTo(2.5, 0.0001)

      ds.stats.getAttributeBounds[Date](ds.getSchema(shpFileToReproject2), "dtg").map(_.tuple) must
        beSome((features.head.getAttribute("dtg"), features.last.getAttribute("dtg"), 2L))
    }

    "skip records with null dates by default" in {
      connectedCommand(shpFileWithNullDates).execute()

      ds.stats.generateStats(ds.getSchema(shpFileWithNullDates)) // re-gen stats to invalidate cache

      val fs = ds.getFeatureSource(shpFileWithNullDates)

      SelfClosingIterator(fs.getFeatures.features).toList must haveLength(3)

      val bounds = fs.getBounds
      bounds.getMinX mustEqual 1.0
      bounds.getMaxX mustEqual 4.0
      bounds.getMinY mustEqual 1.5
      bounds.getMaxY mustEqual 2.5

      ds.stats.getAttributeBounds[Date](ds.getSchema(shpFileWithNullDates), "dtg").map(_.tuple) must
        beSome((features.head.getAttribute("dtg"), featuresWithNulls.last.getAttribute("dtg"), 3L))
    }

    // In this test, the third record has a null date. Raising an error stops the ingest.
    "raise error on null dates via override" in {
      Modes.ErrorMode.systemProperty.threadLocalValue.set("raise-errors")
      try {
        val command = connectedCommand(shpFileWithNullDates)
        command.params.featureName = "nullDates2"
        command.execute()
      } finally {
        Modes.ErrorMode.systemProperty.threadLocalValue.remove()
      }

      ds.stats.generateStats(ds.getSchema("nullDates2")) // re-gen stats to invalidate cache

      val fs = ds.getFeatureSource("nullDates2")

      SelfClosingIterator(fs.getFeatures.features).toList must haveLength(2)

      val bounds = fs.getBounds
      bounds.getMinX mustEqual 1.0
      bounds.getMaxX mustEqual 2.0
      bounds.getMinY mustEqual 1.5
      bounds.getMaxY mustEqual 2.5

      ds.stats.getAttributeBounds[Date](ds.getSchema("nullDates2"), "dtg").map(_.tuple) must
        beSome((features.head.getAttribute("dtg"), features.last.getAttribute("dtg"), 2L))
    }

    "index no data with null dates with batch parse mode and raise-errors via override" in {
      Modes.ErrorMode.systemProperty.threadLocalValue.set("raise-errors")
      Modes.ParseMode.systemProperty.threadLocalValue.set("batch")

      try {
        val command = connectedCommand(shpFileWithNullDates)
        command.params.featureName = "nullDates4"
        command.execute()
      } finally {
        Modes.ErrorMode.systemProperty.threadLocalValue.remove()
        Modes.ParseMode.systemProperty.threadLocalValue.remove()
      }

      val fs = ds.getFeatureSource("nullDates4")
      SelfClosingIterator(fs.getFeatures.features).toList must haveLength(0)
    }
  }

  step {
    if (dir != null) {
      PathUtils.deleteRecursively(dir)
    }
  }
}
