/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.tools.ingest

import java.io.File
import java.nio.file.{Files, Path}
import java.util.concurrent.atomic.AtomicInteger
import java.util.{Collections, Date}

import org.geotools.data.Transaction
import org.geotools.data.shapefile.ShapefileDataStore
import org.geotools.data.store.ReprojectingFeatureCollection
import org.geotools.referencing.CRS
import org.geotools.util.factory.Hints
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.MiniCluster
import org.locationtech.geomesa.convert.Modes
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.{PathUtils, WithClose, WithStore}
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ShpIngestTest extends Specification {

  private val sftCounter = new AtomicInteger(0)

  // note: shpfile always puts geom first
  val sft = SimpleFeatureTypes.createType(getClass.getSimpleName, "*geom:Point:srid=4326,age:Integer,dtg:Date")

  val features = Seq(
    ScalaSimpleFeature.create(sft, "1", "POINT(1.0 1.5)", 1, "2011-01-01T00:00:00.000Z"),
    ScalaSimpleFeature.create(sft, "2", "POINT(2.0 2.5)", 2, "2012-01-01T00:00:00.000Z")
  )

  val featuresWithNulls = features ++ Seq(
    ScalaSimpleFeature.create(sft, "3", "POINT(3.0 1.5)", 3, null),
    ScalaSimpleFeature.create(sft, "4", "POINT(4.0 2.5)", 4, "2013-01-01T00:00:00.000Z")
  )

  def createCommand(file: String): AccumuloIngestCommand = {
    val command = new AccumuloIngestCommand()
    command.params.user        = MiniCluster.Users.root.name
    command.params.instance    = MiniCluster.cluster.getInstanceName
    command.params.zookeepers  = MiniCluster.cluster.getZooKeepers
    command.params.password    = MiniCluster.Users.root.password
    command.params.catalog     = s"${MiniCluster.namespace}.${getClass.getSimpleName}${sftCounter.getAndIncrement()}"
    command.params.force       = true
    command.params.files       = Collections.singletonList(new File(dir.toFile, s"$file.shp").getAbsolutePath)
    command.params.compact     = false
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
      val command = createCommand(shpFile)
      command.execute()

      command.withDataStore { ds =>
        ds.stats.writer.analyze(ds.getSchema(shpFile)) // re-gen stats to invalidate cache

        val fs = ds.getFeatureSource(shpFile)

        SelfClosingIterator(fs.getFeatures.features).toList must haveLength(2)

        val bounds = fs.getBounds
        bounds.getMinX mustEqual 1.0
        bounds.getMaxX mustEqual 2.0
        bounds.getMinY mustEqual 1.5
        bounds.getMaxY mustEqual 2.5

        ds.stats.getMinMax[Date](ds.getSchema(shpFile), "dtg").map(_.tuple) must
            beSome((features.head.getAttribute("dtg"), features.last.getAttribute("dtg"), 2L))
      }
    }

    "should support renaming the feature type" in {
      val command = createCommand(shpFile)
      command.params.featureName = "changed"
      command.execute()

      command.withDataStore { ds =>
        ds.stats.writer.analyze(ds.getSchema("changed")) // re-gen stats to invalidate cache

        val fs = ds.getFeatureSource("changed")

        SelfClosingIterator(fs.getFeatures.features).toList must haveLength(2)

        val bounds = fs.getBounds
        bounds.getMinX mustEqual 1.0
        bounds.getMaxX mustEqual 2.0
        bounds.getMinY mustEqual 1.5
        bounds.getMaxY mustEqual 2.5

        ds.stats.getMinMax[Date](ds.getSchema("changed"), "dtg").map(_.tuple) must
            beSome((features.head.getAttribute("dtg"), features.last.getAttribute("dtg"), 2L))
      }
    }

    "reproject to 4326 automatically on ingest" in {
      val command = createCommand(shpFileToReproject)
      command.execute()

      command.withDataStore { ds =>
        ds.stats.writer.analyze(ds.getSchema(shpFileToReproject)) // re-gen stats to invalidate cache

        val fs = ds.getFeatureSource(shpFileToReproject)

        SelfClosingIterator(fs.getFeatures.features).toList must haveLength(2)

        val bounds = fs.getBounds
        bounds.getMinX must beCloseTo(1.0, 0.0001)
        bounds.getMaxX must beCloseTo(2.0, 0.0001)
        bounds.getMinY must beCloseTo(1.5, 0.0001)
        bounds.getMaxY must beCloseTo(2.5, 0.0001)

        ds.stats.getMinMax[Date](ds.getSchema(shpFileToReproject), "dtg").map(_.tuple) must
          beSome((features.head.getAttribute("dtg"), features.last.getAttribute("dtg"), 2L))
      }
    }

    "reproject to 4326 automatically on ingest with flipped inputs" in {
      val command = createCommand(shpFileToReproject2)
      command.execute()

      command.withDataStore { ds =>
        ds.stats.writer.analyze(ds.getSchema(shpFileToReproject2)) // re-gen stats to invalidate cache

        val fs = ds.getFeatureSource(shpFileToReproject2)

        SelfClosingIterator(fs.getFeatures.features).toList must haveLength(2)

        val bounds = fs.getBounds
        bounds.getMinX must beCloseTo(1.0, 0.0001)
        bounds.getMaxX must beCloseTo(2.0, 0.0001)
        bounds.getMinY must beCloseTo(1.5, 0.0001)
        bounds.getMaxY must beCloseTo(2.5, 0.0001)

        ds.stats.getMinMax[Date](ds.getSchema(shpFileToReproject2), "dtg").map(_.tuple) must
          beSome((features.head.getAttribute("dtg"), features.last.getAttribute("dtg"), 2L))
      }
    }

    "skip records with null dates by default" in {
      val command = createCommand(shpFileWithNullDates)
      command.execute()

      command.withDataStore { ds =>
        ds.stats.writer.analyze(ds.getSchema(shpFileWithNullDates)) // re-gen stats to invalidate cache

        val fs = ds.getFeatureSource(shpFileWithNullDates)

        SelfClosingIterator(fs.getFeatures.features).toList must haveLength(3)

        val bounds = fs.getBounds
        bounds.getMinX mustEqual 1.0
        bounds.getMaxX mustEqual 4.0
        bounds.getMinY mustEqual 1.5
        bounds.getMaxY mustEqual 2.5

        ds.stats.getMinMax[Date](ds.getSchema(shpFileWithNullDates), "dtg").map(_.tuple) must
          beSome((features.head.getAttribute("dtg"), featuresWithNulls.last.getAttribute("dtg"), 3L))
      }
    }

    // In this test, the third record has a null date. Raising an error stops the ingest.
    "raise error on null dates via override" in {
      val command = createCommand(shpFileWithNullDates)
      command.params.featureName = "nullDates2"

      Modes.ErrorMode.systemProperty.threadLocalValue.set("raise-errors")
      try { command.execute() } finally {
        Modes.ErrorMode.systemProperty.threadLocalValue.remove()
      }

      command.withDataStore { ds =>
        ds.stats.writer.analyze(ds.getSchema("nullDates2")) // re-gen stats to invalidate cache

        val fs = ds.getFeatureSource("nullDates2")

        SelfClosingIterator(fs.getFeatures.features).toList must haveLength(2)

        val bounds = fs.getBounds
        bounds.getMinX mustEqual 1.0
        bounds.getMaxX mustEqual 2.0
        bounds.getMinY mustEqual 1.5
        bounds.getMaxY mustEqual 2.5

        ds.stats.getMinMax[Date](ds.getSchema("nullDates2"), "dtg").map(_.tuple) must
          beSome((features.head.getAttribute("dtg"), features.last.getAttribute("dtg"), 2L))
      }
    }

    "index no data with null dates with batch parse mode and raise-errors via override" in {
      val command = createCommand(shpFileWithNullDates)
      command.params.featureName = "nullDates4"

      Modes.ErrorMode.systemProperty.threadLocalValue.set("raise-errors")
      Modes.ParseMode.systemProperty.threadLocalValue.set("batch")
      try { command.execute() } finally {
        Modes.ErrorMode.systemProperty.threadLocalValue.remove()
        Modes.ParseMode.systemProperty.threadLocalValue.remove()
      }

      command.withDataStore { ds =>
        val fs = ds.getFeatureSource("nullDates4")
        SelfClosingIterator(fs.getFeatures.features).toList must beEmpty
      }
    }
  }

  step {
    if (dir != null) {
      PathUtils.deleteRecursively(dir)
    }
  }
}
