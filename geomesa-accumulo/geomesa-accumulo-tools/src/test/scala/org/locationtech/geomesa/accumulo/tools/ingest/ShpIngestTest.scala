/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.tools.ingest

import java.io.File
import java.text.SimpleDateFormat
import java.util.Date

import com.google.common.io.Files
import com.vividsolutions.jts.geom.Coordinate
import org.geotools.data.Transaction
import org.geotools.data.shapefile.ShapefileDataStoreFactory
import org.geotools.factory.Hints
import org.geotools.geometry.jts.JTSFactoryFinder
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.tools.{AccumuloDataStoreCommand, AccumuloRunner}
import org.locationtech.geomesa.index.stats.AttributeBounds
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class ShpIngestTest extends Specification {

  sequential

  "ShpIngest" >> {

    val geomBuilder = JTSFactoryFinder.getGeometryFactory

    val shpStoreFactory = new ShapefileDataStoreFactory
    val shpFile = new File(Files.createTempDir(), "shpingest.shp")
    val shpUrl = shpFile.toURI.toURL
    val params = Map("url" -> shpUrl)
    val shpStore = shpStoreFactory.createNewDataStore(params)
    val schema = SimpleFeatureTypes.createType("shpingest", "age:Integer,dtg:Date,*geom:Point:srid=4326")
    shpStore.createSchema(schema)
    val df = new SimpleDateFormat("dd-MM-yyyy")
    val (minDate, maxDate) = (df.parse("01-01-2011"), df.parse("01-01-2012"))
    val (minX, maxX, minY, maxY) = (10.0, 20.0, 30.0, 40.0)
    val data =
      List(
        ("1", 1, minDate, (minX, minY)),
        ("1", 2, maxDate, (maxX, maxY))
      )
    val writer = shpStore.getFeatureWriterAppend("shpingest", Transaction.AUTO_COMMIT)
    data.foreach { case (id, age, dtg, (lat, lon)) =>
      val f = writer.next()
      f.setAttribute("age", age)
      f.setAttribute("dtg", dtg)
      val pt = geomBuilder.createPoint(new Coordinate(lat, lon))
      f.setDefaultGeometry(pt)
      f.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
      f.getUserData.put(Hints.PROVIDED_FID, id)
      writer.write()
    }
    writer.flush()
    writer.close()

    val args = Array[String]("ingest", "--zookeepers", "zoo", "--mock", "--instance", "mycloud", "--user", "myuser",
      "--password", "mypassword", "--catalog", "testshpingestcatalog", shpFile.getAbsolutePath)

    "should properly ingest a shapefile" >> {
      val command = AccumuloRunner.parseCommand(args).asInstanceOf[AccumuloDataStoreCommand]
      command.execute()

      val fs = command.withDataStore(_.getFeatureSource("shpingest"))

      SelfClosingIterator(fs.getFeatures.features).toList must haveLength(2)

      val bounds = fs.getBounds
      bounds.getMinX mustEqual minX
      bounds.getMaxX mustEqual maxX
      bounds.getMinY mustEqual minY
      bounds.getMaxY mustEqual maxY

      command.withDataStore { (ds) =>
        ds.stats.getAttributeBounds[Date](ds.getSchema("shpingest"), "dtg") must
            beSome(AttributeBounds(minDate, maxDate, 2))
      }
    }

    "should support renaming the feature type" >> {
      val newArgs = Array(args.head) ++ Array("--feature-name", "changed") ++ args.tail
      val command = AccumuloRunner.parseCommand(newArgs).asInstanceOf[AccumuloDataStoreCommand]
      command.execute()

      val fs = command.withDataStore(_.getFeatureSource("changed"))

      SelfClosingIterator(fs.getFeatures.features).toList must haveLength(2)

      val bounds = fs.getBounds
      bounds.getMinX mustEqual minX
      bounds.getMaxX mustEqual maxX
      bounds.getMinY mustEqual minY
      bounds.getMaxY mustEqual maxY

      command.withDataStore { (ds) =>
        ds.stats.getAttributeBounds[Date](ds.getSchema("changed"), "dtg") must
            beSome(AttributeBounds(minDate, maxDate, 2))
      }
    }
  }
}
