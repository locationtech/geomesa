/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.tools

import java.io.File
import java.text.SimpleDateFormat

import com.google.common.io.Files
import com.vividsolutions.jts.geom.Coordinate
import org.geotools.data.Transaction
import org.geotools.data.shapefile.ShapefileDataStoreFactory
import org.geotools.factory.Hints
import org.geotools.geometry.jts.JTSFactoryFinder
import org.joda.time.DateTime
import org.junit.runner.RunWith
import org.locationtech.geomesa.tools.commands.IngestCommand.IngestParameters
import org.locationtech.geomesa.utils.geotools.Conversions._
import org.locationtech.geomesa.utils.geotools.{GeneralShapefileIngest, SimpleFeatureTypes}
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

    val ingestParams = new IngestParameters()
    ingestParams.instance = "mycloud"
    ingestParams.zookeepers = "zoo1,zoo2,zoo3"
    ingestParams.user = "myuser"
    ingestParams.password = "mypassword"
    ingestParams.catalog = "testshpingestcatalog"
    ingestParams.useMock = true

    val ds = new DataStoreHelper(ingestParams).getOrCreateDs

    "should properly ingest a shapefile" >> {
      ingestParams.files.add(shpFile.getPath)
      GeneralShapefileIngest.shpToDataStore(ingestParams.files(0), ds, ingestParams.featureName)

      val fs = ds.getFeatureSource("shpingest")

      val bounds = fs.getBounds
      bounds.getMinX mustEqual minX
      bounds.getMaxX mustEqual maxX
      bounds.getMinY mustEqual minY
      bounds.getMaxY mustEqual maxY

      val timeBounds = ds.getTimeBounds("shpingest")
      timeBounds.getStart mustEqual new DateTime(minDate)
      timeBounds.getEnd mustEqual new DateTime(maxDate)

      val result = fs.getFeatures.features().toList
      result.length mustEqual 2
    }

    "should support renaming the feature type" >> {
      ingestParams.featureName = "changed"
      GeneralShapefileIngest.shpToDataStore(ingestParams.files(0), ds, ingestParams.featureName)

      val fs = ds.getFeatureSource("changed")

      val timeBounds = ds.getTimeBounds("changed")
      timeBounds.getStart mustEqual new DateTime(minDate)
      timeBounds.getEnd mustEqual new DateTime(maxDate)

      val bounds = fs.getBounds
      bounds.getMinX mustEqual minX
      bounds.getMaxX mustEqual maxX
      bounds.getMinY mustEqual minY
      bounds.getMaxY mustEqual maxY

      val result = fs.getFeatures.features().toList
      result.length mustEqual 2
    }
  }
}
