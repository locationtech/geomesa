/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.tools.ingest

import java.time.LocalDateTime
import java.util.Date

import com.vividsolutions.jts.geom.{Coordinate, GeometryFactory, Point}
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.tools.{AccumuloDataStoreCommand, AccumuloRunner}
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.opengis.feature.simple.SimpleFeature
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class GeoJsonIngestTest extends Specification {

  sequential
  val baseArgs = Array[String]("ingest", "--zookeepers", "zoo", "--mock", "--instance", "mycloud", "--user", "myuser",
    "--password", "mypassword", "--catalog", "testshpingestcatalog")

  var features: List[SimpleFeature] = _

  val geomFactory = new GeometryFactory()
  val geoms = Seq(geomFactory.createPoint(new Coordinate(3, -62.23)),
                  geomFactory.createPoint(new Coordinate(-100.236523, 23)),
                  geomFactory.createPoint(new Coordinate(40.232, -53.2356)))

  "GeoJsonIngest" should {

    "ingest a geojson file" >> {
        val filePath = this.getClass.getClassLoader.getResource("examples/example1.json").getPath
        val args = baseArgs :+ filePath

        val command = AccumuloRunner.parseCommand(args).asInstanceOf[AccumuloDataStoreCommand]
        command.execute()

        val fs = command.withDataStore(_.getFeatureSource("geojson"))
        features = SelfClosingIterator(fs.getFeatures.features).toList
                  .sortBy(f => f.getAttribute("idField").asInstanceOf[Integer])
        features must haveLength(3)
      }

      "parse geometries correctly" >> {
        val geom0 = features(0).getAttribute("geometry").asInstanceOf[Point]
        val geom1 = features(1).getAttribute("geometry").asInstanceOf[Point]
        val geom2 = features(2).getAttribute("geometry").asInstanceOf[Point]
        geom0.equalsExact(geoms(0)) mustEqual true
        geom1.equalsExact(geoms(1)) mustEqual true
        geom2.equalsExact(geoms(2)) mustEqual true
      }

      "parse dates correctly" >> {
        val date0 = features(0).getAttribute("LastSeen").asInstanceOf[java.util.Date]
        val date1 = features(1).getAttribute("LastSeen").asInstanceOf[java.util.Date]
        val date2 = features(2).getAttribute("LastSeen").asInstanceOf[java.util.Date]
        date0 mustEqual Date.from(LocalDateTime.parse("2015-10-23T00:00:00").toInstant(java.time.ZoneOffset.UTC))
        date1 mustEqual Date.from(LocalDateTime.parse("2015-05-06T00:00:00").toInstant(java.time.ZoneOffset.UTC))
        date2 mustEqual Date.from(LocalDateTime.parse("2015-06-07T00:00:00").toInstant(java.time.ZoneOffset.UTC))
      }

      "parse arrays correctly" >> {
        val arr0 = features(0).getAttribute("Friends").asInstanceOf[java.util.ArrayList[String]]
        val arr1 = features(1).getAttribute("Friends").asInstanceOf[java.util.ArrayList[String]]
        val arr2 = features(2).getAttribute("Friends").asInstanceOf[java.util.ArrayList[String]]
        arr0 must haveLength(3)
        arr1 must haveLength(3)
        arr2 must haveLength(3)
      }
    }

}
