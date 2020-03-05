/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.process.tube

import java.util.Date

import org.geotools.data.collection.ListFeatureCollection
import org.geotools.feature.DefaultFeatureCollection
import org.geotools.filter.text.cql2.CQL
import org.geotools.filter.text.ecql.ECQL
import org.geotools.referencing.GeodeticCalculator
import org.geotools.util.Converters
import org.geotools.util.factory.Hints
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithMultipleSfts
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.features.avro.AvroSimpleFeatureFactory
import org.locationtech.geomesa.index.conf.QueryProperties
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.geomesa.utils.text.WKTUtils
import org.locationtech.jts.geom.{Coordinate, GeometryFactory, Point}
import org.opengis.filter.Filter
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class TubeSelectProcessTest extends TestWithMultipleSfts {

  sequential

  lazy val sft1 = createNewSchema("type:String,*geom:Point:srid=4326,dtg:Date")

  lazy val sft2 = createNewSchema("type:String,*geom:Point:srid=4326,dtg:Date")

  lazy val sft3 = createNewSchema("type:String,*geom:Geometry:srid=4326,dtg:Date;geomesa.mixed.geometries=true")

  "TubeSelect" should {
    "work with an empty input collection" in {
      addFeatures {
        List("a", "b").flatMap { name =>
          List(1, 2, 3, 4).zip(List(45, 46, 47, 48)).map { case (i, lat) =>
            ScalaSimpleFeature.create(sft1, s"$name$i", name, f"POINT($lat%d $lat%d)", "2011-01-01T00:00:00Z")
          }
        }
      }

      val fs = ds.getFeatureSource(sft1.getTypeName)

      // result set to tube on
      val features = fs.getFeatures(CQL.toFilter("type <> 'a'"))

      forall(Seq("nofill", "line")) { fill =>
        forall(Seq((ECQL.toFilter("in('a1')"), true), (Filter.EXCLUDE, false))) { case (filter, next) =>
          // tube features
          val tubeFeatures = fs.getFeatures(filter)
          val ts = new TubeSelectProcess()
          WithClose(ts.execute(tubeFeatures, features, null, 1L, 1L, 0.0, 5, fill).features()) { results =>
            results.hasNext mustEqual next
          }
        }
      }
    }

    "should do a simple tube with geo interpolation" in {
      val fs = ds.getFeatureSource(sft1.getTypeName)

      // tube features
      val tubeFeatures = fs.getFeatures(CQL.toFilter("type = 'a'"))

      // result set to tube on
      val features = fs.getFeatures(CQL.toFilter("type <> 'a'"))

      // get back type b from tube
      val ts = new TubeSelectProcess()
      val results = ts.execute(tubeFeatures, features, null, 1L, 1L, 0.0, 5, null)

      forall(SelfClosingIterator(results.features()).toList)(_.getAttribute("type") mustEqual "b")
      results.size mustEqual 4
    }

    "should do a simple tube with geo + time interpolation" in {
      val fs = ds.getFeatureSource(sft1.getTypeName)

      addFeatures {
        List("c").flatMap { name =>
          List(1, 2, 3, 4).zip(List(45, 46, 47, 48)).map { case (i, lat) =>
            ScalaSimpleFeature.create(sft1, s"$name$i", name, f"POINT($lat%d $lat%d)", "2011-01-02T00:00:00Z")
          }
        }
      }

      // tube features
      val tubeFeatures = fs.getFeatures(CQL.toFilter("type = 'a'"))

      // result set to tube on
      val features = fs.getFeatures(CQL.toFilter("type <> 'a'"))

      // get back type b from tube
      val ts = new TubeSelectProcess()
      val results = ts.execute(tubeFeatures, features, null, 1L, 1L, 0.0, 5, null)

      forall(SelfClosingIterator(results.features()).toList)(_.getAttribute("type") mustEqual "b")
      results.size mustEqual 4
    }

    "should properly convert speed/time to distance" in {
      var i = -1
      addFeatures {
        List("a", "b").flatMap { name =>
          for (lon <- 40 until 50; lat <- 40 until 50) yield {
            i += 1
            ScalaSimpleFeature.create(sft2, s"$name$i", name, f"POINT($lon%d $lat%d)", "2011-01-02T00:00:00Z")
          }
        }
      }

      val fs = ds.getFeatureSource(sft2.getTypeName)

      // tube features
      val tubeFeatures = fs.getFeatures(CQL.toFilter("BBOX(geom, 39.999999999,39.999999999, 40.00000000001, 50.000000001) AND type = 'a'"))

      // result set to tube on
      val features = fs.getFeatures(CQL.toFilter("type <> 'a'"))

      // get back type b from tube
      val ts = new TubeSelectProcess()

      // 110 m/s times 1000 seconds is just 100km which is under 1 degree
      val results = ts.execute(tubeFeatures, features, null, 110L, 1000L, 0.0, 5, null)

      forall(SelfClosingIterator(results.features()).toList) { sf =>
        sf.getAttribute("type") mustEqual "b"
        val point = sf.getDefaultGeometry.asInstanceOf[Point]
        point.getX mustEqual 40.0
        point.getY must beBetween(40.0, 50.0)
      }
      results.size mustEqual 10
    }

    "should properly dedup overlapping results based on buffer size " in {
      val fs = ds.getFeatureSource(sft2.getTypeName)

      // tube features
      val tubeFeatures = fs.getFeatures(CQL.toFilter("BBOX(geom, 39.999999999,39.999999999, 40.00000000001, 50.000000001) AND type = 'a'"))

      // result set to tube on
      val features = fs.getFeatures(CQL.toFilter("type <> 'a'"))

      // get back type b from tube
      val ts = new TubeSelectProcess()

      // this time we use 112km which is just over 1 degree so we should pick up additional features
      // but with buffer overlap since the features in the collection are 1 degrees apart
      val results = ts.execute(tubeFeatures, features, null, 112L, 1000L, 0.0, 5, null)


      forall(SelfClosingIterator(results.features()).toList) { sf =>
        sf.getAttribute("type") mustEqual "b"
        val point = sf.getDefaultGeometry.asInstanceOf[Point]
        point.getX must beBetween(40.0, 41.0)
        point.getY must beBetween(40.0, 50.0)
      }

      results.size mustEqual 20
    }

    "should properly handle geometries crossing the IDL" in {
      val fs = ds.getFeatureSource(sft1.getTypeName)

      addFeatures {
        List("c", "d").flatMap { name =>
          List(1, 2, 3, 4).zip(List(-178, -179, 179, 178)).map { case (i, lat) =>
            ScalaSimpleFeature.create(sft1, s"$name$i", name, f"POINT($lat%d 0)", f"2011-01-01T00:0$i%d:00Z")
          }
        }
      }

      // Add a point that would be in the buffer if it wrapped around the earth
      // This point falls between the time of C2 & C3 but west of C2. This point
      // would be grabbed if the buffer wrapped the globe due to the IDL.
      addFeature(ScalaSimpleFeature.create(sft1, "d5", "d", "POINT(-178.5 0)", "2011-01-01T00:02:03Z"))

      // tube features
      val tubeFeatures = fs.getFeatures(CQL.toFilter("type = 'c'"))

      // result set to tube on
      val features = fs.getFeatures(CQL.toFilter("type <> 'c'"))

      // get back type d from tube
      val ts = new TubeSelectProcess()
      val results = ts.execute(tubeFeatures, features, null, 1L, 1L, 0.0, 5, "line")

      forall(SelfClosingIterator(results.features()).toList)(_.getAttribute("type") mustEqual "d")

      results.size mustEqual 4
    }

    "properly interpolate times and geometries" in {
      val fs = ds.getFeatureSource(sft1.getTypeName)

      val featureCollection = new DefaultFeatureCollection(sft1.getTypeName, sft1)
      //Make data that actually emulated a track, and two alternate tracks, one offset from the other by a small amount of time
      val calc = new GeodeticCalculator()
      //Configure track generation for 1 hour at 10.2 meters per second @ 80 degrees starting at -141.0, 35.0
      var curPoint = WKTUtils.read("POINT(-141.0 35.0)")
      val timePerPoint = 150000L  //2.5 minutes in milliseconds
      val speed =.0102 // meters/millisecond
      val startTime = Converters.convert(f"2011-01-01T00:00:00Z", classOf[Date]).getTime
      val endTime = Converters.convert(f"2011-01-01T01:00:00Z", classOf[Date]).getTime
      val heading = 80.0
      //Validation values
      var i=0                     //Index for providing an FID to each point
      var trackCount = 0          //Count number of track points
      var poolCount = 0           //Count number of valid pool points
      var invalidPoolCount = 0    //Count number of invalid pool points (those back in time)

      (startTime to endTime).by(timePerPoint).foreach { t =>
        i+=1
        val sf = AvroSimpleFeatureFactory.buildAvroFeature(sft1, List(), i.toString)
        sf.setDefaultGeometry(curPoint)
        sf.setAttribute("dtg", new Date(t))
        sf.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE
        featureCollection.add(sf)
        if((t - startTime) % 600000 == 0 ) {         //Create a track point every 10 minutes
          sf.setAttribute("type","track")
          trackCount+=1
        } else if ((t - startTime) % 450000 == 0) { //Create invalid pool points every 7.5 minutes, but put them back in time.
          sf.setAttribute("dtg", new Date(t-450000L))
          sf.setAttribute("type", "pool2")
          invalidPoolCount+=1
        } else {                                    //Else create valid pool points where they ought to be in time/space
          sf.setAttribute("type", "pool")
          poolCount+=1
        }
        featureCollection.add(sf)
        //Increment for next point
        calc.setStartingGeographicPoint(curPoint.getCoordinate.x, curPoint.getCoordinate.y)
        calc.setDirection(heading, speed*timePerPoint)
        val next = calc.getDestinationGeographicPoint
        val nextPointX = next.getX
        val nextPointY = next.getY
        curPoint = WKTUtils.read(f"POINT($nextPointX%f $nextPointY%f)")
      }

      // write the feature to the store
      fs.addFeatures(featureCollection)

      // tube features
      val tubeFeatures = fs.getFeatures(CQL.toFilter("type = 'track'"))

      // result set to tube on
      val features = fs.getFeatures(CQL.toFilter("type = 'pool' or type = 'pool2'"))

      // get back type b from tube
      val ts = new TubeSelectProcess()
      val results = ts.execute(tubeFeatures, features, null, 1L, 1L, 1000.0, 5, "interpolated")
      forall(SelfClosingIterator(results.features()).toList)(_.getAttribute("type") mustEqual "pool")

      results.size mustEqual poolCount
    }

    "handle all geometries" in {
      addFeatures {
        List("b").flatMap { name =>
          List(1, 2, 3, 4).zip(List(45, 46, 47, 48)).map { case (i, lat) =>
            ScalaSimpleFeature.create(sft3, s"$name$i", name, f"POINT(40 $lat%d)", "2011-01-01T00:00:00Z")
          }
        }
      }
      addFeature {
        ScalaSimpleFeature.create(sft3, "b-line", "b", "LINESTRING(40 40, 40 50)", "2011-01-01T00:00:00Z")
      }
      addFeature {
        ScalaSimpleFeature.create(sft3, "b-poly", "b", "POLYGON((40 40, 41 40, 41 41, 40 41, 40 40))", "2011-01-01T00:00:00Z")
      }

      // tube features
      val aLine = ScalaSimpleFeature.create(sft3, "a-line", "a", "LINESTRING(40 40, 40 50)", "2011-01-01T00:00:00Z")
      // aLine.setAttribute("end", new DateTime("2011-01-01T00:00:00Z", DateTimeZone.UTC).toDate)
      val tubeFeatures = new ListFeatureCollection(sft3, List(aLine))

      val fs = ds.getFeatureSource(sft3.getTypeName)

      // result set to tube on
      val features = fs.getFeatures(CQL.toFilter("type <> 'a'"))

      QueryProperties.QueryExactCount.threadLocalValue.set("true")
      try { features.size mustEqual 6 } finally {
        QueryProperties.QueryExactCount.threadLocalValue.remove()
      }

      // get back type b from tube
      val ts = new TubeSelectProcess()
      val results = ts.execute(tubeFeatures, features, null, 112L, 1L, 0.0, 5, null)

      forall(SelfClosingIterator(results.features()).toList)(_.getAttribute("type") mustEqual "b")

      QueryProperties.QueryExactCount.threadLocalValue.set("true")
      try { results.size mustEqual 6 } finally {
        QueryProperties.QueryExactCount.threadLocalValue.remove()
      }
    }

    "properly handle values for execute" in {
      val ts = new TubeSelectProcess

      val fs = ds.getFeatureSource(sft3.getTypeName)

      val res = fs.getFeatures()

      // tube features
      val aLine = ScalaSimpleFeature.create(sft3, "a-line", "a", "LINESTRING(40 40, 40 50)", "2011-01-01T00:00:00Z")
      // aLine.setAttribute("end", new DateTime("2011-01-01T00:00:00Z", DateTimeZone.UTC).toDate)
      val tubeFeatures = new ListFeatureCollection(sft3, List(aLine))

      // ensure null values work and don't throw exceptions
      ts.execute(tubeFeatures, res, null, null, null, null, null, null) should not(throwAn[ClassCastException])
    }

    "should do a simple tube with geo interpolation with a different date field" in {
      val sft = createNewSchema("type:String,*geom:Point:srid=4326,datefield:Date")

      val fs = ds.getFeatureSource(sft.getTypeName)

      addFeatures {
        List("a", "b").flatMap { name =>
          List(1, 2, 3, 4).zip(List(45, 46, 47, 48)).map { case (i, lat) =>
            ScalaSimpleFeature.create(sft, s"$name$i", name, f"POINT($lat%d $lat%d)", "2011-01-01T00:00:00Z")
          }
        }
      }

      // tube features
      val tubeFeatures = fs.getFeatures(CQL.toFilter("type = 'a'"))

      // result set to tube on
      val features = fs.getFeatures(CQL.toFilter("type <> 'a'"))

      // get back type b from tube
      val ts = new TubeSelectProcess()
      val results = ts.execute(tubeFeatures, features, null, 1L, 1L, 0.0, 5, null)

      forall(SelfClosingIterator(results.features()).toList)(_.getAttribute("type") mustEqual "b")
      results.size mustEqual 4
    }
  }

  "approximate meters to degrees" in {
    val geoFac = new GeometryFactory
    val gf = new NoGapFill(new DefaultFeatureCollection(sft1.getTypeName, sft1), 0, 0)
    // calculated km at various latitude by USGS
    forall(List(0, 30, 60, 89).zip(List(110.57, 110.85, 111.41, 111.69))) { case (lat, dist) =>
      val deg = gf.metersToDegrees(dist * 1000, geoFac.createPoint(new Coordinate(0, lat)))
      deg must beCloseTo(1d, .0001)
    }
  }
}
