/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.process.tube

import java.util.Date

import org.locationtech.jts.geom.{Coordinate, GeometryFactory, Point}
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.data.{DataStoreFinder, Query}
import org.geotools.factory.Hints
import org.geotools.feature.DefaultFeatureCollection
import org.geotools.filter.text.cql2.CQL
import org.geotools.filter.text.ecql.ECQL
import org.geotools.referencing.GeodeticCalculator
import org.geotools.util.Converters
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, AccumuloDataStoreParams}
import org.locationtech.geomesa.features.avro.AvroSimpleFeatureFactory
import org.locationtech.geomesa.index.conf.QueryProperties
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.filter.Filter
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class TubeSelectProcessTest extends Specification {

  import TubeBuilder.DefaultDtgField

  sequential

  val geotimeAttributes = s"*geom:Point:srid=4326,$DefaultDtgField:Date"

  def createStore: AccumuloDataStore =
  // the specific parameter values should not matter, as we
  // are requesting a mock data store connection to Accumulo
    DataStoreFinder.getDataStore(Map(
      AccumuloDataStoreParams.InstanceIdParam.key -> "mycloud",
      AccumuloDataStoreParams.ZookeepersParam.key -> "zoo1:2181,zoo2:2181,zoo3:2181",
      AccumuloDataStoreParams.UserParam.key       -> "myuser",
      AccumuloDataStoreParams.PasswordParam.key   -> "mypassword",
      AccumuloDataStoreParams.AuthsParam.key      -> "A,B,C",
      AccumuloDataStoreParams.CatalogParam.key    -> "testtube",
      AccumuloDataStoreParams.MockParam.key       -> "true")).asInstanceOf[AccumuloDataStore]

  "TubeSelect" should {
    "work with an empty input collection" in {
      val sftName = "emptyTubeTestType"
      val sft = SimpleFeatureTypes.createType(sftName, s"type:String,$geotimeAttributes")

      val ds = createStore

      ds.createSchema(sft)
      val fs = ds.getFeatureSource(sftName)

      val featureCollection = new DefaultFeatureCollection(sftName, sft)

      List("a", "b").foreach { name =>
        List(1, 2, 3, 4).zip(List(45, 46, 47, 48)).foreach { case (i, lat) =>
          val sf = AvroSimpleFeatureFactory.buildAvroFeature(sft, List(), name + i.toString)
          sf.setDefaultGeometry(WKTUtils.read(f"POINT($lat%d $lat%d)"))
          sf.setAttribute(DefaultDtgField, "2011-01-01T00:00:00Z")
          sf.setAttribute("type", name)
          sf.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE
          featureCollection.add(sf)
        }
      }

      // write the feature to the store
      val res = fs.addFeatures(featureCollection)

      // result set to tube on
      val features = fs.getFeatures(CQL.toFilter("type <> 'a'"))

      forall(Seq("nofill", "line")) { fill =>
        forall(Seq((ECQL.toFilter("in('a1')"), true), (Filter.EXCLUDE, false))) { case (filter, next) =>
          // tube features
          val tubeFeatures = fs.getFeatures(filter)
          val ts = new TubeSelectProcess()
          val results = ts.execute(tubeFeatures, features, null, 1L, 1L, 0.0, 5, fill)
          results.features().hasNext mustEqual next
        }

      }
    }

    "should do a simple tube with geo interpolation" in {
      val sftName = "tubeTestType"
      val sft = SimpleFeatureTypes.createType(sftName, s"type:String,$geotimeAttributes")

      val ds = createStore

      ds.createSchema(sft)
      val fs = ds.getFeatureSource(sftName)

      val featureCollection = new DefaultFeatureCollection(sftName, sft)

      List("a", "b").foreach { name =>
        List(1, 2, 3, 4).zip(List(45, 46, 47, 48)).foreach { case (i, lat) =>
          val sf = AvroSimpleFeatureFactory.buildAvroFeature(sft, List(), name + i.toString)
          sf.setDefaultGeometry(WKTUtils.read(f"POINT($lat%d $lat%d)"))
          sf.setAttribute(DefaultDtgField, "2011-01-01T00:00:00Z")
          sf.setAttribute("type", name)
          sf.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE
          featureCollection.add(sf)
        }
      }

      // write the feature to the store
      val res = fs.addFeatures(featureCollection)

      // tube features
      val tubeFeatures = fs.getFeatures(CQL.toFilter("type = 'a'"))

      // result set to tube on
      val features = fs.getFeatures(CQL.toFilter("type <> 'a'"))

      // get back type b from tube
      val ts = new TubeSelectProcess()
      val results = ts.execute(tubeFeatures, features, null, 1L, 1L, 0.0, 5, null)

      val f = results.features()
      while (f.hasNext) {
        val sf = f.next
        sf.getAttribute("type") mustEqual "b"
      }

      results.size mustEqual 4
    }

    "should do a simple tube with geo + time interpolation" in {
      val sftName = "tubeTestType"
      val sft = SimpleFeatureTypes.createType(sftName, s"type:String,$geotimeAttributes")

      val ds = createStore
      val fs = ds.getFeatureSource(sftName)

      val featureCollection = new DefaultFeatureCollection(sftName, sft)

      List("c").foreach { name =>
        List(1, 2, 3, 4).zip(List(45, 46, 47, 48)).foreach { case (i, lat) =>
          val sf = AvroSimpleFeatureFactory.buildAvroFeature(sft, List(), name + i.toString)
          sf.setDefaultGeometry(WKTUtils.read(f"POINT($lat%d $lat%d)"))
          sf.setAttribute(DefaultDtgField, "2011-01-02T00:00:00Z")
          sf.setAttribute("type", name)
          sf.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE
          featureCollection.add(sf)
        }
      }

      // write the feature to the store
      fs.addFeatures(featureCollection)

      // tube features
      val tubeFeatures = fs.getFeatures(CQL.toFilter("type = 'a'"))

      // result set to tube on
      val features = fs.getFeatures(CQL.toFilter("type <> 'a'"))

      // get back type b from tube
      val ts = new TubeSelectProcess()
      val results = ts.execute(tubeFeatures, features, null, 1L, 1L, 0.0, 5, null)

      val f = results.features()
      while (f.hasNext) {
        val sf = f.next
        sf.getAttribute("type") mustEqual "b"
      }

      results.size mustEqual 4
    }

    "should properly convert speed/time to distance" in {
      val sftName = "tubetest2"
      val sft = SimpleFeatureTypes.createType(sftName, s"type:String,$geotimeAttributes")

      val ds = createStore

      ds.createSchema(sft)

      val fs = ds.getFeatureSource(sftName)

      val featureCollection = new DefaultFeatureCollection(sftName, sft)

      var i = 0
      List("a", "b").foreach { name =>
        for (lon <- 40 until 50; lat <- 40 until 50) {
          val sf = AvroSimpleFeatureFactory.buildAvroFeature(sft, List(), name + i.toString)
          i += 1
          sf.setDefaultGeometry(WKTUtils.read(f"POINT($lon%d $lat%d)"))
          sf.setAttribute(DefaultDtgField, "2011-01-02T00:00:00Z")
          sf.setAttribute("type", name)
          sf.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE
          featureCollection.add(sf)
        }
      }

      // write the feature to the store
      val res = fs.addFeatures(featureCollection)

      // tube features
      val tubeFeatures = fs.getFeatures(CQL.toFilter("BBOX(geom, 39.999999999,39.999999999, 40.00000000001, 50.000000001) AND type = 'a'"))

      // result set to tube on
      val features = fs.getFeatures(CQL.toFilter("type <> 'a'"))

      // get back type b from tube
      val ts = new TubeSelectProcess()

      // 110 m/s times 1000 seconds is just 100km which is under 1 degree
      val results = ts.execute(tubeFeatures, features, null, 110L, 1000L, 0.0, 5, null)

      val f = results.features()
      while (f.hasNext) {
        val sf = f.next
        sf.getAttribute("type") mustEqual "b"
        val point = sf.getDefaultGeometry.asInstanceOf[Point]
        point.getX mustEqual 40.0
        point.getY should be between(40.0, 50.0)
      }

      results.size mustEqual 10
    }

    "should properly dedup overlapping results based on buffer size " in {
      val sftName = "tubetest2"

      val ds = createStore

      val fs = ds.getFeatureSource(sftName)

      // tube features
      val tubeFeatures = fs.getFeatures(CQL.toFilter("BBOX(geom, 39.999999999,39.999999999, 40.00000000001, 50.000000001) AND type = 'a'"))

      // result set to tube on
      val features = fs.getFeatures(CQL.toFilter("type <> 'a'"))

      // get back type b from tube
      val ts = new TubeSelectProcess()

      // this time we use 112km which is just over 1 degree so we should pick up additional features
      // but with buffer overlap since the features in the collection are 1 degrees apart
      val results = ts.execute(tubeFeatures, features, null, 112L, 1000L, 0.0, 5, null)

      val f = results.features()
      while (f.hasNext) {
        val sf = f.next
        sf.getAttribute("type") mustEqual "b"
        val point = sf.getDefaultGeometry.asInstanceOf[Point]
        point.getX should be between(40.0, 41.0)
        point.getY should be between(40.0, 50.0)
      }

      results.size mustEqual 20
    }

    "should properly handle geometries crossing the IDL" in {
      val sftName = "tubeTestType"
      val sft = SimpleFeatureTypes.createType(sftName, s"type:String, $geotimeAttributes")

      val ds = createStore

      ds.createSchema(sft)
      val fs = ds.getFeatureSource(sftName)

      val featureCollection = new DefaultFeatureCollection(sftName, sft)

      List("c", "d").foreach { name =>
        List(1, 2, 3, 4).zip(List(-178, -179, 179, 178)).foreach { case (i, lat) =>
          val sf = AvroSimpleFeatureFactory.buildAvroFeature(sft, List(), name + i.toString)
          sf.setDefaultGeometry(WKTUtils.read(f"POINT($lat%d 0)"))
          sf.setAttribute(DefaultDtgField, f"2011-01-01T00:0$i%d:00Z")
          sf.setAttribute("type", name)
          sf.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE
          featureCollection.add(sf)
        }
      }
      //Add a point that would be in the buffer if it wrapped around the earth
      //This point falls between the time of C2 & C3 but west of C2. This point
      //Would be grabbed if the buffer wrapped the globe due to the IDL.
      val sf = AvroSimpleFeatureFactory.buildAvroFeature(sft, List(), "d5")
      sf.setDefaultGeometry(WKTUtils.read(f"POINT(-178.5 0)"))
      sf.setAttribute(DefaultDtgField, f"2011-01-01T00:02:03Z")
      sf.setAttribute("type", "d")
      sf.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE
      featureCollection.add(sf)

      // write the feature to the store
      val res = fs.addFeatures(featureCollection)

      // tube features
      val tubeFeatures = fs.getFeatures(CQL.toFilter("type = 'c'"))

      // result set to tube on
      val features = fs.getFeatures(CQL.toFilter("type <> 'c'"))

      // get back type d from tube
      val ts = new TubeSelectProcess()
      val results = ts.execute(tubeFeatures, features, null, 1L, 1L, 0.0, 5, "line")

      val f = results.features()
      while (f.hasNext) {
        val sf = f.next
        sf.getAttribute("type") mustEqual "d"
      }

      results.size mustEqual 4
    }

    "properly interpolate times and geometries" in {
      val sftName = "tubeTestType"
      val sft = SimpleFeatureTypes.createType(sftName, s"type:String, $geotimeAttributes")

      val ds = createStore

      ds.createSchema(sft)
      val fs = ds.getFeatureSource(sftName)

      val featureCollection = new DefaultFeatureCollection(sftName, sft)
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
        val sf = AvroSimpleFeatureFactory.buildAvroFeature(sft, List(), i.toString)
        sf.setDefaultGeometry(curPoint)
        sf.setAttribute(DefaultDtgField, new Date(t))
        sf.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE
        featureCollection.add(sf)
        if((t - startTime) % 600000 == 0 ) {         //Create a track point every 10 minutes
          sf.setAttribute("type","track")
          trackCount+=1
        } else if ((t - startTime) % 450000 == 0) { //Create invalid pool points every 7.5 minutes, but put them back in time.
          sf.setAttribute(DefaultDtgField, new Date(t-450000l))
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
      val res = fs.addFeatures(featureCollection)

      // tube features
      val tubeFeatures = fs.getFeatures(CQL.toFilter("type = 'track'"))

      // result set to tube on
      val features = fs.getFeatures(CQL.toFilter("type = 'pool' or type = 'pool2'"))

      // get back type b from tube
      val ts = new TubeSelectProcess()
      val results = ts.execute(tubeFeatures, features, null, 1L, 1L, 1000.0, 5, "interpolated")
      val f = results.features()
      while (f.hasNext) {
        val sf = f.next
        sf.getAttribute("type") mustEqual "pool"
      }

      results.size mustEqual poolCount
    }
  }



  "TubeSelect" should {
    "should handle all geometries" in {
      val sftName = "tubeline"
      val sft = SimpleFeatureTypes.createType(sftName, "type:String,*geom:Geometry:srid=4326,dtg:Date;geomesa.mixed.geometries=true")

      val ds = createStore

      ds.createSchema(sft)
      val fs = ds.getFeatureSource(sftName)

      val featureCollection = new DefaultFeatureCollection(sftName, sft)

      List("b").foreach { name =>
        List(1, 2, 3, 4).zip(List(45, 46, 47, 48)).foreach { case (i, lat) =>
          val sf = AvroSimpleFeatureFactory.buildAvroFeature(sft, List(), name + i.toString)
          sf.setDefaultGeometry(WKTUtils.read(f"POINT(40 $lat%d)"))
          sf.setAttribute(DefaultDtgField, "2011-01-01T00:00:00Z")
          sf.setAttribute("type", name)
          sf.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE
          featureCollection.add(sf)
        }
      }

      val bLine = AvroSimpleFeatureFactory.buildAvroFeature(sft, List(), "b-line")
      bLine.setDefaultGeometry(WKTUtils.read("LINESTRING(40 40, 40 50)"))
      bLine.setAttribute(DefaultDtgField, "2011-01-01T00:00:00Z")
      bLine.setAttribute("type", "b")
      bLine.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE
      featureCollection.add(bLine)

      val bPoly = AvroSimpleFeatureFactory.buildAvroFeature(sft, List(), "b-poly")
      bPoly.setDefaultGeometry(WKTUtils.read("POLYGON((40 40, 41 40, 41 41, 40 41, 40 40))"))
      bPoly.setAttribute(DefaultDtgField, "2011-01-01T00:00:00Z")
      bPoly.setAttribute("type", "b")
      bPoly.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE
      featureCollection.add(bPoly)



      // tube features
      val aLine = AvroSimpleFeatureFactory.buildAvroFeature(sft, List(), "a-line")
      aLine.setDefaultGeometry(WKTUtils.read("LINESTRING(40 40, 40 50)"))
      aLine.setAttribute(DefaultDtgField, "2011-01-01T00:00:00Z")
//      aLine.setAttribute("end", new DateTime("2011-01-01T00:00:00Z", DateTimeZone.UTC).toDate)
      aLine.setAttribute("type", "a")
      aLine.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE
      val tubeFeatures = new ListFeatureCollection(sft, List(aLine))

      // write the feature to the store
      val res = fs.addFeatures(featureCollection)

      // result set to tube on
      val features = fs.getFeatures(CQL.toFilter("type <> 'a'"))

      QueryProperties.QueryExactCount.threadLocalValue.set("true")
      try {
        features.size mustEqual 6
      } finally {
        QueryProperties.QueryExactCount.threadLocalValue.remove()
      }

      // get back type b from tube
      val ts = new TubeSelectProcess()
      val results = ts.execute(tubeFeatures, features, null, 112L, 1L, 0.0, 5, null)

      val f = results.features()
      while (f.hasNext) {
        val sf = f.next
        sf.getAttribute("type") mustEqual "b"
      }

      QueryProperties.QueryExactCount.threadLocalValue.set("true")
      try {
        results.size mustEqual 6
      } finally {
        QueryProperties.QueryExactCount.threadLocalValue.remove()
      }
    }
  }

  "TubeBuilder" should {
    "approximate meters to degrees" in {
      val geoFac = new GeometryFactory

      val sftName = "tubeline"
      val sft = SimpleFeatureTypes.createType(sftName, s"type:String,$geotimeAttributes")

      // calculated km at various latitude by USGS
      forall(List(0, 30, 60, 89).zip(List(110.57, 110.85, 111.41, 111.69))) { case(lat, dist) =>
        val deg = new NoGapFill(new DefaultFeatureCollection(sftName, sft), 0, 0).metersToDegrees(110.57*1000, geoFac.createPoint(new Coordinate(0, lat)))
        (1.0-dist) should beLessThan(.0001)
      }
    }
  }

  "TubeSelect" should {
    "properly handle values for execute" in {
      val sftName = "tubeline"
      val sft = SimpleFeatureTypes.createType(sftName, s"type:String,$geotimeAttributes")
      val ts = new TubeSelectProcess
      val ds = createStore

      val fs = ds.getFeatureSource(sftName)

      val q = new Query(sftName, Filter.INCLUDE)
      val res = fs.getFeatures(q)

      // tube features
      val aLine = AvroSimpleFeatureFactory.buildAvroFeature(sft, List(), "a-line")
      aLine.setDefaultGeometry(WKTUtils.read("LINESTRING(40 40, 40 50)"))
      aLine.setAttribute(DefaultDtgField, "2011-01-01T00:00:00Z")
//      aLine.setAttribute("end", new DateTime("2011-01-01T00:00:00Z", DateTimeZone.UTC).toDate)
      aLine.setAttribute("type", "a")
      aLine.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE
      val tubeFeatures = new ListFeatureCollection(sft, List(aLine))

      // ensure null values work and don't throw exceptions
      ts.execute( tubeFeatures, res, null, null, null, null, null, null) should not(throwAn[ClassCastException])
    }
  }

  "TubeSelect" should {
    "should do a simple tube with geo interpolation with a different date field" in {
      val nonDefaultDtgField = "datefield"
      val sftName = "tubeTestType-date"
      val geotimeAttributes = s"*geom:Point:srid=4326,$nonDefaultDtgField:Date"
      val sft = SimpleFeatureTypes.createType(sftName, s"type:String,$geotimeAttributes")

      val ds = createStore

      ds.createSchema(sft)
      val fs = ds.getFeatureSource(sftName)

      val featureCollection = new DefaultFeatureCollection(sftName, sft)

      List("a", "b").foreach { name =>
        List(1, 2, 3, 4).zip(List(45, 46, 47, 48)).foreach { case (i, lat) =>
          val sf = AvroSimpleFeatureFactory.buildAvroFeature(sft, List(), name + i.toString)
          sf.setDefaultGeometry(WKTUtils.read(f"POINT($lat%d $lat%d)"))
          sf.setAttribute(nonDefaultDtgField, "2011-01-01T00:00:00Z")
          sf.setAttribute("type", name)
          sf.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE
          featureCollection.add(sf)
        }
      }

      // write the feature to the store
      val res = fs.addFeatures(featureCollection)

      // tube features
      val tubeFeatures = fs.getFeatures(CQL.toFilter("type = 'a'"))

      // result set to tube on
      val features = fs.getFeatures(CQL.toFilter("type <> 'a'"))

      // get back type b from tube
      val ts = new TubeSelectProcess()
      val results = ts.execute(tubeFeatures, features, null, 1L, 1L, 0.0, 5, null)

      val f = results.features()
      while (f.hasNext) {
        val sf = f.next
        sf.getAttribute("type") mustEqual "b"
      }

      results.size mustEqual 4
    }
  }

}
