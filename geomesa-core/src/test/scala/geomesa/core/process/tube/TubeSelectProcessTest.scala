package geomesa.core.process.tube

import com.vividsolutions.jts.geom.{Coordinate, GeometryFactory, Point}
import geomesa.core.data.{AccumuloDataStore, AccumuloFeatureStore}
import geomesa.core.index.{Constants, IndexSchemaBuilder}
import geomesa.feature.AvroSimpleFeatureFactory
import geomesa.utils.geotools.SimpleFeatureTypes
import geomesa.utils.text.WKTUtils
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.data.{DataStoreFinder, Query}
import org.geotools.factory.Hints
import org.geotools.feature.DefaultFeatureCollection
import org.geotools.filter.text.cql2.CQL
import org.joda.time.{DateTime, DateTimeZone}
import org.junit.runner.RunWith
import org.opengis.filter.Filter
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class TubeSelectProcessTest extends Specification {

  sequential

  val dtgField = geomesa.core.process.tube.DEFAULT_DTG_FIELD
  val geotimeAttributes = s"*geom:Geometry:srid=4326,$dtgField:Date"

  def createStore: AccumuloDataStore =
  // the specific parameter values should not matter, as we
  // are requesting a mock data store connection to Accumulo
    DataStoreFinder.getDataStore(Map(
      "instanceId"        -> "mycloud",
      "zookeepers"        -> "zoo1:2181,zoo2:2181,zoo3:2181",
      "user"              -> "myuser",
      "password"          -> "mypassword",
      "auths"             -> "A,B,C",
      "tableName"         -> "testwrite",
      "useMock"           -> "true",
      "indexSchemaFormat" -> new IndexSchemaBuilder("~").randomNumber(3).constant("TEST").geoHash(0, 3).date("yyyyMMdd").nextPart().geoHash(3, 2).nextPart().id().build(),
      "featureEncoding"   -> "avro")).asInstanceOf[AccumuloDataStore]

  "TubeSelect" should {
    "should do a simple tube with geo interpolation" in {
      val sftName = "tubeTestType"
      val sft = SimpleFeatureTypes.createType(sftName, s"type:String,$geotimeAttributes")
      sft.getUserData()(Constants.SF_PROPERTY_START_TIME) = dtgField

      val ds = createStore

      ds.createSchema(sft)
      val fs = ds.getFeatureSource(sftName).asInstanceOf[AccumuloFeatureStore]

      val featureCollection = new DefaultFeatureCollection(sftName, sft)

      List("a", "b").foreach { name =>
        List(1, 2, 3, 4).zip(List(45, 46, 47, 48)).foreach { case (i, lat) =>
          val sf = AvroSimpleFeatureFactory.buildAvroFeature(sft, List(), name + i.toString)
          sf.setDefaultGeometry(WKTUtils.read(f"POINT($lat%d $lat%d)"))
          sf.setAttribute(geomesa.core.process.tube.DEFAULT_DTG_FIELD, new DateTime("2011-01-01T00:00:00Z", DateTimeZone.UTC).toDate)
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
      val results = ts.execute(tubeFeatures, features, null, 1, 1, 0, 5, null)

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
      sft.getUserData()(Constants.SF_PROPERTY_START_TIME) = dtgField

      val ds = createStore
      val fs = ds.getFeatureSource(sftName).asInstanceOf[AccumuloFeatureStore]

      val featureCollection = new DefaultFeatureCollection(sftName, sft)

      List("c").foreach { name =>
        List(1, 2, 3, 4).zip(List(45, 46, 47, 48)).foreach { case (i, lat) =>
          val sf = AvroSimpleFeatureFactory.buildAvroFeature(sft, List(), name + i.toString)
          sf.setDefaultGeometry(WKTUtils.read(f"POINT($lat%d $lat%d)"))
          sf.setAttribute(geomesa.core.process.tube.DEFAULT_DTG_FIELD, new DateTime("2011-01-02T00:00:00Z", DateTimeZone.UTC).toDate)
          sf.setAttribute("type", name)
          sf.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE
          sf.getUserData()(Constants.SF_PROPERTY_START_TIME) = dtgField
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
      val results = ts.execute(tubeFeatures, features, null, 1, 1, 0, 5, null)

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
      sft.getUserData()(Constants.SF_PROPERTY_START_TIME) = dtgField

      val ds = createStore

      ds.createSchema(sft)

      val fs = ds.getFeatureSource(sftName).asInstanceOf[AccumuloFeatureStore]

      val featureCollection = new DefaultFeatureCollection(sftName, sft)

      var i = 0
      List("a", "b").foreach { name =>
        for (lon <- 40 until 50; lat <- 40 until 50) {
          val sf = AvroSimpleFeatureFactory.buildAvroFeature(sft, List(), name + i.toString)
          i += 1
          sf.setDefaultGeometry(WKTUtils.read(f"POINT($lon%d $lat%d)"))
          sf.setAttribute(geomesa.core.process.tube.DEFAULT_DTG_FIELD, new DateTime("2011-01-02T00:00:00Z", DateTimeZone.UTC).toDate)
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
      val results = ts.execute(tubeFeatures, features, null, 110, 1000, 0, 5, null)

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

      val fs = ds.getFeatureSource(sftName).asInstanceOf[AccumuloFeatureStore]

      // tube features
      val tubeFeatures = fs.getFeatures(CQL.toFilter("BBOX(geom, 39.999999999,39.999999999, 40.00000000001, 50.000000001) AND type = 'a'"))

      // result set to tube on
      val features = fs.getFeatures(CQL.toFilter("type <> 'a'"))

      // get back type b from tube
      val ts = new TubeSelectProcess()

      // this time we use 112km which is just over 1 degree so we should pick up additional features
      // but with buffer overlap since the features in the collection are 1 degrees apart
      val results = ts.execute(tubeFeatures, features, null, 112, 1000, 0, 5, null)

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
  }

  "TubeSelect" should {
    "should handle all geometries" in {
      val sftName = "tubeline"
      val sft = SimpleFeatureTypes.createType(sftName, s"type:String,$geotimeAttributes")
      sft.getUserData()(Constants.SF_PROPERTY_START_TIME) = dtgField

      val ds = createStore

      ds.createSchema(sft)
      val fs = ds.getFeatureSource(sftName).asInstanceOf[AccumuloFeatureStore]

      val featureCollection = new DefaultFeatureCollection(sftName, sft)

      List("b").foreach { name =>
        List(1, 2, 3, 4).zip(List(45, 46, 47, 48)).foreach { case (i, lat) =>
          val sf = AvroSimpleFeatureFactory.buildAvroFeature(sft, List(), name + i.toString)
          sf.setDefaultGeometry(WKTUtils.read(f"POINT(40 $lat%d)"))
          sf.setAttribute(geomesa.core.process.tube.DEFAULT_DTG_FIELD, new DateTime("2011-01-01T00:00:00Z", DateTimeZone.UTC).toDate)
          sf.setAttribute("type", name)
          sf.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE
          featureCollection.add(sf)
        }
      }

      val bLine = AvroSimpleFeatureFactory.buildAvroFeature(sft, List(), "b-line")
      bLine.setDefaultGeometry(WKTUtils.read("LINESTRING(40 40, 40 50)"))
      bLine.setAttribute(geomesa.core.process.tube.DEFAULT_DTG_FIELD, new DateTime("2011-01-01T00:00:00Z", DateTimeZone.UTC).toDate)
      bLine.setAttribute("type", "b")
      bLine.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE
      featureCollection.add(bLine)

      val bPoly = AvroSimpleFeatureFactory.buildAvroFeature(sft, List(), "b-poly")
      bPoly.setDefaultGeometry(WKTUtils.read("POLYGON((40 40, 41 40, 41 41, 40 41, 40 40))"))
      bPoly.setAttribute(geomesa.core.process.tube.DEFAULT_DTG_FIELD, new DateTime("2011-01-01T00:00:00Z", DateTimeZone.UTC).toDate)
      bPoly.setAttribute("type", "b")
      bPoly.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE
      featureCollection.add(bPoly)



      // tube features
      val aLine = AvroSimpleFeatureFactory.buildAvroFeature(sft, List(), "a-line")
      aLine.setDefaultGeometry(WKTUtils.read("LINESTRING(40 40, 40 50)"))
      aLine.setAttribute(geomesa.core.process.tube.DEFAULT_DTG_FIELD, new DateTime("2011-01-01T00:00:00Z", DateTimeZone.UTC).toDate)
//      aLine.setAttribute("end", new DateTime("2011-01-01T00:00:00Z", DateTimeZone.UTC).toDate)
      aLine.setAttribute("type", "a")
      aLine.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE
      val tubeFeatures = new ListFeatureCollection(sft, List(aLine))

      // write the feature to the store
      val res = fs.addFeatures(featureCollection)

      // result set to tube on
      val features = fs.getFeatures(CQL.toFilter("type <> 'a'"))

      features.size mustEqual 6

      // get back type b from tube
      val ts = new TubeSelectProcess()
      val results = ts.execute(tubeFeatures, features, null, 112, 1, 0, 5, null)

      val f = results.features()
      while (f.hasNext) {
        val sf = f.next
        sf.getAttribute("type") mustEqual "b"
      }

      results.size mustEqual 6
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

      val fs = ds.getFeatureSource(sftName).asInstanceOf[AccumuloFeatureStore]

      val q = new Query(sftName, Filter.INCLUDE)
      val res = fs.getFeatures(q)

      // tube features
      val aLine = AvroSimpleFeatureFactory.buildAvroFeature(sft, List(), "a-line")
      aLine.setDefaultGeometry(WKTUtils.read("LINESTRING(40 40, 40 50)"))
      aLine.setAttribute(geomesa.core.process.tube.DEFAULT_DTG_FIELD, new DateTime("2011-01-01T00:00:00Z", DateTimeZone.UTC).toDate)
//      aLine.setAttribute("end", new DateTime("2011-01-01T00:00:00Z", DateTimeZone.UTC).toDate)
      aLine.setAttribute("type", "a")
      aLine.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE
      val tubeFeatures = new ListFeatureCollection(sft, List(aLine))

      // ensure null values work and don't throw exceptions
      ts.execute( tubeFeatures, res, null, null, null, null, null, null) should not(throwAn[ClassCastException])
    }
  }
}
