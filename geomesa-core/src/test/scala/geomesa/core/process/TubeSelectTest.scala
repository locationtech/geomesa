package geomesa.core.process

import collection.JavaConversions._
import com.vividsolutions.jts.geom.{Coordinate, GeometryFactory}
import geomesa.core.data.{AccumuloFeatureStore, AccumuloDataStore}
import geomesa.process.{TubeVisitor, TubeSelect}
import geomesa.utils.text.WKTUtils
import org.geotools.data.{DataUtilities, DataStoreFinder}
import org.geotools.factory.Hints
import org.geotools.feature.DefaultFeatureCollection
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.filter.text.cql2.CQL
import org.joda.time.{DateTimeZone, DateTime}
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TubeSelectTest extends Specification {

  sequential

  val geotimeAttributes = geomesa.core.index.spec

  def createStore: AccumuloDataStore =
  // the specific parameter values should not matter, as we
  // are requesting a mock data store connection to Accumulo
    DataStoreFinder.getDataStore(Map(
      "instanceId" -> "mycloud",
      "zookeepers" -> "zoo1:2181,zoo2:2181,zoo3:2181",
      "user"       -> "myuser",
      "password"   -> "mypassword",
      "auths"      -> "A,B,C",
      "tableName"  -> "testwrite",
      "useMock"    -> "true",
      "featureEncoding" -> "avro")).asInstanceOf[AccumuloDataStore]

  val sftName = "tubeTestType"
  val sft = DataUtilities.createType(sftName, s"type:String,$geotimeAttributes")

  "TubeSelect" should {
    "should do a simple tube with geo interpolation" in {
      val ds = createStore

      ds.createSchema(sft)
      val fs = ds.getFeatureSource(sftName).asInstanceOf[AccumuloFeatureStore]

      val featureCollection = new DefaultFeatureCollection(sftName, sft)

      List("a", "b").foreach { name =>
        List(1,2,3,4).zip(List(45,46,47,48)).foreach { case (i, lat) =>
          val sf = SimpleFeatureBuilder.build(sft, List(), name+i.toString)
          sf.setDefaultGeometry(WKTUtils.read(f"POINT($lat%d $lat%d)"))
          sf.setAttribute(geomesa.core.index.SF_PROPERTY_START_TIME, new DateTime("2011-01-01T00:00:00Z", DateTimeZone.UTC).toDate)
          sf.setAttribute("type",name)
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
      val ts = new TubeSelect()
      val results = ts.execute(tubeFeatures, features, null, 1, 1, 0, "not implemented yet", 0)

      val f = results.features()
      while(f.hasNext) {
        val sf = f.next
        sf.getAttribute("type") should equalTo("b")
      }

      results.size should equalTo(4)
    }

    "should do a simple tube with geo + time interpolation" in {
      val ds = createStore
      val fs = ds.getFeatureSource(sftName).asInstanceOf[AccumuloFeatureStore]

      val featureCollection = new DefaultFeatureCollection(sftName, sft)

      List("c").foreach { name =>
        List(1,2,3,4).zip(List(45,46,47,48)).foreach { case (i, lat) =>
          val sf = SimpleFeatureBuilder.build(sft, List(), name+i.toString)
          sf.setDefaultGeometry(WKTUtils.read(f"POINT($lat%d $lat%d)"))
          sf.setAttribute(geomesa.core.index.SF_PROPERTY_START_TIME, new DateTime("2011-01-02T00:00:00Z", DateTimeZone.UTC).toDate)
          sf.setAttribute("type",name)
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
      val ts = new TubeSelect()
      val results = ts.execute(tubeFeatures, features, null, 1, 1, 0, "not implemented yet", 0)

      val f = results.features()
      while(f.hasNext) {
        val sf = f.next
        sf.getAttribute("type") should equalTo("b")
      }

      results.size should equalTo(4)
    }


  }

  "TubeVistitor" should {
    "approximate meters to degrees" in {
      val geoFac = new GeometryFactory

      // calculated km at various latitude by USGS
      List(0, 30, 60, 89).zip(List(110.57, 110.85, 111.41, 111.69)).foreach { case(lat, dist) =>
        val deg = TubeVisitor.metersToDegrees(110.57*1000, geoFac.createPoint(new Coordinate(0, lat)))
        (1.0-dist) should beLessThan(.0001)
      }
    }
  }
}
