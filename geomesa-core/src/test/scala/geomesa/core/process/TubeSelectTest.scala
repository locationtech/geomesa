package geomesa.core.process

import collection.JavaConversions._
import org.junit.Test
import org.geotools.data.{Query, DataUtilities, DataStoreFinder}
import org.specs2.mutable.Specification
import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner
import org.geotools.feature.simple.SimpleFeatureBuilder
import geomesa.utils.text.WKTUtils
import org.geotools.factory.Hints
import org.geotools.feature.DefaultFeatureCollection
import org.joda.time.{DateTimeZone, DateTime}
import org.geotools.filter.text.cql2.CQL
import org.opengis.filter.Filter
import geomesa.core.data.{AccumuloFeatureStore, AccumuloDataStore}
import geomesa.process.TubeSelect

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
      val results = ts.execute(tubeFeatures, features, null, "not implemented yet", 1, 1, 0)

      val f = results.features()
      while(f.hasNext) {
        val sf = f.next
        sf.getAttribute("type") should equalTo("b")
        println(DataUtilities.encodeFeature(sf))
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
      val results = ts.execute(tubeFeatures, features, null, "not implemented yet", 1, 1, 0)

      val f = results.features()
      while(f.hasNext) {
        val sf = f.next
        sf.getAttribute("type") should equalTo("b")
        println(DataUtilities.encodeFeature(sf))
      }

      results.size should equalTo(4)
    }


  }
}
