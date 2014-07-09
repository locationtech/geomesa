package geomesa.core.iterators

import java.text.SimpleDateFormat
import java.util.TimeZone

import com.vividsolutions.jts.geom.Geometry
import geomesa.core.data.{AccumuloFeatureStore, AccumuloDataStore}
import geomesa.utils.text.WKTUtils
import org.geotools.data.{Query, DataUtilities, DataStoreFinder}
import org.geotools.factory.{CommonFactoryFinder, Hints}
import org.geotools.feature.DefaultFeatureCollection
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.filter.text.cql2.CQL
import org.geotools.filter.text.ecql.ECQL
import org.joda.time.{DateTimeZone, DateTime}
import org.junit.runner.RunWith
import org.opengis.filter.Filter
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner


import geomesa.utils.geotools.Conversions._

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class AttributeIndexFilteringIteratorTest extends Specification {

  val sftName = "AttributeIndexFilteringIteratorTest"
  val sft = DataUtilities.createType(sftName, s"name:String,age:Integer,dtg:Date,*geom:Geometry:srid=4326")

  val sdf = new SimpleDateFormat("yyyyMMdd")
  sdf.setTimeZone(TimeZone.getTimeZone("Zulu"))
  val dateToIndex = sdf.parse("20140102")

  def createStore: AccumuloDataStore =
  // the specific parameter values should not matter, as we
  // are requesting a mock data store connection to Accumulo
    DataStoreFinder.getDataStore(
      Map(
        "instanceId" -> "mycloud",
        "zookeepers" -> "zoo1:2181,zoo2:2181,zoo3:2181",
        "user"       -> "myuser",
        "password"   -> "mypassword",
        "auths"      -> "A,B,C",
        "tableName"  -> "AttributeIndexFilteringIteratorTest",
        "useMock"    -> "true")
    ).asInstanceOf[AccumuloDataStore]

  val ds = createStore

  ds.createSchema(sft)
  val fs = ds.getFeatureSource(sftName).asInstanceOf[AccumuloFeatureStore]

  val featureCollection = new DefaultFeatureCollection(sftName, sft)

  List("a", "b").foreach { name =>
    List(1, 2, 3, 4).zip(List(45, 46, 47, 48)).foreach { case (i, lat) =>
      val sf = SimpleFeatureBuilder.build(sft, List(), name + i.toString)
      sf.setDefaultGeometry(WKTUtils.read(f"POINT($lat%d $lat%d)"))
      sf.setAttribute("dtg", new DateTime("2011-01-01T00:00:00Z", DateTimeZone.UTC).toDate)
      sf.setAttribute("name", name)
      sf.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE
      featureCollection.add(sf)
    }
  }

  fs.addFeatures(featureCollection)

  val ff = CommonFactoryFinder.getFilterFactory2

  "AttributeIndexFilteringIterator" should {
    "handle like queries" in {
      fs.getFeatures(ff.like(ff.property("name"),"%")).features.size should equalTo(8)
      fs.getFeatures(ff.like(ff.property("name"),"%a")).features.size should equalTo(4)
      fs.getFeatures(ff.like(ff.property("name"),"%a%")).features.size should equalTo(4)
      fs.getFeatures(ff.like(ff.property("name"),"a%")).features.size should equalTo(4)
    }

    "handle transforms" in {
      val query = new Query(sftName, ECQL.toFilter("name <> 'a'"), Array("geom"))
      val features = fs.getFeatures(query)

      features.size should equalTo(4)
      features.features.foreach { sf =>
        sf.getAttributeCount should equalTo(1)
        sf.getAttribute(0) should beAnInstanceOf[Geometry]
      }
    }


  }



}
