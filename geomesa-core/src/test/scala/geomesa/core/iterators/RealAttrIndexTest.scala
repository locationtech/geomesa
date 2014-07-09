//package geomesa.core.iterators
//
//import java.text.SimpleDateFormat
//import java.util.TimeZone
//
//import geomesa.core.data.{AccumuloFeatureStore, AccumuloDataStore}
//import geomesa.utils.text.WKTUtils
//import org.geotools.data.{DataUtilities, DataStoreFinder}
//import org.geotools.factory.{CommonFactoryFinder, Hints}
//import org.geotools.feature.DefaultFeatureCollection
//import org.geotools.feature.simple.SimpleFeatureBuilder
//import org.joda.time.{DateTimeZone, DateTime}
//import org.junit.runner.RunWith
//import org.opengis.filter.Filter
//import org.specs2.mutable.Specification
//import org.specs2.runner.JUnitRunner
//
//
//import geomesa.utils.geotools.Conversions._
//
//import scala.collection.JavaConversions._
//import scala.collection.JavaConverters._
//
//@RunWith(classOf[JUnitRunner])
//class RealAttrIndexTest extends Specification {
//
//  "AttributeIndexFilteringIterator" should {
//    "handle like queries" in {
//      val sftName = "twitter"
//      val sft = DataUtilities.createType(sftName, s"name:String,age:Integer,dtg:Date,*geom:Geometry:srid=4326")
//
//      def createStore: AccumuloDataStore =
//      // the specific parameter values should not matter, as we
//      // are requesting a mock data store connection to Accumulo
//        DataStoreFinder.getDataStore(
//          Map(
//            "instanceId" -> "dcloud",
//            "zookeepers" -> "dzoo1:2181,dzoo2:2181,dzoo3:2181",
//            "user"       -> "root",
//            "password"   -> "secret",
//            "tableName"  -> "afox_gm_catalog")
//        ).asInstanceOf[AccumuloDataStore]
//
//      val ds = createStore
//
////      ds.createSchema(sft)
//      val fs = ds.getFeatureSource(sftName).asInstanceOf[AccumuloFeatureStore]
////
////      val featureCollection = new DefaultFeatureCollection(sftName, sft)
////
////      List("a", "b").foreach { name =>
////        List(1, 2, 3, 4).zip(List(45, 46, 47, 48)).foreach { case (i, lat) =>
////          val sf = SimpleFeatureBuilder.build(sft, List(), name + i.toString)
////          sf.setDefaultGeometry(WKTUtils.read(f"POINT($lat%d $lat%d)"))
////          sf.setAttribute("dtg", new DateTime("2011-01-01T00:00:00Z", DateTimeZone.UTC).toDate)
////          sf.setAttribute("name", name)
////          sf.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE
////          featureCollection.add(sf)
////        }
////      }
////
////      fs.addFeatures(featureCollection)
//
////      1 should equalTo(1)
//
//      val ff = CommonFactoryFinder.getFilterFactory2
//      val found = fs.getFeatures(ff.like(ff.property("user_name"),"hello%"))
//      println(s"Num results: ${found.size.toString}")
//
//        //found.size should equalTo(4)
//      1 should equalTo(1)
////      fs.getFeatures(ff.like(ff.property("name"),"a")).features.size should equalTo(4)
////      fs.getFeatures(ff.like(ff.property("name"),"a%")).features.size should equalTo(4)
////      fs.getFeatures(ff.like(ff.property("name"),"%a")).features.size should equalTo(4)
////      fs.getFeatures(ff.like(ff.property("name"),"%a%")).features.size should equalTo(4)
////      fs.getFeatures(ff.like(ff.property("name"),"a%")).features.size should equalTo(4)
//    }
//  }
//
//}
