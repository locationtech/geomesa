package org.locationtech.geomesa.hbase.data

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.hbase.HBaseTestingUtility
import org.apache.hadoop.hbase.client.Connection
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.data.simple.SimpleFeatureStore
import org.geotools.data.{DataStore, DataStoreFinder, Query}
import org.geotools.factory.{CommonFactoryFinder, Hints}
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class HBaseServerSideTests extends Specification with LazyLogging {
  import org.locationtech.geomesa.utils.geotools.Conversions._

  import scala.collection.JavaConversions._

  sequential

  val cluster = new HBaseTestingUtility()
  var connection: Connection = _
  var ds: DataStore = _
  var fs: SimpleFeatureStore = _
  private val ff = CommonFactoryFinder.getFilterFactory2

  step {
    logger.info("Starting embedded hbase")
    cluster.startMiniCluster(1)
    connection = cluster.getConnection
    logger.info("Started")

    logger.info("Populating data")
    val typeName = "testpoints"

    val params =
      Map(
        HBaseDataStoreParams.ConnectionParam.getName -> connection,
        HBaseDataStoreParams.BigTableNameParam.getName -> "test_sft",
        HBaseDataStoreParams.LooseBBoxParam.getName -> false)

    ds = DataStoreFinder.getDataStore(params).asInstanceOf[HBaseDataStore]
    ds.createSchema(SimpleFeatureTypes.createType(typeName, "name:String:index=full,attr:String,dtg:Date,*geom:Point:srid=4326"))
    val sft = ds.getSchema(typeName)
    fs = ds.getFeatureSource(typeName).asInstanceOf[SimpleFeatureStore]

    val toAdd = (0 until 10).map { i =>
      val sf = new ScalaSimpleFeature(i.toString, sft)
      sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
      sf.setAttribute(0, s"name$i")
      sf.setAttribute(1, s"name$i")
      sf.setAttribute(2, f"2017-01-${i + 1}%02dT00:00:01.000Z")
      sf.setAttribute(3, s"POINT(4$i 5$i)")
      sf
    }

    fs.addFeatures(new ListFeatureCollection(sft, toAdd))
    fs.flush()
    logger.info("Done populating data")
    success
  }

  "transforms" should {
    "work for id queries" >> {
      val idFilt = ff.id(ff.featureId("1"))
      val query = new Query("testpoints", idFilt, Array("name"))
      // NOTE: geometry is implicitly returned
      val results = fs.getFeatures(query).features.toList

      // we expect only 1 result with two attributes and one of the attributes is 'name'
      results.length must be equalTo 1 and
        (results.head.getType.getAttributeDescriptors.map(_.getLocalName) must containAllOf(Seq("geom","name"))) and
        (results.head.getAttributes.length must be equalTo 2) and
        (results.head.get[String]("name") must be equalTo "name1")
    }

    "work for attribute indexes" >> {
      val nameFilter = ff.equals(ff.property("name"), ff.literal("name1"))
      val query = new Query("testpoints", nameFilter, Array("name"))
      val results = fs.getFeatures(query).features.toList

      // we expect only 1 result with 2 attributes and one of the attributes is 'name'
      results.length must be equalTo 1 and
        (results.head.getType.getAttributeDescriptors.map(_.getLocalName) must containAllOf(Seq("geom","name"))) and
        (results.head.getAttributes.length must be equalTo 2) and
        (results.head.get[String]("name") must be equalTo "name1")

    }
  }

  step {
    logger.info("Stopping embedded hbase")
    cluster.shutdownMiniCluster()
    logger.info("Stopped")
  }

}
