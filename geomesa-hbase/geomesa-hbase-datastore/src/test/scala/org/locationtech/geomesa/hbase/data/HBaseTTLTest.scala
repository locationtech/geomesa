package org.locationtech.geomesa.hbase.data

import java.io.File
import java.util.{Collections, Date}
import java.text.SimpleDateFormat

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.hbase.TableName
import org.geotools.data._
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.data.simple.SimpleFeatureStore
import org.geotools.filter.text.ecql.ECQL
import org.geotools.util.factory.Hints
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.filter.function.ProxyIdFunction
import org.locationtech.geomesa.hbase.HBaseSystemProperties
import org.locationtech.geomesa.hbase.data.HBaseDataStoreParams._
import org.locationtech.geomesa.index.api.GeoMesaFeatureIndex
import org.locationtech.geomesa.index.conf.{QueryHints, QueryProperties, SchemaProperties}
import org.locationtech.geomesa.index.index.attribute.AttributeIndex
import org.locationtech.geomesa.index.index.id.IdIndex
import org.locationtech.geomesa.index.index.z3.Z3Index
import org.locationtech.geomesa.process.query.ProximitySearchProcess
import org.locationtech.geomesa.process.tube.TubeSelectProcess
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.conf.{GeoMesaProperties, SemanticVersion}
import org.locationtech.geomesa.utils.geotools.{FeatureUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.WithClose
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.Filter
import org.specs2.matcher.MatchResult
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class HBaseTTLTest extends Specification with LazyLogging {

  sequential

  val typeName = "test_data"
  val params = Map(
    ConnectionParam.getName -> MiniCluster.connection,
    HBaseCatalogParam.getName -> getClass.getSimpleName)
  val ttl = HBaseSystemProperties.WriteTTL.toLong.getOrElse(0L)
  val sft = SimpleFeatureTypes.createType(typeName, "name:String:index=true,dtg:Date")//;geomesa.feature.expiry=10000")


  "HBase TTL" should {
    "remove based on TTL" >> {
      val ds = DataStoreFinder.getDataStore(params).asInstanceOf[HBaseDataStore]
      ds must not(beNull)
      ds.createSchema(sft)

      try {
        //        ds.getSchema(typeName) must beNull
        //        val sft = ds.getSchema(typeName)

        sft must not(beNull)

        //        val ns = DataStoreFinder.getDataStore(params ++ Map(NamespaceParam.key -> "ns0")).getSchema(typeName).getName
        //        ns.getNamespaceURI mustEqual "ns0"
        //        ns.getLocalPart mustEqual typeName

        val fs = ds.getFeatureSource(typeName).asInstanceOf[SimpleFeatureStore]

        val dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
        val startTime = new Date(System.currentTimeMillis())
        val numFeatures = 1
        val toAdd = (0 until numFeatures).map { i =>
          val sf = new ScalaSimpleFeature(sft, i.toString)
          // don't know what this line does but it's needed
          sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
          sf.setAttribute(0, s"name$i")

          val featureTime = new Date(startTime.getTime + 5000 * i)
          sf.setAttribute(1, dateFormat.format(featureTime))
          sf
        }

        val ids = fs.addFeatures(new ListFeatureCollection(sft, toAdd))
        ids.asScala.map(_.getID) must containTheSameElementsAs((0 until numFeatures).map(_.toString))

        getElements(ds,typeName).size mustEqual numFeatures

        Thread.sleep(10000)
        (1 until numFeatures).map { i =>
          logger.info(f"running ${i}")
          getElements(ds,typeName).size mustEqual numFeatures - i
          Thread.sleep(5000)
        }
        true
      } finally {
        ds.dispose()
      }
    }
    "refuse to add already-expired features" >> {
      val ds = DataStoreFinder.getDataStore(params).asInstanceOf[HBaseDataStore]
      ds must not(beNull)
      ds.createSchema(sft)

      try {
        sft must not(beNull)

        val fs = ds.getFeatureSource(typeName).asInstanceOf[SimpleFeatureStore]

        val dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ")

        val toAdd = (0 until 1).map { i =>
          val sf = new ScalaSimpleFeature(sft, i.toString)
          sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
          sf.setAttribute(0, s"name$i")

          val featureTime = new Date(System.currentTimeMillis - 15000)
          sf.setAttribute(1, dateFormat.format(featureTime))
          sf
        }
        val ids = fs.addFeatures(new ListFeatureCollection(sft, toAdd))
//        ids.asScala.map(_.getID) must containTheSameElementsAs((0 until numFeatures).map(_.toString))

        getElements(ds,typeName).size mustEqual 0
      } finally {
        ds.dispose()
      }
    }
  }

  def getElements(ds: HBaseDataStore,
                  typeName: String): List[SimpleFeature] = {
    runQuery(ds, new Query(typeName))
  }

  def runQuery(ds: HBaseDataStore,
               typeName: String,
               filter: String): List[SimpleFeature] = {
    //    var query: Query = null; // don't think this is a a great way to do it
    //    if (filter != null) {
    //      val test: Array[String] = null
    //      query = new Query(typeName, ECQL.toFilter(filter), test)
    val query = new Query(typeName, ECQL.toFilter(filter))
    //    }
    //    else {
    //      query = new Query(typeName)
    //    }
    runQuery(ds, query)
  }

  def runQuery(ds: HBaseDataStore,
               query: Query): List[SimpleFeature] = {
    val fr = ds.getFeatureReader(query, Transaction.AUTO_COMMIT)
    val features = SelfClosingIterator(fr).toList
    features
  }
}
