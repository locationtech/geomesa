package org.locationtech.geomesa.hbase.data

import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.{DataStore, DataStoreFinder, Query}
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.data.simple.SimpleFeatureStore
import org.geotools.filter.text.ecql.ECQL
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.geotools.util.factory.Hints
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.filter.FilterHelper
import org.locationtech.geomesa.hbase.data.HBaseDataStoreParams.{ConnectionParam, CoprocessorThreadsParam, HBaseCatalogParam}
import org.locationtech.geomesa.hbase.data.HBaseDensityFilterTest.TestTableSplit
import org.locationtech.geomesa.index.conf.{QueryHints, QueryProperties, TableSplitter}
import org.locationtech.geomesa.index.iterators.DensityScan
import org.locationtech.geomesa.index.iterators.DensityScan.GridIterator
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.jts.geom.Envelope
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class HBasePartialResultsTest extends Specification with LazyLogging {
  sequential

  //System.setProperty("geomesa.density.batch.size", "100")

  val splitterClassName = "org.locationtech.geomesa.hbase.data.HBasePartialResultsTest$TestTableSplit"

  val TEST_FAMILY = s"an_id:java.lang.Integer,attr:java.lang.Double,dtg:Date,geom:Point:srid=4326;table.splitter.class=${splitterClassName}"
  val TEST_HINT = new Hints()
  val sftName = "test_sft"
  val typeName = "HBasePartialResultsTest"

  lazy val params = Map(
    ConnectionParam.getName -> MiniCluster.connection,
    HBaseCatalogParam.getName -> getClass.getSimpleName
    // JNH abstract over Copro Threads
    //CoprocessorThreadsParam.getName -> "1"
  )

  lazy val ds: HBaseDataStore = DataStoreFinder.getDataStore(params).asInstanceOf[HBaseDataStore]
  //lazy val dsSemiLocal = DataStoreFinder.getDataStore(params ++ Map(HBaseDataStoreParams.DensityCoprocessorParam.key -> false)).asInstanceOf[HBaseDataStore]
  lazy val dsFullLocal = DataStoreFinder.getDataStore(params ++ Map(HBaseDataStoreParams.RemoteFilteringParam.key -> false)).asInstanceOf[HBaseDataStore]
  lazy val dsThreads1 = DataStoreFinder.getDataStore(params ++ Map(HBaseDataStoreParams.CoprocessorThreadsParam.key -> "1")).asInstanceOf[HBaseDataStore]
  lazy val dsThreads2 = DataStoreFinder.getDataStore(params ++ Map(HBaseDataStoreParams.CoprocessorThreadsParam.key -> "2")).asInstanceOf[HBaseDataStore]

  //  lazy val dsFullLocal = DataStoreFinder.getDataStore(params ++ Map(HBaseDataStoreParams.RemoteFilteringParam.key -> false)).asInstanceOf[HBaseDataStore]

  //lazy val dataStores = Seq(ds, dsFullLocal) //, dsThreads1, dsThreads2)
  lazy val dataStores = Seq(ds, dsFullLocal, dsThreads1, dsThreads2)

  var sft: SimpleFeatureType = _
  var fs: SimpleFeatureStore = _

  step {
    logger.info("Starting the Partial Results Test")
    ds.getSchema(typeName) must beNull
    ds.createSchema(SimpleFeatureTypes.createType(typeName, TEST_FAMILY))
    sft = ds.getSchema(typeName)
    fs = ds.getFeatureSource(typeName).asInstanceOf[SimpleFeatureStore]

    val features = (0 until 2048).map { i =>
      val sf = new ScalaSimpleFeature(sft, i.toString)
      sf.setAttribute(0, i.toString)
      sf.setAttribute(1, s"$i.0")
      sf.setAttribute(2, "2012-01-01T19:00:00Z")
      sf.setAttribute(3, s"POINT(${i % 64} ${i / 64})")
      sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
      sf
    }

    fs.addFeatures(new ListFeatureCollection(sft, features))

    // Check initial count
    QueryProperties.QueryExactCount.threadLocalValue.set("true")
    try {
      fs.getCount(Query.ALL) mustEqual 2048
    } finally {
      QueryProperties.QueryExactCount.threadLocalValue.remove()
    }
  }

  "Partial Results" should {
    "work with Arrow Scans" in {
      "by blah" >> {
        ok
      }
    }

    "work with Bin Scans" in {
      "by blah" >> {
        ok
      }
    }

    "work with Density Scans" in {
      def getDensity(typeName: String, query: String, ds: DataStore): (Seq[SimpleFeature], List[(Double, Double, Double)]) = {
        val fs: SimpleFeatureStore = ds.getFeatureSource(typeName).asInstanceOf[SimpleFeatureStore]
        val filter = ECQL.toFilter(query)
        val envelope = FilterHelper.extractGeometries(filter, "geom").values.headOption match {
          case None    => ReferencedEnvelope.create(new Envelope(-180, 180, -90, 90), DefaultGeographicCRS.WGS84)
          case Some(g) => ReferencedEnvelope.create(g.getEnvelopeInternal,  DefaultGeographicCRS.WGS84)
        }
        val q = new Query(typeName, filter)
        q.getHints.put(QueryHints.DENSITY_BBOX, envelope)
        q.getHints.put(QueryHints.DENSITY_WIDTH, 500)
        q.getHints.put(QueryHints.DENSITY_HEIGHT, 500)
        val decode: GridIterator = DensityScan.decodeResult(envelope, 500, 500)
        val features: Seq[SimpleFeature] = SelfClosingIterator(fs.getFeatures(q).features).toList
        val grid: List[(Double, Double, Double)] = features.flatMap(decode).toList
        (features, grid)
      }

      "by blah" >> {
        forall(dataStores) { dataStore =>
          val q = "INCLUDE"
          val (features, density) = getDensity(typeName, q, dataStore)

          val compiled = density.groupBy(d => (d._1, d._2)).map { case (_, group) => group.map(_._3).sum }

          println(s"Datastore remote filter flag: ${dataStore.config.remoteFilter} Size: ${features.size}")

          if (dataStore.config.remoteFilter) {
            // JNH: Find batch
          } else { // local case, we get one result.
            features.size mustEqual 1
          }

          // should be 5 bins of 30
          compiled must haveLength(2048)
          density.map(_._3).sum mustEqual 2048
          forall(compiled){ _ mustEqual 1 }
        }
      }
    }

    "work with Stats Scans" in {
      "by blah" >> {
        ok
      }
    }
  }

  step {
    logger.info("Cleaning up HBase Density Test")
    ds.dispose()
    // JNH dispose other DSes!
  }
}

object HBasePartialResultsTest {
  val splits: Array[Array[Byte]] = Array()
  //  val splits: Array[Array[Byte]] = Array(
  //    Array(1,8,-113,112,-60,124,126,-57,-125,-49,-1,51,50,54),
  //    Array(1,8,-113,112,-60,125,-2,-57,-3,-79,59,51,50,56),
  //    Array(1,8,-113,112,-57,124,120,28,-1,-16,39,51,52,49),
  //    Array(1,8,-113,112,-36,16,120,-32,13,-74,28,51,52,53),
  //    Array(1,8,-113,112,-36,28,72,-29,97,-119,3,51,52,56),
  //    Array(1,8,-113,112,-33,112,78,-29,28,70,-33,51,54,56),
  //    Array(1,8,-113,112,-33,113,-50,-8,16,120,-64,51,55,48),
  //    Array(1,8,-113,112,-33,113,-2,-8,29,-8,-4,51,55,49),
  //    Array(1,8,-113,118,4,17,-8,35,19,-63,-40,51,55,56),
  //    Array(1,8,-113,118,4,28,120,35,127,-50,59,51,56,48),
  //    Array(1,8,-113,118,4,113,-8,-32,-127,-65,39,51,56,54),
  //    Array(1,8,-113,118,4,124,120,-32,-19,-9,-29,51,56,56),
  //    Array(1,8,-113,118,7,125,-50,59,-16,64,-57,52,48,53),
  //    Array(1,8,-113,118,28,29,-2,-8,-1,-10,3,52,49,52),
  //    Array(1,8,-113,118,28,112,78,-5,-98,119,-64,52,49,53),
  //    Array(1,8,-113,118,28,124,120,36,109,-128,-29,52,49,57),
  //    Array(1,8,-113,118,28,125,-8,39,97,-114,7,52,50,49),
  //    Array(1,8,-113,118,31,16,72,60,0,15,-64,52,50,50),
  //    Array(1,8,-113,118,31,28,72,63,96,120,-29,52,50,54),
  //    Array(1,8,-113,118,31,112,120,-25,98,8,39,52,51,49),
  //    Array(1,8,-113,118,-60,17,-50,36,125,-72,-25,52,52,48),
  //    Array(1,8,-113,118,-60,17,-2,39,28,63,32,52,52,49),
  //    Array(1,8,-113,118,-57,16,126,-4,115,-1,36,52,53,53),
  //    Array(1,8,-113,118,-57,17,-50,-1,-128,54,56,52,53,54),
  //    Array(1,8,-113,118,-57,29,-56,36,-116,9,3,52,53,57),
  //    Array(1,8,-113,118,-57,112,72,39,-128,64,-4,52,54,49),
  //    Array(1,8,-113,118,-57,112,120,39,-115,-57,36,52,54,50),
  //    Array(1,8,-113,118,-57,113,-8,60,-127,-49,-29,52,54,52),
  //    Array(1,8,-113,118,-57,125,-56,63,-114,56,27,52,54,55),
  //    Array(1,8,-113,118,-36,16,72,-28,-126,63,36,52,54,57),
  //    Array(1,8,-113,118,-36,112,72,-1,-111,-114,63,52,55,55),
  //    Array(1,8,-113,118,-36,124,78,36,-15,-8,-60,52,56,49),
  //    Array(1,8,-113,118,-36,125,-2,39,-2,54,63,52,56,52),
  //    Array(1,8,-113,118,-33,17,-2,63,-97,-64,-60,52,56,56),
  //    Array(2,8,-113,64,4,29,-63,28,30,14,-29,54),
  //    Array(2,8,-113,64,4,29,-15,28,115,-71,4,55),
  //    Array(2,8,-113,64,4,112,113,31,31,-15,-64,57),
  //    Array(2,8,-113,64,4,113,-63,31,126,120,-37,49,48),
  //    Array(2,8,-113,64,7,17,-15,-33,-116,71,63,49,57),
  //    Array(2,8,-113,64,7,28,119,4,-128,121,32,50,49),
  //    Array(2,8,-113,64,7,29,-9,4,-18,49,-60,50,51),
  //    Array(2,8,-113,64,28,16,71,-60,-100,7,3,51,50),
  //    Array(2,8,-113,64,28,28,71,-57,-4,112,56,51,54),
  //    Array(2,8,-113,64,28,29,-57,-36,-16,120,-33,51,56),
  //    Array(2,8,-113,64,-57,124,113,56,-19,-16,3,57,49),
  //    Array(2,8,-113,64,-36,16,65,-32,-128,126,36,57,52),
  //    Array(2,8,-113,64,-36,17,-63,-32,-18,54,-29,57,54),
  //    Array(2,8,-113,64,-36,17,-15,-29,-125,-73,-33,57,55),
  //    Array(2,8,-113,64,-36,28,113,-8,-126,9,-64,57,57),
  //    Array(2,8,-113,64,-36,112,113,-5,-30,78,-29,49,48,51),
  //    Array(2,8,-113,64,-36,124,71,32,-15,-79,-64,49,48,54),
  //    Array(2,8,-113,64,-33,16,119,56,-16,119,-5,49,49,49),
  //    Array(2,8,-113,64,-33,112,71,-29,-98,15,-5,49,49,56),
  //    Array(2,8,-113,70,4,16,113,36,13,-120,56,49,50,54),
  //    Array(2,8,-113,70,4,29,-15,60,97,-7,36,49,51,50),
  //    Array(2,8,-113,70,4,112,65,63,2,48,56,49,51,51),
  //    Array(2,8,-113,70,4,112,113,63,15,-79,-60,49,51,52),
  //    Array(2,8,-113,70,4,113,-63,63,110,56,-33,49,51,53),
  //    Array(2,8,-113,70,4,124,113,-28,111,-10,-25,49,51,56),
  //    Array(2,8,-113,70,4,125,-15,-25,110,72,56,49,52,48),
  //    Array(2,8,-113,70,7,113,-57,39,127,-74,27,49,53,49),
  //    Array(2,8,-113,70,28,16,71,-28,30,71,7,49,53,55),
  //    Array(2,8,-113,70,28,28,119,-4,-127,-79,-25,49,54,50),
  //    Array(2,8,-113,70,28,112,71,-1,-116,118,-60,49,54,53),
  //    Array(2,8,-113,70,28,113,-15,36,-127,-56,0,49,54,55),
  //    Array(2,8,-113,70,28,125,-63,39,-114,0,-5,49,55,48),
  //    Array(2,8,-113,70,31,16,113,60,-113,-113,-36,49,55,51),
  //    Array(2,8,-113,70,31,29,-63,-28,-114,127,56,49,55,56),
  //    Array(2,8,-113,70,31,112,65,-25,-112,55,-36,49,56,48),
  //    Array(2,8,-113,70,-60,17,-57,36,-1,-72,-61,49,57,48),
  //    Array(2,8,-113,70,-60,28,71,39,-13,-65,60,49,57,50),
  //    Array(2,8,-113,70,-60,125,-2,-61,109,-15,31,50,48,51),
  //    Array(2,8,-113,70,-57,17,-2,-37,15,-74,-28,50,48,55),
  //    Array(2,8,-113,70,-33,112,78,-61,-116,79,-1,50,52,51),
  //    Array(2,8,-113,70,-33,112,126,-61,-29,-80,60,50,52,52),
  //    Array(2,8,-113,70,-33,113,-50,-40,-126,49,-32,50,52,53),
  //    Array(2,8,-113,112,4,28,72,3,-16,7,35,50,53,52),
  //    Array(2,8,-113,112,4,112,72,27,-112,112,60,50,53,56),
  //    Array(2,8,-113,112,4,112,120,27,-99,-15,-28,50,53,57),
  //    Array(2,8,-113,112,7,16,72,-40,-109,-119,-28,50,54,54),
  //    Array(2,8,-113,112,7,17,-56,-40,-1,-57,35,50,54,56),
  //    Array(2,8,-113,112,7,17,-8,-37,-98,78,59,50,54,57),
  //    Array(2,8,-113,112,7,28,126,4,0,48,4,50,55,49),
  //    Array(2,8,-113,112,28,17,-50,-57,2,15,-8,50,56,52),
  //    Array(2,8,-113,112,28,17,-2,-57,15,-16,4,50,56,53),
  //    Array(2,8,-113,112,28,28,78,-57,110,113,28,50,56,54),
  //    Array(2,8,-113,112,28,28,126,-36,3,-8,-61,50,56,55),
  //    Array(2,8,-113,112,28,29,-50,-36,98,120,-1,50,56,56),
  //    Array(2,8,-113,112,28,124,72,4,112,9,31,50,57,51),
  //    Array(2,8,-113,112,31,16,72,28,16,78,-28,50,57,55),
  //    Array(2,8,-113,112,31,28,72,31,114,56,-57,51,48,49),
  //    Array(2,8,-113,112,31,112,120,-57,114,72,35,51,48,54),
  //    Array(2,8,-113,112,31,125,-8,-33,-20,15,-4,51,49,50),
  //    Array(2,8,-113,112,-60,113,-2,-60,-113,-50,24,51,50,52),
  //    Array(2,8,-113,112,-57,16,78,-36,-100,56,-8,51,50,57),
  //    Array(2,8,-113,112,-57,17,-50,-33,-112,118,28,51,51,49),
  //    Array(2,8,-113,112,-57,17,-2,-33,-99,-9,-64,51,51,50),
  //    Array(2,8,-113,112,-57,28,78,-33,-4,126,-37,51,51,51),
  //    Array(2,8,-113,112,-57,29,-56,4,-100,72,39,51,51,52),
  //    Array(2,8,-113,112,-57,29,-8,4,-15,-55,59,51,51,53),
  //    Array(2,8,-113,112,-57,113,-56,7,-2,14,28,51,51,56),
  //    Array(2,8,-113,112,-57,113,-8,28,-109,-113,-61,51,51,57),
  //    Array(2,8,-113,112,-57,125,-56,31,-98,113,63,51,52,50),
  //    Array(2,8,-113,112,-36,16,72,-60,-110,127,32,51,52,52),
  //    Array(2,8,-113,112,-36,29,-8,-8,108,71,32,51,53,49),
  //    Array(2,8,-113,112,-36,113,-56,-5,111,-122,-37,51,53,52),
  //    Array(2,8,-113,118,7,16,72,-8,-125,-128,-60,51,57,49),
  //    Array(2,8,-113,118,7,17,-56,-8,-17,-114,3,51,57,51),
  //    Array(2,8,-113,118,7,17,-8,-5,-114,14,63,51,57,52),
  //    Array(2,8,-113,118,7,28,72,-5,-29,-113,-4,51,57,53),
  //    Array(2,8,-113,118,7,28,126,32,-126,112,32,51,57,54),
  //    Array(2,8,-113,118,7,125,-2,59,-3,-63,-33,52,48,54),
  //    Array(2,8,-113,118,28,17,-2,-29,-97,-80,36,52,49,48),
  //    Array(2,8,-113,118,28,28,78,-29,-2,49,56,52,49,49),
  //    Array(2,8,-113,118,28,28,126,-8,-109,-72,-57,52,49,50),
  //    Array(2,8,-113,118,31,17,-56,60,108,112,36,52,50,52),
  //    Array(2,8,-113,118,31,29,-56,-28,14,54,28,52,50,56),
  //    Array(2,8,-113,118,31,112,72,-25,2,62,-40,52,51,48),
  //    Array(2,8,-113,118,31,113,-56,-25,111,-119,63,52,51,50),
  //    Array(2,8,-113,118,31,124,120,-1,2,78,28,52,51,53),
  //    Array(2,8,-113,118,31,125,-8,-1,124,6,-40,52,51,55),
  //    Array(2,8,-113,118,-60,28,126,60,16,119,-60,52,52,51),
  //    Array(2,8,-113,118,-60,29,-50,60,29,-2,-40,52,52,52),
  //    Array(2,8,-113,118,-60,112,78,63,28,73,63,52,52,54),
  //    Array(2,8,-113,118,-60,113,-2,-28,31,-114,56,52,52,57),
  //    Array(2,8,-113,118,-60,124,78,-28,126,15,-60,52,53,48),
  //    Array(2,8,-113,118,-57,17,-2,-1,-115,-73,-60,52,53,55),
  //    Array(2,8,-113,118,-36,16,120,-28,-113,-10,60,52,55,48),
  //    Array(2,8,-113,118,-36,17,-8,-25,-125,-2,-33,52,55,50),
  //    Array(2,8,-113,118,-36,28,72,-25,-29,-55,7,52,55,51),
  //    Array(2,8,-113,118,-36,112,120,-1,-16,15,-29,52,55,56),
  //    Array(2,8,-113,118,-33,16,78,60,-109,-73,-25,52,56,53),
  //    Array(2,8,-113,118,-33,29,-50,-28,-14,78,63,52,57,49),
  //    Array(2,8,-113,118,-33,29,-2,-28,-1,-49,-25,52,57,50),
  //    Array(2,8,-113,118,-33,113,-1,-40,13,-71,-36,52,57,54),
  //    Array(3,8,-113,64,4,16,113,4,29,-56,24,49),
  //    Array(3,8,-113,64,4,28,113,7,127,-121,59,53),
  //    Array(3,8,-113,64,4,112,65,31,18,112,28,56),
  //    Array(3,8,-113,64,4,124,113,-60,-19,-74,-29,49,51),
  //    Array(3,8,-113,64,4,125,-15,-57,-20,8,28,49,53),
  //    Array(3,8,-113,64,7,16,65,-36,-127,-119,-64,49,54),
  //    Array(3,8,-113,64,7,112,71,7,-125,-72,-37,50,52),
  //    Array(3,8,-113,64,7,112,119,7,-30,63,3,50,53),
  //    Array(3,8,-113,64,7,113,-57,7,-17,-65,63,50,54),
  //    Array(3,8,-113,64,7,124,71,28,-18,65,32,50,56),
  //    Array(3,8,-113,64,7,125,-9,31,-3,-128,-37,51,49),
  //    Array(3,8,-113,64,28,17,-9,-57,-99,-71,32,51,53),
  //    Array(3,8,-113,64,28,28,119,-36,-111,-15,-57,51,55),
  //    Array(3,8,-113,64,28,112,71,-33,-98,54,-64,52,48),
  //    Array(3,8,-113,64,31,16,65,28,-110,78,-64,52,55),
  //    Array(3,8,-113,64,31,28,65,59,96,49,-29,53,49),
  //    Array(3,8,-113,64,-36,124,119,35,-112,56,-40,49,48,55),
  //    Array(3,8,-113,64,-33,28,71,59,-2,0,-40,49,49,52),
  //    Array(3,8,-113,64,-33,29,-57,-32,-14,7,63,49,49,54),
  //    Array(3,8,-113,70,4,16,65,36,0,1,32,49,50,53),
  //    Array(3,8,-113,70,4,125,-63,-25,15,-63,36,49,51,57),
  //    Array(3,8,-113,70,7,16,113,-4,112,0,-33,49,52,50),
  //    Array(3,8,-113,70,7,29,-9,36,124,113,-32,49,52,56),
  //    Array(3,8,-113,70,-57,112,72,3,2,64,-40,50,49,49),
  //    Array(3,8,-113,70,-57,124,120,24,125,-80,35,50,49,54),
  //    Array(3,8,-113,70,-57,125,-56,27,28,49,31,50,49,55),
  //    Array(3,8,-113,70,-36,29,-56,-40,31,-128,-36,50,50,53),
  //    Array(3,8,-113,70,-36,112,72,-37,19,-114,27,50,50,55),
  //    Array(3,8,-113,70,-36,124,78,0,115,-15,-32,50,51,49),
  //    Array(3,8,-113,70,-36,125,-50,3,31,-1,4,50,51,51),
  //    Array(3,8,-113,70,-33,29,-2,-64,-19,-49,-61,50,52,50),
  //    Array(3,8,-113,70,-33,113,-2,-40,-113,-72,-8,50,52,54),
  //    Array(3,8,-113,70,-33,124,78,-40,-18,63,7,50,52,55),
  //    Array(3,8,-113,70,-33,125,-50,-37,-30,119,-61,50,52,57),
  //    Array(3,8,-113,112,4,28,120,3,-3,-114,31,50,53,53),
  //    Array(3,8,-113,112,4,29,-56,24,-100,15,-57,50,53,54),
  //    Array(3,8,-113,112,4,29,-8,24,-15,-16,0,50,53,55),
  //    Array(3,8,-113,112,4,113,-8,-64,-111,-1,35,50,54,49),
  //    Array(3,8,-113,112,4,125,-56,-61,-97,-120,0,50,54,52),
  //    Array(3,8,-113,112,4,125,-8,-61,-2,9,24,50,54,53),
  //    Array(3,8,-113,112,7,29,-50,4,13,-79,24,50,55,50),
  //    Array(3,8,-113,112,7,29,-2,4,108,49,-28,50,55,51),
  //    Array(3,8,-113,112,7,112,126,7,96,63,35,50,55,53),
  //    Array(3,8,-113,112,7,125,-50,31,98,0,-61,50,56,48),
  //    Array(3,8,-113,112,28,16,78,-60,14,7,39,50,56,50),
  //    Array(3,8,-113,112,28,29,-2,-36,111,-1,39,50,56,57),
  //    Array(3,8,-113,112,28,113,-8,4,17,-120,4,50,57,50),
  //    Array(3,8,-113,112,28,124,120,4,125,-64,-61,50,57,52),
  //    Array(3,8,-113,112,28,125,-8,7,113,-57,39,50,57,54),
  //    Array(3,8,-113,112,31,28,120,31,127,-71,-37,51,48,50),
  //    Array(3,8,-113,112,31,29,-56,-60,30,63,60,51,48,51),
  //    Array(3,8,-113,112,31,112,72,-57,18,119,-8,51,48,53),
  //    Array(3,8,-113,112,31,124,72,-36,-31,-127,-37,51,48,57),
  //    Array(3,8,-113,112,31,125,-56,-33,-115,-114,-28,51,49,49),
  //    Array(3,8,-113,112,-60,16,126,4,-32,113,59,51,49,52),
  //    Array(3,8,-113,112,-60,17,-50,4,-19,-8,-57,51,49,53),
  //    Array(3,8,-113,112,-60,28,78,7,-29,-74,24,51,49,55),
  //    Array(3,8,-113,112,-60,28,126,28,-126,55,-64,51,49,56),
  //    Array(3,8,-113,112,-60,112,126,31,-29,-64,-57,51,50,50),
  //    Array(3,8,-113,112,-57,112,72,7,-110,0,-8,51,51,54),
  //    Array(3,8,-113,112,-57,124,72,28,-14,70,-37,51,52,48),
  //    Array(3,8,-113,112,-57,125,-8,31,-13,-8,-8,51,52,51),
  //    Array(3,8,-113,112,-36,17,-56,-32,108,55,-57,51,52,54),
  //    Array(3,8,-113,112,-36,112,72,-5,1,-50,31,51,53,50),
  //    Array(3,8,-113,112,-36,124,126,35,2,56,-4,51,53,55),
  //    Array(3,8,-113,112,-33,124,78,-8,124,127,39,51,55,50),
  //    Array(3,8,-113,112,-33,125,-50,-5,114,55,-29,51,55,52),
  //    Array(3,8,-113,118,4,113,-56,59,-20,57,-37,51,56,53),
  //    Array(3,8,-113,118,7,16,120,-8,-30,1,-37,51,57,50),
  //    Array(3,8,-113,118,7,29,-2,32,-18,120,-60,51,57,56),
  //    Array(3,8,-113,118,7,112,126,35,-16,54,3,52,48,48),
  //    Array(3,8,-113,118,7,113,-2,56,-100,55,-4,52,48,50),
  //    Array(3,8,-113,118,7,124,78,56,-4,8,32,52,48,51),
  //    Array(3,8,-113,118,7,124,126,59,-111,-119,56,52,48,52),
  //    Array(3,8,-113,118,-60,113,-50,-28,18,7,32,52,52,56),
  //    Array(3,8,-113,118,-57,29,-8,36,-31,-64,27,52,54,48),
  //    Array(3,8,-113,118,-57,124,72,60,-30,6,-33,52,54,53),
  //    Array(3,8,-113,118,-57,124,120,60,-17,-79,7,52,54,54),
  //    Array(3,8,-113,118,-57,125,-8,63,-29,-71,-40,52,54,56),
  //    Array(3,8,-113,118,-36,28,120,-4,-112,0,-64,52,55,52),
  //    Array(3,8,-113,118,-36,124,126,39,-112,121,-40,52,56,50),
  //    Array(3,8,-113,118,-33,16,126,60,-14,62,-5,52,56,54),
  //    Array(3,8,-113,118,-33,17,-50,63,-110,9,56,52,56,55),
  //    Array(3,8,-113,118,-33,28,78,63,-2,65,-40,52,56,57),
  //    Array(3,8,-113,118,-33,112,127,-61,97,-79,56,52,57,52),
  //    Array(3,8,-113,118,-33,113,-49,-40,0,56,-60,52,57,53),
  //    Array(3,8,-113,118,-33,125,-49,-37,96,119,-25,52,57,57)
  //  )

  class TestTableSplit extends TableSplitter {
    /**
      * Get splits for a table
      *
      * @param sft     simple feature type
      * @param index   name of the index being configured
      * @param options splitter options
      * @return split points
      */
    override def getSplits(sft: SimpleFeatureType, index: String, options: String): Array[Array[Byte]] = splits
  }
}