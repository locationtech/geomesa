package org.locationtech.geomesa.hbase.data

import java.io.File
import java.util.Collections

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

  "HBaseDataStore" should {
    "work with points" in {
      val typeName = "test_data"

      val params = Map(
        ConnectionParam.getName -> MiniCluster.connection,
        HBaseCatalogParam.getName -> getClass.getSimpleName)
      val ds = DataStoreFinder.getDataStore(params).asInstanceOf[HBaseDataStore]
      ds must not(beNull)

      try {
        ds.getSchema(typeName) must beNull

        ds.createSchema(SimpleFeatureTypes.createType(typeName, "name:String:index=true,dtg:Date"))

        val sft = ds.getSchema(typeName)

        sft must not(beNull)

        val ns = DataStoreFinder.getDataStore(params ++ Map(NamespaceParam.key -> "ns0")).getSchema(typeName).getName
        ns.getNamespaceURI mustEqual "ns0"
        ns.getLocalPart mustEqual typeName

        val fs = ds.getFeatureSource(typeName).asInstanceOf[SimpleFeatureStore]

        val toAdd = (0 until 10).map { i =>
          val sf = new ScalaSimpleFeature(sft, i.toString)
          sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
          sf.setAttribute(0, s"name$i")
          //          sf.setAttribute(1, s"name$i")
          sf.setAttribute(1, f"2014-01-${i + 1}%02dT00:00:01.000Z")
          //          sf.setAttribute(3, s"POINT(4$i 5$i)")
          sf
        }

        val ids = fs.addFeatures(new ListFeatureCollection(sft, toAdd))
        ids.asScala.map(_.getID) must containTheSameElementsAs((0 until 10).map(_.toString))

        val transformsList = Seq(null, Array("geom"), Array("geom", "dtg"), Array("name"), Array("dtg", "geom", "attr", "name"))

        foreach(Seq(true, false)) { remote =>
          foreach(Seq(true, false)) { loose =>
            val settings = Map(LooseBBoxParam.getName -> loose, RemoteFilteringParam.getName -> remote)
            val ds = DataStoreFinder.getDataStore(params ++ settings).asInstanceOf[HBaseDataStore]


            //            val result = runQuery(ds, typeName, "INCLUDE")
            //            val result = SelfClosingIterator(ds.getFeatureReader(typeName, ECQL.toFilter("INCLUDE"))).toList
            //            val result = runQuery(ds, typeName, "dtg DURING 2014-01-01T00:00:00.000Z/2014-01-08T12:00:00.000Z")
            val result = runQuery(ds, typeName, "INCLUDE")
            println(result)

            //            foreach(transformsList) { transforms =>
            //              // test that blocking full table scans doesn't interfere with regular queries
            //              QueryProperties.BlockFullTableScans.threadLocalValue.set("true")
            //              testQuery(ds, typeName, "IN('0', '2')", transforms, Seq(toAdd(0), toAdd(2)))
            //              testQuery(ds, typeName, "bbox(geom,38,48,52,62) and dtg DURING 2014-01-01T00:00:00.000Z/2014-01-08T12:00:00.000Z", transforms, toAdd.dropRight(2))
            //              testQuery(ds, typeName, "bbox(geom,42,48,52,62) and dtg DURING 2013-12-15T00:00:00.000Z/2014-01-15T00:00:00.000Z", transforms, toAdd.drop(2))
            //              testQuery(ds, typeName, "bbox(geom,42,48,52,62)", transforms, toAdd.drop(2))
            //              testQuery(ds, typeName, "dtg DURING 2014-01-01T00:00:00.000Z/2014-01-08T12:00:00.000Z", transforms, toAdd.dropRight(2))
            //              testQuery(ds, typeName, "attr = 'name5' and bbox(geom,38,48,52,62) and dtg DURING 2014-01-01T00:00:00.000Z/2014-01-08T12:00:00.000Z", transforms, Seq(toAdd(5)))
            //              testQuery(ds, typeName, "name < 'name5'", transforms, toAdd.take(5))
            //              testQuery(ds, typeName, "name = 'name5'", transforms, Seq(toAdd(5)))
            //              testQuery(ds, typeName, s"bbox(geom,39,49,50,60) AND dtg DURING 2014-01-01T00:00:00.000Z/2014-01-08T12:00:00.000Z AND (proxyId() = ${ProxyIdFunction.proxyId("0")} OR proxyId() = ${ProxyIdFunction.proxyId("1")})", transforms, toAdd.take(2))
            //
            //              // this query should be blocked
            //              testQuery(ds, typeName, "INCLUDE", transforms, toAdd) must throwA[RuntimeException]
            //              // with max features set, it should go through - don't count, that will be blocked
            //              testQuery(ds, new Query(typeName, ECQL.toFilter("INCLUDE"), 10, transforms, null), toAdd, count = false)
            //
            //              QueryProperties.BlockFullTableScans.threadLocalValue.remove()
            //              // now it should go through
            //              testQuery(ds, typeName, "INCLUDE", transforms, toAdd)
            //            }
            true
          }
        }

        def testTransforms(ds: HBaseDataStore): MatchResult[_] = {
          forall(Seq(("INCLUDE", toAdd), ("bbox(geom,42,48,52,62)", toAdd.drop(2)))) { case (filter, results) =>
            val transforms = Array("derived=strConcat('hello',name)", "geom")
            val fr = ds.getFeatureReader(new Query(typeName, ECQL.toFilter(filter), transforms), Transaction.AUTO_COMMIT)
            val features = SelfClosingIterator(fr).toList
            features.headOption.map(f => SimpleFeatureTypes.encodeType(f.getFeatureType)) must
              beSome("derived:String,*geom:Point:srid=4326")
            features.map(_.getID) must containTheSameElementsAs(results.map(_.getID))
            forall(features) { feature =>
              feature.getAttribute("derived") mustEqual s"helloname${feature.getID}"
              feature.getAttribute("geom") mustEqual results.find(_.getID == feature.getID).get.getAttribute("geom")
            }
          }
        }

        testTransforms(ds)

        def testProcesses(ds: HBaseDataStore): MatchResult[_] = {
          val query = new ListFeatureCollection(sft, Array[SimpleFeature](toAdd(4)))
          val source = ds.getFeatureSource(typeName).getFeatures()

          val proximity = new ProximitySearchProcess().execute(query, source, 10.0)
          SelfClosingIterator(proximity.features()).toList mustEqual Seq(toAdd(4))

          val tube = new TubeSelectProcess().execute(query, source, Filter.INCLUDE, null, 1L, 100.0, 10, null)
          SelfClosingIterator(tube.features()).toList mustEqual Seq(toAdd(4))
        }

        testProcesses(ds)

        def testCount(ds: HBaseDataStore): MatchResult[_] = {
          val query = new Query(typeName, Filter.INCLUDE, Query.NO_PROPERTIES)
          val results = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList
          results must haveLength(10)
          results.map(_.getID) must containTheSameElementsAs(toAdd.map(_.getID))
          foreach(results)(_.getAttributeCount mustEqual 0)
        }

        def testExactCount(ds: HBaseDataStore): MatchResult[_] = {
          // without hints
          ds.getFeatureSource(typeName).getFeatures(new Query(typeName, Filter.INCLUDE)).size() mustEqual 0
          ds.getFeatureSource(typeName).getCount(new Query(typeName, Filter.INCLUDE)) mustEqual -1

          val queryWithHint = new Query(typeName, Filter.INCLUDE)
          queryWithHint.getHints.put(QueryHints.EXACT_COUNT, java.lang.Boolean.TRUE)
          val queryWithViewParam = new Query(typeName, Filter.INCLUDE)
          queryWithViewParam.getHints.put(Hints.VIRTUAL_TABLE_PARAMETERS,
            Collections.singletonMap("EXACT_COUNT", "true"))

          foreach(Seq(queryWithHint, queryWithViewParam)) { query =>
            ds.getFeatureSource(typeName).getFeatures(query).size() mustEqual 10
          }
        }

        testCount(ds)
        testExactCount(ds)

        ds.getFeatureSource(typeName).removeFeatures(ECQL.toFilter("INCLUDE"))

        forall(Seq("INCLUDE",
          "IN('0', '2')",
          "bbox(geom,42,48,52,62)",
          "bbox(geom,38,48,52,62) and dtg DURING 2014-01-01T00:00:00.000Z/2014-01-08T12:00:00.000Z",
          "bbox(geom,42,48,52,62) and dtg DURING 2013-12-15T00:00:00.000Z/2014-01-15T00:00:00.000Z",
          "dtg DURING 2014-01-01T00:00:00.000Z/2014-01-08T12:00:00.000Z",
          "attr = 'name5' and bbox(geom,38,48,52,62) and dtg DURING 2014-01-01T00:00:00.000Z/2014-01-08T12:00:00.000Z",
          "name < 'name5'",
          "name = 'name5'")) { filter =>
          val fr = ds.getFeatureReader(new Query(typeName, ECQL.toFilter(filter)), Transaction.AUTO_COMMIT)
          SelfClosingIterator(fr).toList must beEmpty
        }
      } finally {
        ds.dispose()
      }
    }
  }

  def runQuery(ds: HBaseDataStore,
               typeName: String,
               filter: String): Unit = {
    var query: Query = null; // don't think this is a a great way to do it
    if (filter != null) {
            val test : Array[String] = null
            query = new Query(typeName, ECQL.toFilter(filter), test)
//      query = new Query(typeName, ECQL.toFilter(filter))
    }
    else {
      query = new Query(typeName)
    }

    val fr = ds.getFeatureReader(query, Transaction.AUTO_COMMIT)
    val features = SelfClosingIterator(fr).toList
    features
  }

  def testQuery(ds: HBaseDataStore,
                typeName: String,
                filter: String,
                transforms: Array[String],
                results: Seq[SimpleFeature]): MatchResult[Any] = {
    testQuery(ds, new Query(typeName, ECQL.toFilter(filter), transforms), results)
  }

  def testQuery(ds: HBaseDataStore, query: Query, results: Seq[SimpleFeature], count: Boolean = true): MatchResult[Any] = {

    val fr = ds.getFeatureReader(query, Transaction.AUTO_COMMIT)
    val features = SelfClosingIterator(fr).toList
    val attributes = Option(query.getPropertyNames)
      .getOrElse(ds.getSchema(query.getTypeName).getAttributeDescriptors.map(_.getLocalName).toArray)
    features.map(_.getID) must containTheSameElementsAs(results.map(_.getID))
    forall(features) { feature =>
      feature.getAttributes must haveLength(attributes.length)
      forall(attributes.zipWithIndex) { case (attribute, i) =>
        feature.getAttribute(attribute) mustEqual feature.getAttribute(i)
        feature.getAttribute(attribute) mustEqual results.find(_.getID == feature.getID).get.getAttribute(attribute)
      }
    }

    if (!count) {
      ok
    } else {
      ds.getFeatureSource(query.getTypeName).getCount(query) mustEqual -1
      ds.getFeatureSource(query.getTypeName).getFeatures(query).size() mustEqual 0
      query.getHints.put(QueryHints.EXACT_COUNT, true)
      ds.getFeatureSource(query.getTypeName).getFeatures(query).size() mustEqual results.length
    }
  }
}
