/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.data

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.hbase.TableName
import org.geotools.data._
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.data.simple.SimpleFeatureStore
import org.geotools.factory.Hints
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.hbase.data.HBaseDataStoreParams._
import org.locationtech.geomesa.hbase.index.{HBaseAttributeIndex, HBaseIdIndex, HBaseZ3Index}
import org.locationtech.geomesa.index.conf.QueryProperties
import org.locationtech.geomesa.process.query.ProximitySearchProcess
import org.locationtech.geomesa.process.tube.TubeSelectProcess
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.Filter
import org.specs2.matcher.MatchResult

import scala.collection.{GenTraversableOnce, immutable}
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

class HBaseDataStoreTest extends HBaseTest with LazyLogging {

  sequential

  step {
    logger.info("Starting the HBase DataStore Test")
  }

  "HBaseDataStore" should {
    "work with points" in {
      val typeName = "testpoints"

      val params = Map(ConnectionParam.getName -> connection, HBaseCatalogParam.getName -> catalogTableName)
      val ds = DataStoreFinder.getDataStore(params).asInstanceOf[HBaseDataStore]

      ds.getSchema(typeName) must beNull

      ds.createSchema(SimpleFeatureTypes.createType(typeName, "name:String:index=true,attr:String,dtg:Date,*geom:Point:srid=4326"))

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
        sf.setAttribute(1, s"name$i")
        sf.setAttribute(2, f"2014-01-${i + 1}%02dT00:00:01.000Z")
        sf.setAttribute(3, s"POINT(4$i 5$i)")
        sf
      }

      val ids = fs.addFeatures(new ListFeatureCollection(sft, toAdd))
      ids.asScala.map(_.getID) must containTheSameElementsAs((0 until 10).map(_.toString))

      val transformsList = Seq(null, Array("geom"), Array("geom", "dtg"), Array("name"), Array("dtg", "geom", "attr", "name"))

      foreach(Seq(true, false)) { remote =>
        foreach(Seq(true, false)) { loose =>
          val settings = Map(LooseBBoxParam.getName -> loose, RemoteFilteringParam.getName -> remote)
          val ds = DataStoreFinder.getDataStore(params ++ settings).asInstanceOf[HBaseDataStore]
          foreach(transformsList) { transforms =>
            // test that blocking full table scans doesn't interfere with regular queries
            QueryProperties.BlockFullTableScans.threadLocalValue.set("true")
            testQuery(ds, typeName, "IN('0', '2')", transforms, Seq(toAdd(0), toAdd(2)))
            testQuery(ds, typeName, "bbox(geom,38,48,52,62) and dtg DURING 2014-01-01T00:00:00.000Z/2014-01-08T12:00:00.000Z", transforms, toAdd.dropRight(2))
            testQuery(ds, typeName, "bbox(geom,42,48,52,62) and dtg DURING 2013-12-15T00:00:00.000Z/2014-01-15T00:00:00.000Z", transforms, toAdd.drop(2))
            testQuery(ds, typeName, "bbox(geom,42,48,52,62)", transforms, toAdd.drop(2))
            testQuery(ds, typeName, "dtg DURING 2014-01-01T00:00:00.000Z/2014-01-08T12:00:00.000Z", transforms, toAdd.dropRight(2))
            testQuery(ds, typeName, "attr = 'name5' and bbox(geom,38,48,52,62) and dtg DURING 2014-01-01T00:00:00.000Z/2014-01-08T12:00:00.000Z", transforms, Seq(toAdd(5)))
            testQuery(ds, typeName, "name < 'name5'", transforms, toAdd.take(5))
            testQuery(ds, typeName, "name = 'name5'", transforms, Seq(toAdd(5)))

            // this query should be blocked
            testQuery(ds, typeName, "INCLUDE", transforms, toAdd) must throwA[RuntimeException]
            QueryProperties.BlockFullTableScans.threadLocalValue.remove()
            // now it should go through
            testQuery(ds, typeName, "INCLUDE", transforms, toAdd)
          }
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
        val source = ds.getFeatureSource(typeName)
        val input = new ListFeatureCollection(sft, Array[SimpleFeature](toAdd(4)))

        val proximity = new ProximitySearchProcess()
        val proximityResults = proximity.execute(input, source.getFeatures(), 10.0)
        SelfClosingIterator(proximityResults.features()).toList mustEqual Seq(toAdd(4))

        val tube = new TubeSelectProcess()
        val tubeResults = tube.execute(input, source.getFeatures(), Filter.INCLUDE, null, null, 100.0, 10, null)
        SelfClosingIterator(proximityResults.features()).toList mustEqual Seq(toAdd(4))
      }

      testProcesses(ds)

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
    }

    "work with polys" in {
      val typeName = "testpolys"

      val params = Map(ConnectionParam.getName -> connection, HBaseCatalogParam.getName -> "HBaseDataStoreTest")
      val ds = DataStoreFinder.getDataStore(params).asInstanceOf[HBaseDataStore]

      ds.getSchema(typeName) must beNull

      ds.createSchema(SimpleFeatureTypes.createType(typeName, "name:String:index=true,dtg:Date,*geom:Polygon:srid=4326"))

      val sft = ds.getSchema(typeName)

      sft must not(beNull)

      val fs = ds.getFeatureSource(typeName).asInstanceOf[SimpleFeatureStore]

      val toAdd = (0 until 10).map { i =>
        val sf = new ScalaSimpleFeature(sft, i.toString)
        sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
        sf.setAttribute(0, s"name$i")
        sf.setAttribute(1, s"2014-01-01T0$i:00:01.000Z")
        sf.setAttribute(2, s"POLYGON((-120 4$i, -120 50, -125 50, -125 4$i, -120 4$i))")
        sf
      }

      val ids = fs.addFeatures(new ListFeatureCollection(sft, toAdd))
      ids.asScala.map(_.getID) must containTheSameElementsAs((0 until 10).map(_.toString))

      foreach(Seq(true, false)) { remote =>
        val settings = Map(RemoteFilteringParam.getName -> remote)
        val ds = DataStoreFinder.getDataStore(params ++ settings).asInstanceOf[HBaseDataStore]
        testQuery(ds, typeName, "INCLUDE", null, toAdd)
        testQuery(ds, typeName, "IN('0', '2')", null, Seq(toAdd(0), toAdd(2)))
        testQuery(ds, typeName, "bbox(geom,-126,38,-119,52) and dtg DURING 2014-01-01T00:00:00.000Z/2014-01-01T07:59:59.000Z", null, toAdd.dropRight(2))
        testQuery(ds, typeName, "bbox(geom,-126,42,-119,45)", null, toAdd.dropRight(4))
        testQuery(ds, typeName, "name < 'name5'", null, toAdd.take(5))
        testQuery(ds, typeName, "name = 'name5'", null, Seq(toAdd(5)))
      }
    }

    "support table splits" in {
      val typeName = "testsplits"

      val params = Map(ConnectionParam.getName -> connection, HBaseCatalogParam.getName -> catalogTableName)
      val ds = DataStoreFinder.getDataStore(params).asInstanceOf[HBaseDataStore]

      ds.getSchema(typeName) must beNull

      // note: we keep the number of splits small b/c the embedded hbase is slow to create them
      ds.createSchema(SimpleFeatureTypes.createType(typeName,
        "name:String:index=true,age:Int:index=true,attr:String,dtg:Date,*geom:Point:srid=4326;" +
            "table.splitter.options='z3.min:2017-01-01,z3.max:2017-01-02,z3.bits:2," +
            "attr.name.pattern:[a-f],attr.age.pattern:[0-9],attr.age.pattern2:[8-8][0-9]'"))

      def splits(index: String): Seq[Array[Byte]] = {
        val table = TableName.valueOf(ds.manager.index(index).getTableName(typeName, ds))
        ds.connection.getRegionLocator(table).getStartKeys
      }

      splits(HBaseAttributeIndex.identifier) must haveLength((6 + 10 + 10) * 4) // a-f for name, 0-9 + [8]0-9 for age * 4 shards
      splits(HBaseZ3Index.identifier) must haveLength(16) // 2 bits * 4 shards
      splits(HBaseIdIndex.identifier) must haveLength(4) // default 4 splits
    }
  }

  def testQuery(ds: HBaseDataStore, typeName: String, filter: String, transforms: Array[String], results: Seq[SimpleFeature]): MatchResult[Any] = {
    val query = new Query(typeName, ECQL.toFilter(filter), transforms)
    val fr = ds.getFeatureReader(query, Transaction.AUTO_COMMIT)
    val features = SelfClosingIterator(fr).toList
    val attributes = Option(transforms).getOrElse(ds.getSchema(typeName).getAttributeDescriptors.map(_.getLocalName).toArray)
    features.map(_.getID) must containTheSameElementsAs(results.map(_.getID))
    forall(features) { feature =>
      feature.getAttributes must haveLength(attributes.length)
      forall(attributes.zipWithIndex) { case (attribute, i) =>
        feature.getAttribute(attribute) mustEqual feature.getAttribute(i)
        feature.getAttribute(attribute) mustEqual results.find(_.getID == feature.getID).get.getAttribute(attribute)
      }
    }
    ds.getFeatureSource(typeName).getFeatures(query).size() mustEqual results.length
  }
}
