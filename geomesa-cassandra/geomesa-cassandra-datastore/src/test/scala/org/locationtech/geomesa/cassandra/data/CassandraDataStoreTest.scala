/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.cassandra.data

import java.io.File
import java.util.concurrent.atomic.AtomicBoolean

import com.datastax.driver.core.{Cluster, SocketOptions}
import org.cassandraunit.CQLDataLoader
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.data.simple.SimpleFeatureStore
import org.geotools.data.{DataStoreFinder, Query, _}
import org.geotools.factory.Hints
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.cassandra.data.CassandraDataStoreFactory.Params
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory.LooseBBoxParam
import org.locationtech.geomesa.index.utils.ExplainString
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeature
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class CassandraDataStoreTest extends Specification {

  sequential

  step {
    CassandraDataStoreTest.startServer()
  }

  "CassandraDataStore" should {

    "work with points" in {
      val typeName = "testpoints"
      val ds = DataStoreFinder.getDataStore(CassandraDataStoreTest.params).asInstanceOf[CassandraDataStore]

      ds.getSchema(typeName) must beNull

      ds.createSchema(SimpleFeatureTypes.createType(typeName, "name:String:index=true,dtg:Date,*geom:Point:srid=4326"))

      val sft = ds.getSchema(typeName)

      sft must not(beNull)

      val fs = ds.getFeatureSource(typeName).asInstanceOf[SimpleFeatureStore]

      val toAdd = (0 until 10).map { i =>
        val sf = new ScalaSimpleFeature(i.toString, sft)
        sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
        sf.setAttribute(0, s"name$i")
        sf.setAttribute(1, f"2014-01-${i + 1}%02dT00:00:01.000Z")
        sf.setAttribute(2, s"POINT(4$i 5$i)")
        sf
      }

      val ids = fs.addFeatures(new ListFeatureCollection(sft, toAdd))
      ids.asScala.map(_.getID) must containTheSameElementsAs((0 until 10).map(_.toString))

      forall(Seq(true, false)) { loose =>
        val ds = DataStoreFinder.getDataStore(CassandraDataStoreTest.params ++ Map(LooseBBoxParam.getName -> loose)).asInstanceOf[CassandraDataStore]
        forall(Seq(null, Array("geom"), Array("geom", "dtg"), Array("geom", "name"))) { transforms =>
          testQuery(ds, typeName, "INCLUDE", transforms, toAdd)
          testQuery(ds, typeName, "IN('0', '2')", transforms, Seq(toAdd(0), toAdd(2)))
          testQuery(ds, typeName, "bbox(geom,38,48,52,62) and dtg DURING 2014-01-01T00:00:00.000Z/2014-01-08T12:00:00.000Z", transforms, toAdd.dropRight(2))
          testQuery(ds, typeName, "bbox(geom,42,48,52,62)", transforms, toAdd.drop(2))
          testQuery(ds, typeName, "name < 'name5'", transforms, toAdd.take(5))
        }
      }

      def testTransforms(ds: CassandraDataStore) = {
        val transforms = Array("derived=strConcat('hello',name)", "geom")
        forall(Seq(("INCLUDE", toAdd), ("bbox(geom,42,48,52,62)", toAdd.drop(2)))) { case (filter, results) =>
          val fr = ds.getFeatureReader(new Query(typeName, ECQL.toFilter(filter), transforms), Transaction.AUTO_COMMIT)
          val features = SelfClosingIterator(fr).toList
          features.headOption.map(f => SimpleFeatureTypes.encodeType(f.getFeatureType)) must
              beSome("*geom:Point:srid=4326,derived:String")
          features.map(_.getID) must containTheSameElementsAs(results.map(_.getID))
          forall(features) { feature =>
            feature.getAttribute("derived") mustEqual s"helloname${feature.getID}"
            feature.getAttribute("geom") mustEqual results.find(_.getID == feature.getID).get.getAttribute("geom")
          }
        }
      }

      testTransforms(ds)

      def testLooseBbox(ds: CassandraDataStore, loose: Boolean) = {
        val filter = ECQL.toFilter("dtg DURING 2014-01-01T00:00:00.000Z/2014-01-08T12:00:00.000Z")
        val out = new ExplainString
        ds.getQueryPlan(new Query(typeName, filter), explainer = out)
        val filterLine = "Client-side filter: "
        val clientSideFilter = out.toString.split("\n").map(_.trim).find(_.startsWith(filterLine)).map(_.substring(filterLine.length))
        if (loose) {
          clientSideFilter must beSome("None")
        } else {
          clientSideFilter must beSome(org.locationtech.geomesa.filter.filterToString(filter))
        }
      }

      // test default loose bbox config
      testLooseBbox(ds, loose = true)

      forall(Seq(true, "true", java.lang.Boolean.TRUE)) { loose =>
        val ds = DataStoreFinder.getDataStore(CassandraDataStoreTest.params ++ Map(LooseBBoxParam.getName -> loose)).asInstanceOf[CassandraDataStore]
        testLooseBbox(ds, loose = true)
      }

      forall(Seq(false, "false", java.lang.Boolean.FALSE)) { loose =>
        val ds = DataStoreFinder.getDataStore(CassandraDataStoreTest.params ++ Map(LooseBBoxParam.getName -> loose)).asInstanceOf[CassandraDataStore]
        testLooseBbox(ds, loose = false)
      }
    }

    "work with polys" in {
      val typeName = "testpolys"

      val ds = DataStoreFinder.getDataStore(CassandraDataStoreTest.params).asInstanceOf[CassandraDataStore]

      ds.getSchema(typeName) must beNull

      ds.createSchema(SimpleFeatureTypes.createType(typeName, "name:String:index=true,dtg:Date,*geom:Polygon:srid=4326"))

      val sft = ds.getSchema(typeName)

      sft must not(beNull)

      val fs = ds.getFeatureSource(typeName).asInstanceOf[SimpleFeatureStore]

      val toAdd = (0 until 10).map { i =>
        val sf = new ScalaSimpleFeature(i.toString, sft)
        sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
        sf.setAttribute(0, s"name$i")
        sf.setAttribute(1, s"2014-01-01T0$i:00:01.000Z")
        sf.setAttribute(2, s"POLYGON((-120 4$i, -120 50, -125 50, -125 4$i, -120 4$i))")
        sf
      }

      val ids = fs.addFeatures(new ListFeatureCollection(sft, toAdd))
      ids.asScala.map(_.getID) must containTheSameElementsAs((0 until 10).map(_.toString))

      testQuery(ds, typeName, "INCLUDE", null, toAdd)
      testQuery(ds, typeName, "IN('0', '2')", null, Seq(toAdd(0), toAdd(2)))
      testQuery(ds, typeName, "bbox(geom,-126,38,-119,52) and dtg DURING 2014-01-01T00:00:00.000Z/2014-01-01T07:59:59.000Z", null, toAdd.dropRight(2))
      testQuery(ds, typeName, "bbox(geom,-126,42,-119,45)", null, toAdd.dropRight(4))
      testQuery(ds, typeName, "name < 'name5'", null, toAdd.take(5))
    }

    def testQuery(ds: CassandraDataStore, typeName: String, filter: String, transforms: Array[String], results: Seq[SimpleFeature]) = {
      val fr = ds.getFeatureReader(new Query(typeName, ECQL.toFilter(filter), transforms), Transaction.AUTO_COMMIT)
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
    }
  }
}

object CassandraDataStoreTest {
  def host: String = EmbeddedCassandraServerHelper.getHost
  def port: Int = EmbeddedCassandraServerHelper.getNativeTransportPort
  def params =  Map(
    Params.CPParam.getName -> CassandraDataStoreTest.CP,
    Params.KSParam.getName -> "geomesa_cassandra",
    Params.NSParam.getName -> "http://geomesa.org",
    Params.CatalogParam.getName -> "test_sft"
  )
  def CP: String = s"$host:$port"

  private val started = new AtomicBoolean(false)

  def cleanup(): Unit = {
    EmbeddedCassandraServerHelper.cleanEmbeddedCassandra()
  }

  def startServer(): Unit = {
    if (started.compareAndSet(false, true)) {
      val storagedir = File.createTempFile("cassandra","sd")
      storagedir.delete()
      storagedir.mkdir()

      System.setProperty("cassandra.storagedir", storagedir.getPath)

      EmbeddedCassandraServerHelper.startEmbeddedCassandra("cassandra-config.yaml", 1200000L)

      var readTimeout: Int = SystemProperty("cassandraReadTimeout", "12000").get.toInt

      if(readTimeout < 0) readTimeout = 12000
      val cluster = new Cluster.Builder().addContactPoints(host).withPort(port)
        .withSocketOptions(new SocketOptions().setReadTimeoutMillis(readTimeout)).build().init()
      val session = cluster.connect()
      val cqlDataLoader = new CQLDataLoader(session)
      cqlDataLoader.load(new ClassPathCQLDataSet("init.cql", false, false))
    }
  }
}
