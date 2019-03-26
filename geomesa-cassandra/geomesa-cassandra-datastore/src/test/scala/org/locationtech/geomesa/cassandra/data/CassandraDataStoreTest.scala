/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.cassandra.data

import java.nio.file.{Files, Path}

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
import org.locationtech.geomesa.index.utils.{ExplainString, Explainer}
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.locationtech.geomesa.utils.geotools.{SchemaBuilder, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.PathUtils
import org.opengis.feature.simple.SimpleFeature
import org.specs2.matcher.MatchResult
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class CassandraDataStoreTest extends Specification {

  sequential

  var storage: Path = _
  var params: Map[String, String] = _
  var ds: CassandraDataStore = _

  step {
    storage = Files.createTempDirectory("cassandra")

    System.setProperty("cassandra.storagedir", storage.toString)

    EmbeddedCassandraServerHelper.startEmbeddedCassandra("cassandra-config.yaml", 1200000L)

    var readTimeout: Int = SystemProperty("cassandraReadTimeout", "12000").get.toInt

    if (readTimeout < 0) {
      readTimeout = 12000
    }
    val host = EmbeddedCassandraServerHelper.getHost
    val port = EmbeddedCassandraServerHelper.getNativeTransportPort
    val cluster = new Cluster.Builder().addContactPoints(host).withPort(port)
        .withSocketOptions(new SocketOptions().setReadTimeoutMillis(readTimeout)).build().init()
    val session = cluster.connect()
    val cqlDataLoader = new CQLDataLoader(session)
    cqlDataLoader.load(new ClassPathCQLDataSet("init.cql", false, false))

    params = Map(
      Params.ContactPointParam.getName -> s"$host:$port",
      Params.KeySpaceParam.getName -> "geomesa_cassandra",
      Params.CatalogParam.getName -> "test_sft"
    )
    ds = DataStoreFinder.getDataStore(params).asInstanceOf[CassandraDataStore]
  }

  "CassandraDataStore" should {

    "throw meaningful exceptions for invalid parameters" in {
      DataStoreFinder.getDataStore(Map(
        Params.ContactPointParam.getName -> "localhost",
        Params.KeySpaceParam.getName -> "geomesa_cassandra",
        Params.CatalogParam.getName -> "test_sft"
      )) must throwAn[IllegalArgumentException](s"Invalid parameter '${Params.ContactPointParam.key}'")
      DataStoreFinder.getDataStore(Map(
        Params.ContactPointParam.getName -> "localhost:foo",
        Params.KeySpaceParam.getName -> "geomesa_cassandra",
        Params.CatalogParam.getName -> "test_sft"
      )) must throwAn[IllegalArgumentException](s"Invalid parameter '${Params.ContactPointParam.key}'")
    }

    "work with points" in {
      val typeName = "testpoints"

      ds.getSchema(typeName) must beNull

      ds.createSchema(SimpleFeatureTypes.createType(typeName, "name:String:index=true,dtg:Date,*geom:Point:srid=4326"))

      val sft = ds.getSchema(typeName)

      sft must not(beNull)

      val ns = DataStoreFinder.getDataStore(params ++
          Map(CassandraDataStoreFactory.Params.NamespaceParam.key -> "ns0")).getSchema(typeName).getName
      ns.getNamespaceURI mustEqual "ns0"
      ns.getLocalPart mustEqual typeName

      val fs = ds.getFeatureSource(typeName).asInstanceOf[SimpleFeatureStore]

      val toAdd = (0 until 10).map { i =>
        val sf = new ScalaSimpleFeature(sft, i.toString)
        sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
        sf.setAttribute(0, s"name$i")
        sf.setAttribute(1, f"2014-01-${i + 1}%02dT00:00:01.000Z")
        sf.setAttribute(2, s"POINT(4$i 5$i)")
        sf
      }

      val ids = fs.addFeatures(new ListFeatureCollection(sft, toAdd))
      ids.asScala.map(_.getID) must containTheSameElementsAs((0 until 10).map(_.toString))

      forall(Seq(true, false)) { loose =>
        val ds = DataStoreFinder.getDataStore(params ++ Map(LooseBBoxParam.getName -> loose)).asInstanceOf[CassandraDataStore]
        forall(Seq(null, Array("geom"), Array("geom", "dtg"), Array("geom", "name"))) { transforms =>
          testQuery(ds, typeName, "INCLUDE", transforms, toAdd)
          testQuery(ds, typeName, "IN('0', '2')", transforms, Seq(toAdd(0), toAdd(2)))
          testQuery(ds, typeName, "bbox(geom,38,48,52,62) and dtg DURING 2014-01-01T00:00:00.000Z/2014-01-08T12:00:00.000Z", transforms, toAdd.dropRight(2))
          testQuery(ds, typeName, "bbox(geom,42,48,52,62)", transforms, toAdd.drop(2))
          testQuery(ds, typeName, "name < 'name5'", transforms, toAdd.take(5))
          testQuery(ds, typeName, "name = 'name5' OR name = 'name7'", transforms, Seq(toAdd(5), toAdd(7)))
          testQuery(ds, typeName, "(name = 'name5' OR name = 'name6') and bbox(geom,38,48,52,62) and dtg DURING 2014-01-01T00:00:00.000Z/2014-01-08T12:00:00.000Z", transforms, Seq(toAdd(5), toAdd(6)))
        }
      }

      def testTransforms(ds: CassandraDataStore) = {
        val transforms = Array("derived=strConcat('hello',name)", "geom")
        forall(Seq(("INCLUDE", toAdd), ("bbox(geom,42,48,52,62)", toAdd.drop(2)))) { case (filter, results) =>
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
      testLooseBbox(ds, loose = false)

      forall(Seq(true, "true", java.lang.Boolean.TRUE)) { loose =>
        val ds = DataStoreFinder.getDataStore(params ++ Map(LooseBBoxParam.getName -> loose)).asInstanceOf[CassandraDataStore]
        testLooseBbox(ds, loose = true)
      }

      forall(Seq(false, "false", java.lang.Boolean.FALSE)) { loose =>
        val ds = DataStoreFinder.getDataStore(params ++ Map(LooseBBoxParam.getName -> loose)).asInstanceOf[CassandraDataStore]
        testLooseBbox(ds, loose = false)
      }

      ds.getFeatureSource(typeName).removeFeatures(ECQL.toFilter(toAdd.map(_.getID).mkString("IN('", "','", "')")))

      forall(Seq("INCLUDE",
        "IN('0', '2')",
        "bbox(geom,42,48,52,62)",
        "bbox(geom,38,48,52,62) and dtg DURING 2014-01-01T00:00:00.000Z/2014-01-08T12:00:00.000Z",
        "(name = 'name5' OR name = 'name6') and bbox(geom,38,48,52,62) and dtg DURING 2014-01-01T00:00:00.000Z/2014-01-08T12:00:00.000Z",
        "name < 'name5'",
        "name = 'name5'")) { filter =>
        val fr = ds.getFeatureReader(new Query(typeName, ECQL.toFilter(filter)), Transaction.AUTO_COMMIT)
        SelfClosingIterator(fr).toList must beEmpty
      }
    }

    "handle bounds exclusivity" in {
      val typeName = "testquerybounds"

      ds.getSchema(typeName) must beNull

      ds.createSchema(SimpleFeatureTypes.createType(typeName, "dtg:Date,*geom:Point:srid=4326"))

      val sft = ds.getSchema(typeName)

      val feature = ScalaSimpleFeature.create(sft, "0", "2019-03-18T00:00:00.000Z", "POINT (-122.32876 47.75205)")
      feature.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)

      val ids = ds.getFeatureSource(typeName).asInstanceOf[SimpleFeatureStore]
          .addFeatures(new ListFeatureCollection(sft, Array[SimpleFeature](feature)))
      ids.asScala.map(_.getID) mustEqual Seq("0")

      val filter = ECQL.toFilter("dtg = '2019-03-18T00:00:00.000Z' AND " +
          "bbox(geom, -122.33072251081467,47.75143951177597,-122.32643097639084,47.753048837184906)")

      val query = new Query(typeName, filter)
      val results = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList
      results mustEqual Seq(feature)
    }

    "work with polys" in {
      val typeName = "testpolys"

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

      testQuery(ds, typeName, "INCLUDE", null, toAdd)
      testQuery(ds, typeName, "IN('0', '2')", null, Seq(toAdd(0), toAdd(2)))
      testQuery(ds, typeName, "bbox(geom,-126,38,-119,52) and dtg DURING 2014-01-01T00:00:00.000Z/2014-01-01T07:59:59.000Z", null, toAdd.dropRight(2))
      testQuery(ds, typeName, "bbox(geom,-126,42,-119,45)", null, toAdd.dropRight(4))
      testQuery(ds, typeName, "name < 'name5'", null, toAdd.take(5))
      testQuery(ds, typeName, "(name = 'name5' OR name = 'name6') and bbox(geom,-126,38,-119,52) and dtg DURING 2014-01-01T00:00:00.000Z/2014-01-01T07:59:59.000Z", null, Seq(toAdd(5), toAdd(6)))
    }

    "delete schemas" in {
      val typeName = "testdelete"

      ds.getSchema(typeName) must beNull

      val inputsft =
        SchemaBuilder.builder()
          .addString("name")
          .addDate("dtg")
          .addPoint("geom", default = true)
          .userData.indices(List("z3"))
          .build(typeName)

      ds.createSchema(inputsft)

      val sft = ds.getSchema(typeName)

      sft must not(beNull)

      val fs = ds.getFeatureSource(typeName).asInstanceOf[SimpleFeatureStore]

      val toAdd = (0 until 2).map { i =>
        val sf = new ScalaSimpleFeature(sft, i.toString)
        sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
        sf.setAttribute(0, s"name$i")
        sf.setAttribute(1, f"2014-01-${i + 1}%02dT00:00:01.000Z")
        sf.setAttribute(2, s"POINT(4$i 5$i)")
        sf
      }

      val ids = fs.addFeatures(new ListFeatureCollection(sft, toAdd))
      ids.asScala.map(_.getID) must containTheSameElementsAs((0 until 2).map(_.toString))

      val filters = Seq("INCLUDE", "IN('0', '1')", "name = 'name0' OR name = 'name1'", "bbox(geom, 39, 49, 42, 52)",
        "bbox(geom, 39, 49, 42, 52) AND dtg during 2014-01-01T00:00:00.000Z/2014-01-03T00:00:00.000Z")

      forall(filters) { f =>
        SelfClosingIterator(fs.getFeatures(ECQL.toFilter(f))).toSeq must containTheSameElementsAs(toAdd)
      }

      val fw = ds.getFeatureWriter(typeName, ECQL.toFilter("IN('1')"), Transaction.AUTO_COMMIT)
      fw.hasNext must beTrue
      fw.next
      fw.remove()
      fw.hasNext must beFalse
      fw.close()

      forall(filters) { f =>
        SelfClosingIterator(fs.getFeatures(ECQL.toFilter(f))).toSeq mustEqual Seq(toAdd.head)
      }

      ds.removeSchema(typeName)

      ds.getSchema(typeName) must beNull
    }

    "support long attribute names" in {
      val long = "testsftverylongnaaaaaaaaaaammmmmmmeeeeeeeee"
      val spec = s"$long:String:index=true,dtg:Date,*geom:Point:srid=4326"
      foreach(Seq("testnames", long)) { typeName =>
        ds.getSchema(typeName) must beNull
        ds.createSchema(SimpleFeatureTypes.createType(typeName, spec)) must not(throwAn[Exception])
      }
    }
  }

  def testQuery(ds: CassandraDataStore,
                typeName: String,
                filter: String,
                transforms: Array[String],
                results: Seq[SimpleFeature],
                explain: Option[Explainer] = None): MatchResult[Traversable[SimpleFeature]] = {
    val query = new Query(typeName, ECQL.toFilter(filter), transforms)
    explain.foreach(e => ds.getQueryPlan(query, explainer = e))
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
  }

  step {
    ds.dispose()
    // note: no way to shutdown c* cluster, and clean method throws exception on system namespace
    PathUtils.deleteRecursively(storage)
  }
}
