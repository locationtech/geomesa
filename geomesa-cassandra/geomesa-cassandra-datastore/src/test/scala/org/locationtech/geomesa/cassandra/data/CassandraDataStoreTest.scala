/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.cassandra.data

import com.datastax.oss.driver.api.core.{AllNodesFailedException, DefaultConsistencyLevel}
import com.datastax.oss.driver.api.core.cql.SimpleStatement
import com.datastax.oss.driver.api.core.servererrors.UnavailableException
import org.geotools.api.data._
import org.geotools.api.feature.simple.SimpleFeature
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.filter.text.ecql.ECQL
import org.geotools.util.factory.Hints
import org.junit.runner.RunWith
import org.locationtech.geomesa.cassandra.data.CassandraDataStoreFactory.Params
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.filter.FilterHelper
import org.locationtech.geomesa.index.conf.{QueryHints, QueryProperties}
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory.LooseBBoxParam
import org.locationtech.geomesa.index.utils.{ExplainString, Explainer}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.conf.FeatureExpiration.FeatureTimeExpiration
import org.locationtech.geomesa.utils.geotools.{SchemaBuilder, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.CloseWithLogging
import org.slf4j.LoggerFactory
import org.specs2.matcher.MatchResult
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.specs2.specification.BeforeAfterAll
import org.testcontainers.cassandra.{CassandraContainer, CassandraDatabaseDelegate}
import org.testcontainers.containers.BindMode
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.utility.{DockerImageName, MountableFile}

import java.io.IOException
import scala.concurrent.duration.DurationInt

@RunWith(classOf[JUnitRunner])
class CassandraDataStoreTest extends Specification with BeforeAfterAll {

  import scala.collection.JavaConverters._

  sequential

  protected def createContainer(): CassandraContainer = {
    new CassandraContainer(DockerImageName.parse("cassandra").withTag(sys.props.getOrElse("cassandra.docker.tag", "5.0.9")))
      .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("cassandra")))
      .withFileSystemBind(MountableFile.forClasspathResource("init.cql").getResolvedPath, "/init.cql", BindMode.READ_ONLY)
  }

  val container: CassandraContainer = createContainer()

  lazy val params: Map[String, String] = Map(
    Params.ContactPointParam.getName -> s"${container.getContactPoint.getHostString}:${container.getContactPoint.getPort}",
    Params.LocalDatacenterParam.getName -> "datacenter1",
    Params.KeySpaceParam.getName -> "geomesa_cassandra",
    Params.CatalogParam.getName -> "test_sft"
  )
  var ds: CassandraDataStore = _

  override def beforeAll(): Unit = {
    container.start()
    // re-create initScript logic without using copyFileToContainer (which fails with rootless docker)
    new CassandraDatabaseDelegate(container).execute(null, "/init.cql", -1, false, false)
    ds = DataStoreFinder.getDataStore(params.asJava).asInstanceOf[CassandraDataStore]
  }

  override def afterAll(): Unit = {
    CloseWithLogging(ds)
    container.close()
  }

  "CassandraDataStore" should {

    "throw meaningful exceptions for invalid parameters" in {
      DataStoreFinder.getDataStore(Map(
        Params.ContactPointParam.getName -> "localhost",
        Params.LocalDatacenterParam.getName -> "datacenter1",
        Params.KeySpaceParam.getName -> "geomesa_cassandra",
        Params.CatalogParam.getName -> "test_sft"
      ).asJava) must throwAn[IllegalArgumentException](s"Invalid parameter '${Params.ContactPointParam.key}'")
      DataStoreFinder.getDataStore(Map(
        Params.ContactPointParam.getName -> "localhost:foo",
        Params.LocalDatacenterParam.getName -> "datacenter1",
        Params.KeySpaceParam.getName -> "geomesa_cassandra",
        Params.CatalogParam.getName -> "test_sft"
      ).asJava) must throwAn[IllegalArgumentException](s"Invalid parameter '${Params.ContactPointParam.key}'")
    }

    "work with points" in {
      val typeName = "testpoints"

      ds.getSchema(typeName) must beNull

      ds.createSchema(SimpleFeatureTypes.createType(typeName, "name:String:index=true,dtg:Date,*geom:Point:srid=4326"))

      val sft = ds.getSchema(typeName)

      sft must not(beNull)

      val ns = DataStoreFinder.getDataStore((params ++
          Map(CassandraDataStoreFactory.Params.NamespaceParam.key -> "ns0")).asJava).getSchema(typeName).getName
      ns.getNamespaceURI mustEqual "ns0"
      ns.getLocalPart mustEqual typeName

      val fs = ds.getFeatureSource(typeName).asInstanceOf[SimpleFeatureStore]

      val toAdd = (0 until 10).map { i =>
        val sf = new ScalaSimpleFeature(sft, i.toString)
        sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
        sf.setAttribute(0, s"name$i")
        sf.setAttribute(1, f"2014-01-${i + 1}%02dT00:00:01.000Z")
        sf.setAttribute(2, s"POINT(4$i 5$i)")
        sf: SimpleFeature
      }

      val ids = fs.addFeatures(new ListFeatureCollection(sft, toAdd.asJava))
      ids.asScala.map(_.getID) must containTheSameElementsAs((0 until 10).map(_.toString))

      forall(Seq(true, false)) { loose =>
        val ds = DataStoreFinder.getDataStore((params ++ Map(LooseBBoxParam.getName -> loose)).asJava).asInstanceOf[CassandraDataStore]
        forall(Seq(null, Array("geom"), Array("geom", "dtg"), Array("geom", "name"))) { transforms =>
          testQuery(ds, typeName, "INCLUDE", transforms, toAdd)
          testQuery(ds, typeName, "IN('0', '2')", transforms, Seq(toAdd(0), toAdd(2)))
          testQuery(ds, typeName, "bbox(geom,38,48,52,62) and dtg DURING 2014-01-01T00:00:00.000Z/2014-01-08T12:00:00.000Z", transforms, toAdd.dropRight(2))
          testQuery(ds, typeName, "bbox(geom,42,48,52,62)", transforms, toAdd.drop(2))
          testQuery(ds, typeName, "name < 'name5'", transforms, toAdd.take(5))
          testQuery(ds, typeName, "name = 'name5' OR name = 'name7'", transforms, Seq(toAdd(5), toAdd(7)))
          testQuery(ds, typeName, "(name = 'name5' OR name = 'name6') and bbox(geom,38,48,52,62) and dtg DURING 2014-01-01T00:00:00.000Z/2014-01-08T12:00:00.000Z", transforms, Seq(toAdd(5), toAdd(6)))
          // open-ended time filters
          testQuery(ds, typeName, "dtg AFTER 2014-01-05T12:00:00.000Z", transforms, toAdd.drop(5))
          testQuery(ds, typeName, "dtg BEFORE 2014-01-05T12:00:00.000Z", transforms, toAdd.take(5))
          testQuery(ds, typeName, "dtg > '2014-01-08T00:00:00.000Z'", transforms, toAdd.drop(7))
          testQuery(ds, typeName, "dtg BEFORE 2014-01-02T12:00:00.000Z OR dtg AFTER 2014-01-08T12:00:00.000Z", transforms,
            toAdd.take(2) ++ toAdd.drop(8))
        }
      }

      // open-ended time filter with a spatial filter, forced to use the z3 index (the z values are only applied
      // client-side for the open-ended periods)
      foreach(Seq("dtg AFTER 2014-01-05T12:00:00.000Z", "dtg BEFORE 2014-01-05T12:00:00.000Z")) { time =>
        val query = new Query(typeName, ECQL.toFilter(s"bbox(geom,42,48,52,62) AND $time"))
        query.getHints.put(QueryHints.QUERY_INDEX, "z3")
        val expected = if (time.contains("AFTER")) { toAdd.drop(5) } else { toAdd.slice(2, 5) }
        val ids = CloseableIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList.map(_.getID)
        ids must haveLength(expected.length)
        ids must containTheSameElementsAs(expected.map(_.getID))
      }

      def testTransforms(ds: CassandraDataStore) = {
        val transforms = Array("derived=strConcat('hello',name)", "geom")
        forall(Seq(("INCLUDE", toAdd), ("bbox(geom,42,48,52,62)", toAdd.drop(2)))) { case (filter, results) =>
          val fr = ds.getFeatureReader(new Query(typeName, ECQL.toFilter(filter), transforms: _*), Transaction.AUTO_COMMIT)
          val features = CloseableIterator(fr).toList
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

      def testLooseBbox(ds: CassandraDataStore, loose: Boolean): MatchResult[Option[String]] = {
        val filter = ECQL.toFilter("dtg DURING 2014-01-01T00:00:00.000Z/2014-01-08T12:00:00.000Z")
        val out = new ExplainString
        ds.getQueryPlan(new Query(typeName, filter), explainer = out)
        val filterLine = "Client-side filter: "
        val clientSideFilter = out.toString.split("\n").map(_.trim).find(_.startsWith(filterLine)).map(_.substring(filterLine.length))
        if (loose) {
          clientSideFilter must beSome("none")
        } else {
          clientSideFilter must beSome(FilterHelper.toString(filter))
        }
      }

      // test default loose bbox config
      testLooseBbox(ds, loose = false)

      forall(Seq(true, "true", java.lang.Boolean.TRUE)) { loose =>
        val ds = DataStoreFinder.getDataStore((params ++ Map(LooseBBoxParam.getName -> loose)).asJava).asInstanceOf[CassandraDataStore]
        testLooseBbox(ds, loose = true)
      }

      forall(Seq(false, "false", java.lang.Boolean.FALSE)) { loose =>
        val ds = DataStoreFinder.getDataStore((params ++ Map(LooseBBoxParam.getName -> loose)).asJava).asInstanceOf[CassandraDataStore]
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
        CloseableIterator(fr).toList must beEmpty
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
          .addFeatures(new ListFeatureCollection(sft, feature))
      ids.asScala.map(_.getID) mustEqual Seq("0")

      val filter = ECQL.toFilter("dtg = '2019-03-18T00:00:00.000Z' AND " +
          "bbox(geom, -122.33072251081467,47.75143951177597,-122.32643097639084,47.753048837184906)")

      val query = new Query(typeName, filter)
      val results = CloseableIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList
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
        sf: SimpleFeature
      }

      val ids = fs.addFeatures(new ListFeatureCollection(sft, toAdd.asJava))
      ids.asScala.map(_.getID) must containTheSameElementsAs((0 until 10).map(_.toString))

      testQuery(ds, typeName, "INCLUDE", null, toAdd)
      testQuery(ds, typeName, "IN('0', '2')", null, Seq(toAdd(0), toAdd(2)))
      testQuery(ds, typeName, "bbox(geom,-126,38,-119,52) and dtg DURING 2014-01-01T00:00:00.000Z/2014-01-01T07:59:59.000Z", null, toAdd.dropRight(2))
      testQuery(ds, typeName, "bbox(geom,-126,42,-119,45)", null, toAdd.dropRight(4))
      testQuery(ds, typeName, "name < 'name5'", null, toAdd.take(5))
      testQuery(ds, typeName, "(name = 'name5' OR name = 'name6') and bbox(geom,-126,38,-119,52) and dtg DURING 2014-01-01T00:00:00.000Z/2014-01-01T07:59:59.000Z", null, Seq(toAdd(5), toAdd(6)))
      // open-ended time filters
      testQuery(ds, typeName, "dtg AFTER 2014-01-01T04:30:00.000Z", null, toAdd.drop(5))
      testQuery(ds, typeName, "dtg BEFORE 2014-01-01T04:30:00.000Z", null, toAdd.take(5))
    }

    "apply full table scan blocks to open-ended time filters" in {
      val typeName = "testblockfulltable"

      ds.getSchema(typeName) must beNull

      ds.createSchema(SimpleFeatureTypes.createType(typeName, "dtg:Date,*geom:Point:srid=4326"))

      val sft = ds.getSchema(typeName)

      val toAdd = (0 until 10).map { i =>
        val sf = ScalaSimpleFeature.create(sft, i.toString, f"2014-01-${i + 1}%02dT00:00:01.000Z", s"POINT(4$i 5$i)")
        sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
        sf: SimpleFeature
      }

      ds.getFeatureSource(typeName).asInstanceOf[SimpleFeatureStore].addFeatures(new ListFeatureCollection(sft, toAdd.asJava))

      // open-ended periods are selected with a single 'allow filtering' statement (not one per shard),
      // while the boundary period still uses the index
      foreach(Seq("dtg AFTER 2014-01-05T12:00:00.000Z", "dtg BEFORE 2014-01-05T12:00:00.000Z")) { filter =>
        val plans = ds.getQueryPlan(new Query(typeName, ECQL.toFilter(filter)))
        val statements = plans.flatMap(_.asInstanceOf[CassandraQueryPlan].ranges).map(_.getQuery)
        val (filtering, indexed) = statements.partition(_.contains("ALLOW FILTERING"))
        filtering must haveLength(1)
        filtering.head must not(contain("shard"))
        indexed must not(beEmpty)
      }

      def blocked(filter: String): MatchResult[Any] = {
        val query = new Query(typeName, ECQL.toFilter(filter))
        CloseableIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList must throwA[RuntimeException].like {
          case e => Iterator.iterate[Throwable](e)(_.getCause).takeWhile(_ != null).map(_.getMessage).mkString(" ") must
              contain("Full-table scans are disabled")
        }
      }

      QueryProperties.BlockFullTableScans.threadLocalValue.set("true")
      try {
        // bounded time filters can use the index
        testQuery(ds, typeName, "dtg DURING 2014-01-01T00:00:00.000Z/2014-01-08T12:00:00.000Z", null, toAdd.take(8))
        // open-ended time filters fall back to a full table scan in cassandra, so they should be blocked
        blocked("dtg AFTER 2014-01-05T12:00:00.000Z")
        blocked("dtg BEFORE 2014-01-05T12:00:00.000Z")
        // unless max features is set
        val limited = new Query(typeName, ECQL.toFilter("dtg AFTER 2014-01-05T12:00:00.000Z"), 10, null: Array[String], null)
        CloseableIterator(ds.getFeatureReader(limited, Transaction.AUTO_COMMIT)).toList.map(_.getID) must
            containTheSameElementsAs(toAdd.drop(5).map(_.getID))
      } finally {
        QueryProperties.BlockFullTableScans.threadLocalValue.remove()
      }

      // now they should go through
      testQuery(ds, typeName, "dtg AFTER 2014-01-05T12:00:00.000Z", null, toAdd.drop(5))
      testQuery(ds, typeName, "dtg BEFORE 2014-01-05T12:00:00.000Z", null, toAdd.take(5))
    }

    "apply the configured query consistency to reads" in {
      val typeName = "testconsistency"

      ds.getSchema(typeName) must beNull

      ds.createSchema(SimpleFeatureTypes.createType(typeName, "name:String:index=true,dtg:Date,*geom:Point:srid=4326"))

      val sft = ds.getSchema(typeName)

      val toAdd = (0 until 10).map { i =>
        val sf = ScalaSimpleFeature.create(sft, i.toString, s"name$i", f"2014-01-${i + 1}%02dT00:00:01.000Z", s"POINT(4$i 5$i)")
        sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
        sf: SimpleFeature
      }

      ds.getFeatureSource(typeName).asInstanceOf[SimpleFeatureStore].addFeatures(new ListFeatureCollection(sft, toAdd.asJava))

      // covers full table scans, id, attribute, z3 with multiple ranges/shards and 'allow filtering' statements
      val filters = Seq(
        "INCLUDE",
        "IN('0', '2')",
        "name < 'name5'",
        "bbox(geom,38,48,52,62) and dtg DURING 2014-01-01T00:00:00.000Z/2014-01-08T12:00:00.000Z",
        "dtg AFTER 2014-01-05T12:00:00.000Z"
      )

      def statements(store: CassandraDataStore, filter: String): Seq[SimpleStatement] =
        store.getQueryPlan(new Query(typeName, ECQL.toFilter(filter))).flatMap(_.asInstanceOf[CassandraQueryPlan].ranges)

      def withConsistency[T](level: String)(fn: CassandraDataStore => T): T = {
        val params = this.params ++ Map(Params.QueryConsistencyParam.getName -> level)
        val store = DataStoreFinder.getDataStore(params.asJava).asInstanceOf[CassandraDataStore]
        try { fn(store) } finally { store.dispose() }
      }

      // by default, the consistency is not set and the driver default is used
      foreach(filters) { filter =>
        val ranges = statements(ds, filter)
        ranges must not(beEmpty)
        foreach(ranges)(_.getConsistencyLevel must beNull)
      }

      // when configured, it's applied to every query statement
      withConsistency("LOCAL_QUORUM") { store =>
        foreach(filters) { filter =>
          val ranges = statements(store, filter)
          ranges must not(beEmpty)
          foreach(ranges)(_.getConsistencyLevel mustEqual DefaultConsistencyLevel.LOCAL_QUORUM)
        }
        testQuery(store, typeName, "INCLUDE", null, toAdd)
      }

      // the test keyspace has a replication factor of 1, so it can't satisfy 'TWO' - this verifies that the
      // configured level is sent to cassandra for metadata reads
      withConsistency("TWO")(_.getTypeNames.toSeq) must throwAn[Exception].like { case e =>
        // the driver retries on the next node, so the error is reported per-node in an AllNodesFailedException
        val errors = Iterator.iterate[Throwable](e)(_.getCause).takeWhile(_ != null).toSeq.flatMap {
          case all: AllNodesFailedException => all +: all.getAllErrors.asScala.values.flatMap(_.asScala).toSeq
          case t => Seq(t)
        }
        errors must contain(beAnInstanceOf[UnavailableException])
      }

      // invalid values and levels that don't apply to regular reads are rejected
      foreach(Seq("ANY", "SERIAL", "LOCAL_SERIAL", "quorum", "FOO")) { level =>
        val params = this.params ++ Map(Params.QueryConsistencyParam.getName -> level)
        new CassandraDataStoreFactory().createDataStore(params.asJava) must throwAn[IOException].like { case e =>
          e.getMessage must contain(s"Invalid property for parameter '${Params.QueryConsistencyParam.getName}'")
          e.getMessage must contain("Accepted values are: ONE, TWO, THREE, QUORUM, ALL, LOCAL_ONE, LOCAL_QUORUM, EACH_QUORUM")
        }
      }
    }

    "disable uuid encoding of feature ids" in {
      import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

      // the fid column is text, so uuids can't be stored in their encoded (binary) form
      val configs = Seq(
        "testuuids" -> Map(SimpleFeatureTypes.Configs.FidsAreUuids -> "true"),
        "testuuidsencoded" ->
            Map(SimpleFeatureTypes.Configs.FidsAreUuids -> "true", SimpleFeatureTypes.Configs.FidsAreUuidEncoded -> "true")
      )

      foreach(configs) { case (typeName, userData) =>
        ds.getSchema(typeName) must beNull

        val toCreate = SimpleFeatureTypes.createType(typeName, "name:String:index=true,dtg:Date,*geom:Point:srid=4326")
        userData.foreach { case (k, v) => toCreate.getUserData.put(k, v) }
        toCreate.isUuidEncoded must beTrue
        ds.createSchema(toCreate)

        val sft = ds.getSchema(typeName)
        sft.isUuid must beTrue
        sft.isUuidEncoded must beFalse

        // include uuids with bytes that aren't valid utf-8 when encoded
        val ids = Seq(
          "2f1c3d4e-5a6b-4c7d-8e9f-0a1b2c3d4e5f",
          "9b8a7c6d-5e4f-4a3b-9c2d-1e0f9a8b7c6d",
          "ffffffff-ffff-4fff-bfff-ffffffffffff",
          "00000000-0000-4000-8000-000000000000"
        )

        val toAdd = ids.zipWithIndex.map { case (id, i) =>
          val sf = ScalaSimpleFeature.create(sft, id, s"name$i", f"2014-01-${i + 1}%02dT00:00:01.000Z", s"POINT(4$i 5$i)")
          sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
          sf: SimpleFeature
        }

        val fs = ds.getFeatureSource(typeName).asInstanceOf[SimpleFeatureStore]
        fs.addFeatures(new ListFeatureCollection(sft, toAdd.asJava)).asScala.map(_.getID) must containTheSameElementsAs(ids)

        // query each index
        testQuery(ds, typeName, "INCLUDE", null, toAdd)
        testQuery(ds, typeName, s"IN('${ids(0)}', '${ids(2)}')", null, Seq(toAdd(0), toAdd(2)))
        testQuery(ds, typeName, "name = 'name1'", null, Seq(toAdd(1)))
        testQuery(ds, typeName, "bbox(geom,38,48,52,62)", null, toAdd)
        testQuery(ds, typeName, "bbox(geom,38,48,52,62) and dtg DURING 2014-01-01T00:00:00.000Z/2014-01-08T12:00:00.000Z", null, toAdd)

        // deletes have to remove the rows from each index
        fs.removeFeatures(ECQL.toFilter(s"IN('${ids(1)}')"))
        val remaining = toAdd.filterNot(_.getID == ids(1))
        testQuery(ds, typeName, "INCLUDE", null, remaining)
        testQuery(ds, typeName, s"IN('${ids(1)}')", null, Seq.empty)
        testQuery(ds, typeName, "name = 'name1'", null, Seq.empty)
        testQuery(ds, typeName, "bbox(geom,38,48,52,62) and dtg DURING 2014-01-01T00:00:00.000Z/2014-01-08T12:00:00.000Z", null, remaining)

        ds.removeSchema(typeName)
        ds.getSchema(typeName) must beNull
      }
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
        sf: SimpleFeature
      }

      val ids = fs.addFeatures(new ListFeatureCollection(sft, toAdd.asJava))
      ids.asScala.map(_.getID) must containTheSameElementsAs((0 until 2).map(_.toString))

      val filters = Seq("INCLUDE", "IN('0', '1')", "name = 'name0' OR name = 'name1'", "bbox(geom, 39, 49, 42, 52)",
        "bbox(geom, 39, 49, 42, 52) AND dtg during 2014-01-01T00:00:00.000Z/2014-01-03T00:00:00.000Z")

      forall(filters) { f =>
        CloseableIterator(fs.getFeatures(ECQL.toFilter(f)).features()).toList must containTheSameElementsAs(toAdd)
      }

      val fw = ds.getFeatureWriter(typeName, ECQL.toFilter("IN('1')"), Transaction.AUTO_COMMIT)
      fw.hasNext must beTrue
      fw.next
      fw.remove()
      fw.hasNext must beFalse
      fw.close()

      forall(filters) { f =>
        CloseableIterator(fs.getFeatures(ECQL.toFilter(f)).features()).toList mustEqual Seq(toAdd.head)
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

    "support ingest-time feature expiry" in {
      import SimpleFeatureTypes.Configs.FeatureExpiration

      val typeName = "testexpiry"
      val spec = "name:String:index=true,dtg:Date,*geom:Point:srid=4326"

      def ttl(table: String): Int = {
        val cql = "SELECT default_time_to_live FROM system_schema.tables WHERE keyspace_name = ? AND table_name = ?"
        ds.session.execute(SimpleStatement.newInstance(cql, "geomesa_cassandra", table)).one().getInt(0)
      }
      def ttls(): Seq[Int] = ds.getAllIndexTableNames(typeName).map(ttl)
      def update(expiry: String): Unit = {
        val sft = SimpleFeatureTypes.copy(ds.getSchema(typeName))
        sft.getUserData.put(FeatureExpiration, expiry) // an empty value removes the expiry
        ds.updateSchema(typeName, sft)
      }
      def query(filter: String): Seq[SimpleFeature] =
        CloseableIterator(ds.getFeatureReader(new Query(typeName, ECQL.toFilter(filter)), Transaction.AUTO_COMMIT)).toList

      ds.getSchema(typeName) must beNull
      ds.createSchema(SimpleFeatureTypes.createType(typeName, s"$spec;$FeatureExpiration='8 seconds'"))
      val sft = ds.getSchema(typeName)
      sft must not(beNull)

      ds.getAllIndexTableNames(typeName) must haveLength(4) // id, z3, z2, attr
      ttls() must contain(beEqualTo(8)).forall
      ttl(params(Params.CatalogParam.getName)) mustEqual 0 // the catalog table should never expire

      val features = (0 until 2).map { i =>
        val sf = ScalaSimpleFeature.create(sft, s"$i", s"name$i", "2024-01-01T00:00:00.000Z", s"POINT(4$i 5$i)")
        sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
        sf: SimpleFeature
      }
      ds.getFeatureSource(typeName).asInstanceOf[SimpleFeatureStore]
          .addFeatures(new ListFeatureCollection(sft, features.asJava))

      val filters = Seq("INCLUDE", "IN('0', '1')", "name = 'name0' OR name = 'name1'", "bbox(geom, 39, 49, 42, 52)",
        "bbox(geom, 39, 49, 42, 52) AND dtg during 2023-12-31T00:00:00.000Z/2024-01-02T00:00:00.000Z")

      foreach(filters)(f => query(f) must containTheSameElementsAs(features))
      eventually(40, 500.millis)(foreach(filters)(f => query(f) must beEmpty))

      // update the expiry on the existing tables
      update("1 day")
      ttls() must contain(beEqualTo(86400)).forall
      // remove the expiry
      update("")
      ttls() must contain(beEqualTo(0)).forall

      // switching to attribute-based expiration removes the table-level ttl
      update("1 day")
      ttls() must contain(beEqualTo(86400)).forall
      // updating with the same expiration re-applies the ttl, e.g. for tables created before expiration was supported
      ds.getAllIndexTableNames(typeName).foreach { table =>
        ds.session.execute(org.locationtech.geomesa.cassandra.schemaStatement(s"ALTER TABLE $table WITH default_time_to_live = 0"))
      }
      ttls() must contain(beEqualTo(0)).forall
      update("1 day")
      ttls() must contain(beEqualTo(86400)).forall
      update("dtg(1 day)")
      ttls() must contain(beEqualTo(0)).forall

      // unsupported expirations
      update("7301 days") must throwAn[IllegalArgumentException]
      ds.getSchema(typeName).getUserData.get(FeatureExpiration) mustEqual "dtg(1 day)"
      val unsupported = SimpleFeatureTypes.createType("testexpiryinvalid", s"$spec;$FeatureExpiration='7301 days'")
      ds.createSchema(unsupported) must throwAn[IllegalArgumentException]
      ds.getSchema("testexpiryinvalid") must beNull

      ds.removeSchema(typeName)
      ds.getSchema(typeName) must beNull
    }

    "support attribute-based feature expiry" in {
      import SimpleFeatureTypes.Configs.FeatureExpiration

      val typeName = "testattributeexpiry"
      val spec = "name:String:index=true,dtg:Date,*geom:Point:srid=4326"

      def query(filter: String): Seq[SimpleFeature] =
        CloseableIterator(ds.getFeatureReader(new Query(typeName, ECQL.toFilter(filter)), Transaction.AUTO_COMMIT)).toList

      ds.getSchema(typeName) must beNull
      ds.createSchema(SimpleFeatureTypes.createType(typeName, s"$spec;$FeatureExpiration='dtg(8 seconds)'"))
      val sft = ds.getSchema(typeName)
      sft must not(beNull)

      val tables = ds.getAllIndexTableNames(typeName)
      tables must haveLength(4) // id, z3, z2, attr
      // no table-level ttl, as it's set per row
      foreach(tables) { table =>
        val cql = "SELECT default_time_to_live FROM system_schema.tables WHERE keyspace_name = ? AND table_name = ?"
        ds.session.execute(SimpleStatement.newInstance(cql, "geomesa_cassandra", table)).one().getInt(0) mustEqual 0
      }

      val now = System.currentTimeMillis()
      val dates = Seq(
        new java.util.Date(now), // expires in 8 seconds
        new java.util.Date(now - 86400000L), // already expired, not written
        null, // no date to expire from, expired like in other data stores
        new java.util.Date(now + 365L * 86400000L) // expires in a year
      )
      val features = dates.zipWithIndex.map { case (date, i) =>
        val sf = ScalaSimpleFeature.create(sft, s"$i", s"name$i", date, s"POINT(4$i 5$i)")
        sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
        sf: SimpleFeature
      }
      ds.getFeatureSource(typeName).asInstanceOf[SimpleFeatureStore]
          .addFeatures(new ListFeatureCollection(sft, features.asJava))

      val filters = Seq("INCLUDE", "IN('0', '1', '2', '3')", "name IN ('name0', 'name1', 'name2', 'name3')",
        "bbox(geom, 39, 49, 44, 54)")

      foreach(filters)(f => query(f).map(_.getID) must containTheSameElementsAs(Seq("0", "3")))

      // check the ttl on the stored rows
      foreach(tables) { table =>
        val ttl = ds.session.execute(s"SELECT TTL(sf) FROM $table").all().asScala.map(_.getInt(0))
        ttl must haveLength(2)
        ttl.max must beBetween(365 * 86400 - 60, 365 * 86400 + 8)
        ttl.min must beBetween(1, 8)
      }

      // ttls are capped at the max supported by cassandra
      val expiration = FeatureTimeExpiration("dtg", sft.indexOf("dtg"), 8.seconds)
      val farFuture = ScalaSimpleFeature.create(sft, "4", "name4", new java.util.Date(now + 100L * 365 * 86400000L),
        "POINT(44 54)")
      CassandraIndexAdapter.timeToLive(expiration, farFuture) mustEqual CassandraIndexAdapter.MaxTimeToLive

      eventually(40, 500.millis)(foreach(filters)(f => query(f).map(_.getID) mustEqual Seq("3")))

      // updating the feature recalculates the ttl - an expired date removes it
      val writer = ds.getFeatureWriter(typeName, ECQL.toFilter("IN('3')"), Transaction.AUTO_COMMIT)
      try {
        writer.hasNext must beTrue
        writer.next().setAttribute("dtg", new java.util.Date(now - 86400000L))
        writer.write()
      } finally {
        writer.close()
      }
      foreach(filters)(f => query(f) must beEmpty)

      ds.removeSchema(typeName)
      ds.getSchema(typeName) must beNull
    }
  }

  def testQuery(ds: CassandraDataStore,
                typeName: String,
                filter: String,
                transforms: Array[String],
                results: Seq[SimpleFeature],
                explain: Option[Explainer] = None): MatchResult[Traversable[SimpleFeature]] = {
    val query = new Query(typeName, ECQL.toFilter(filter), transforms: _*)
    explain.foreach(e => ds.getQueryPlan(query, explainer = e))
    val fr = ds.getFeatureReader(query, Transaction.AUTO_COMMIT)
    val features = CloseableIterator(fr).toList
    val attributes = Option(transforms).getOrElse(ds.getSchema(typeName).getAttributeDescriptors.asScala.map(_.getLocalName).toArray)
    features.map(_.getID) must containTheSameElementsAs(results.map(_.getID))
    features must haveLength(results.length) // no duplicates
    forall(features) { feature =>
      feature.getAttributes must haveLength(attributes.length)
      forall(attributes.zipWithIndex) { case (attribute, i) =>
        feature.getAttribute(attribute) mustEqual feature.getAttribute(i)
        feature.getAttribute(attribute) mustEqual results.find(_.getID == feature.getID).get.getAttribute(attribute)
      }
    }
  }
}
