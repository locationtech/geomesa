/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.hbase.data

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.security.SecurityCapability
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}
import org.apache.hadoop.hbase.security.User
import org.apache.hadoop.hbase.security.visibility.VisibilityClient
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.geotools.api.data.{DataStoreFinder, Query, SimpleFeatureStore, Transaction}
import org.geotools.api.feature.simple.SimpleFeature
import org.geotools.api.filter.Filter
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.filter.text.ecql.ECQL
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.geotools.util.factory.Hints
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.filter.FilterHelper
import org.locationtech.geomesa.hbase.data.HBaseDataStoreParams._
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.index.iterators.DensityScan
import org.locationtech.geomesa.security.AuthorizationsProvider
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.CloseWithLogging
import org.locationtech.jts.geom.Envelope
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.specs2.specification.BeforeAfterAll

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets
import java.security.PrivilegedExceptionAction

@RunWith(classOf[JUnitRunner])
class HBaseVisibilityTest extends Specification with BeforeAfterAll with LazyLogging {

  import scala.collection.JavaConverters._

  sequential

  private var adminUser: UserConnection = _
  private var user1:     UserConnection = _
  private var user2:     UserConnection = _
  private var privUser:  UserConnection = _
  private var dynUser:   UserConnection = _

  private lazy val conf = {
    val conf = new Configuration(HBaseConfiguration.create())
    conf.addResource(new ByteArrayInputStream(HBaseCluster.hbaseSiteXml.getBytes(StandardCharsets.UTF_8)))
    conf
  }

  private case class UserConnection(user: User, connection: Connection) {
    def exec[T](fn: => T): T = {
      user.runAs(new PrivilegedExceptionAction[T]() {
        override def run(): T = fn
      })
    }
  }

  private object UserConnection {
    def apply(name: String, groups: Array[String] = Array.empty): UserConnection = {
      val user = User.createUserForTesting(conf, name, groups)
      val connection = user.runAs(new PrivilegedExceptionAction[Connection]() {
        override def run(): Connection = ConnectionFactory.createConnection(conf)
      })
      UserConnection(user, connection)
    }
  }

  def setAuths(user: String, auths: Array[String]): Unit =
    adminUser.exec(VisibilityClient.setAuths(adminUser.connection, auths, user))

  def getAuths(user: String): Seq[String] =
    adminUser.exec(VisibilityClient.getAuths(adminUser.connection, user).getAuthList.asScala.map(_.toStringUtf8).toSeq)

  def clearAllAuths(user: String): Unit =
    adminUser.exec(VisibilityClient.clearAuths(adminUser.connection, getAuths(user).toArray, user))

  override def beforeAll(): Unit = {
    logger.info("Starting Visibility Test")

    adminUser = UserConnection("admin")
    user1     = UserConnection("user1")
    user2     = UserConnection("user2")
    privUser  = UserConnection("privUser")
    dynUser   = UserConnection("dynUser")

    adminUser.exec({
      val labels = Array[String]("extra", "admin", "vis1", "vis2", "vis3", "super")
      HBaseIndexAdapter.waitForTable(adminUser.connection.getAdmin, TableName.valueOf("hbase:labels"))
      VisibilityClient.addLabels(adminUser.connection, labels)
      val start = System.currentTimeMillis()
      while (labels.diff(VisibilityClient.listLabels(adminUser.connection, null).getLabelList.asScala.map(_.toStringUtf8)).nonEmpty) {
        if (System.currentTimeMillis() - start > 10000) {
          throw new RuntimeException("Timed out waiting for labels to be created")
        }
        Thread.sleep(100)
      }
      setAuths("user1", Array("vis1"))
      setAuths("user2", Array("vis2"))
      setAuths("privUser", Array("super", "vis3"))
    })

    logger.info("Successfully created authorizations")
  }

  override def afterAll(): Unit =
    CloseWithLogging(Seq(adminUser, user1, user2, privUser, dynUser).map(_.connection))

  "HBase cluster" should {
    "have vis enabled" >> {
      adminUser.connection.getAdmin.getSecurityCapabilities.asScala must contain(SecurityCapability.CELL_VISIBILITY)
    }
  }

  "HBaseDataStore" should {

    def idQuery(conn: Connection, tableName: String, typeName: String): Seq[String] = {
      val params = Map(
        ConnectionParam.getName -> conn,
        HBaseCatalogParam.getName -> tableName)
      val ds = DataStoreFinder.getDataStore(params.asJava).asInstanceOf[HBaseDataStore]
      idQueryWithDS(ds, typeName)
    }

    def idQueryWithDS(ds: HBaseDataStore, typeName: String): Seq[String] = {
      val query = new Query(typeName, Filter.INCLUDE)

      val fr = ds.getFeatureReader(query, Transaction.AUTO_COMMIT)
      val features = CloseableIterator(fr).toList

      features.map(_.getID)
    }

    "properly filter vis" >> {
      val typeName = "vistest1"
      val tableName = "vistest1"
      val params = Map(
        ConnectionParam.getName -> adminUser.connection,
        HBaseCatalogParam.getName -> tableName)
      val writeDS = DataStoreFinder.getDataStore(params.asJava).asInstanceOf[HBaseDataStore]

      writeDS.getSchema(typeName) must beNull
      writeDS.createSchema(SimpleFeatureTypes.createType(typeName, "name:String:index=true,dtg:Date,*geom:Point:srid=4326"))

      val sft = writeDS.getSchema(typeName)
      sft must not(beNull)
      val fs = writeDS.getFeatureSource(typeName).asInstanceOf[SimpleFeatureStore]

      case class Data(id: String, dtg: String, wkt: String, vis: String)
      val data = Seq(
        Data("1",     "2014-01-01T00:00:00.000Z", "POINT(1 1)", "vis1"),
        Data("2",     "2014-01-01T00:00:00.000Z", "POINT(1 1)", "vis2"),
        Data("1-2",   "2014-01-01T00:00:00.000Z", "POINT(1 1)", "vis1|vis2"),
        Data("1-2-3", "2014-01-01T00:00:00.000Z", "POINT(1 1)", "vis1|vis2|vis3"),
        Data("super", "2014-01-01T00:00:00.000Z", "POINT(1 1)", "(vis1|vis2|vis3)&super")
      )

      val toAdd = data.map{ d =>
        import org.locationtech.geomesa.security._
        val sf = new ScalaSimpleFeature(sft, d.id)
        sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
        sf.setAttribute(0, d.id)
        sf.setAttribute(1, d.dtg)
        sf.setAttribute(2, d.wkt)
        SecurityUtils.setFeatureVisibility(sf, d.vis)
        sf: SimpleFeature
      }

      fs.addFeatures(new ListFeatureCollection(sft, toAdd.asJava))
      val expect = Seq(
        (user1.connection, Seq("1", "1-2", "1-2-3")),
        (user2.connection, Seq("2", "1-2", "1-2-3")),
        (privUser.connection,  Seq("1-2-3", "super"))
      )

      forall(expect) { vals =>
        idQuery(vals._1, tableName, typeName) must containTheSameElementsAs(vals._2)
      }
    }

    "work with an auth provider argument and dynamic visibilities" >> {
      var auths = List.empty[String]
      val authsProvider = new AuthorizationsProvider {
        override def getAuthorizations: java.util.List[String] = auths.asJava
        override def configure(params: java.util.Map[String, _]): Unit = {}
      }

      val typeName = "vistest1"
      val tableName = "vistest1"
      val params = Map(
        ConnectionParam.getName -> dynUser.connection,
        HBaseCatalogParam.getName -> tableName,
        org.locationtech.geomesa.security.AuthProviderParam.getName -> authsProvider,
        EnableSecurityParam.getName -> "true")
      val ds = DataStoreFinder.getDataStore(params.asJava).asInstanceOf[HBaseDataStore]

      // User may have everything but that doesn't matter
      setAuths("dynUser", Array[String]("admin", "vis1", "vis2", "vis3", "super"))
      getAuths("dynUser") must containTheSameElementsAs(Seq("admin", "vis1", "vis2", "vis3", "super"))

      idQueryWithDS(ds, typeName) must beEmpty

      auths = List("vis1")
      idQueryWithDS(ds, typeName) must containTheSameElementsAs(Seq("1", "1-2", "1-2-3"))

      auths = List("vis2")
      idQueryWithDS(ds, typeName) must containTheSameElementsAs(Seq("2", "1-2", "1-2-3"))

      auths = List("vis3")
      idQueryWithDS(ds, typeName) must containTheSameElementsAs(Seq("1-2-3"))

      auths = List("super")
      idQueryWithDS(ds, typeName) must beEmpty

      auths = List("vis3", "super")
      idQueryWithDS(ds, typeName) must containTheSameElementsAs(Seq("1-2-3", "super"))

      // Reset dynUser to have just vis1 set on the server side
      clearAllAuths("dynUser")
      setAuths("dynUser", Array[String]("vis1"))
      getAuths("dynUser") must containTheSameElementsAs(Seq("vis1"))

      auths = List("vis1")
      idQueryWithDS(ds, typeName) must containTheSameElementsAs(Seq("1", "1-2", "1-2-3"))

      auths = List("vis2")
      idQueryWithDS(ds, typeName) must beEmpty

      auths = List("vis3")
      idQueryWithDS(ds, typeName) must beEmpty

      auths = List("super")
      idQueryWithDS(ds, typeName) must beEmpty

      auths = List("vis3", "super")
      idQueryWithDS(ds, typeName) must beEmpty
    }

    "work with points" in {
      val typeName = "vis_testpoints"

      val params = Map(
        ConnectionParam.getName -> user1.connection,
        HBaseCatalogParam.getName -> getClass.getSimpleName)
      val ds = DataStoreFinder.getDataStore(params.asJava).asInstanceOf[HBaseDataStore]

      ds.getSchema(typeName) must beNull

      ds.createSchema(SimpleFeatureTypes.createType(typeName, "name:String:index=true,dtg:Date,*geom:Point:srid=4326"))

      val sft = ds.getSchema(typeName)

      sft must not(beNull)

      val fs = ds.getFeatureSource(typeName).asInstanceOf[SimpleFeatureStore]

      val toAdd = (0 until 10).map { i =>
        import org.locationtech.geomesa.security._
        val sf = new ScalaSimpleFeature(sft, i.toString)
        sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
        sf.setAttribute(0, s"name$i")
        sf.setAttribute(1, f"2014-01-${i + 1}%02dT00:00:01.000Z")
        sf.setAttribute(2, s"POINT(4$i 5$i)")
        SecurityUtils.setFeatureVisibility(sf, if (i < 5) { "admin|vis1|super" } else { "(admin|vis1)&super" })
        sf: SimpleFeature
      }

      val ids = fs.addFeatures(new ListFeatureCollection(sft, toAdd.asJava))
      ids.asScala.map(_.getID) must containTheSameElementsAs((0 until 10).map(_.toString))

      val adminParams = Map(
        ConnectionParam.getName -> adminUser.connection,
        HBaseCatalogParam.getName -> getClass.getSimpleName)

      def getDensity(typeName: String, query: String, fs: SimpleFeatureStore): Double = {
        val filter = ECQL.toFilter(query)
        val envelope = FilterHelper.extractGeometries(filter, "geom").values.headOption match {
          case None => ReferencedEnvelope.envelope(new Envelope(-180, 180, -90, 90), DefaultGeographicCRS.WGS84)
          case Some(g) => ReferencedEnvelope.envelope(g.getEnvelopeInternal, DefaultGeographicCRS.WGS84)
        }
        val q = new Query(typeName, filter)
        q.getHints.put(QueryHints.DENSITY_BBOX, envelope)
        q.getHints.put(QueryHints.DENSITY_WIDTH, 500)
        q.getHints.put(QueryHints.DENSITY_HEIGHT, 500)
        val decode = DensityScan.decodeResult(envelope, 500, 500)
        CloseableIterator(fs.getFeatures(q).features).flatMap(decode).toList.map(_._3).sum
      }

      def testQuery(ds: HBaseDataStore, typeName: String, filter: String, transforms: Array[String], results: Seq[SimpleFeature]) = {
        val query = new Query(typeName, ECQL.toFilter(filter), transforms: _*)
        val fr = ds.getFeatureReader(query, Transaction.AUTO_COMMIT)
        val features = CloseableIterator(fr).toList
        val attributes = Option(transforms).getOrElse(ds.getSchema(typeName).getAttributeDescriptors.asScala.map(_.getLocalName).toArray)
        features.map(_.getID) must containTheSameElementsAs(results.map(_.getID))
        forall(features) { feature =>
          feature.getAttributes must haveLength(attributes.length)
          forall(attributes.zipWithIndex) { case (attribute, i) =>
            feature.getAttribute(attribute) mustEqual feature.getAttribute(i)
            feature.getAttribute(attribute) mustEqual results.find(_.getID == feature.getID).get.getAttribute(attribute)
          }
        }

        val densitySize = getDensity(typeName, filter, ds.getFeatureSource(typeName))
        densitySize mustEqual results.length
        CloseableIterator(ds.getFeatureSource(typeName).getFeatures(query).features()).size mustEqual results.length
      }

      foreach(Seq(true, false)) { loose =>
        val ds = DataStoreFinder.getDataStore((params ++ Map(LooseBBoxParam.getName -> loose)).asJava).asInstanceOf[HBaseDataStore]
        foreach(Seq(null, Array("geom"), Array("geom", "dtg"), Array("geom", "name"))) { transforms =>
          testQuery(ds, typeName, "INCLUDE", transforms, toAdd.take(5))
          testQuery(ds, typeName, "IN('0', '2')", transforms, Seq(toAdd(0), toAdd(2)))
          testQuery(ds, typeName, "bbox(geom,38,48,52,62) and dtg DURING 2014-01-01T00:00:00.000Z/2014-01-08T12:00:00.000Z", transforms, toAdd.take(5))
          testQuery(ds, typeName, "bbox(geom,42,48,52,62)", transforms, Seq(toAdd(2), toAdd(3), toAdd(4)))
          testQuery(ds, typeName, "name < 'name5'", transforms, toAdd.take(5))
          testQuery(ds, typeName, "name = 'name5'", transforms, Seq.empty)
        }
      }

      foreach(Seq(true, false)) { loose =>
        val ds = DataStoreFinder.getDataStore((adminParams ++ Map(LooseBBoxParam.getName -> loose)).asJava).asInstanceOf[HBaseDataStore]
        foreach(Seq(null, Array("geom"), Array("geom", "dtg"), Array("geom", "name"))) { transforms =>
          testQuery(ds, typeName, "INCLUDE", transforms, toAdd)
          testQuery(ds, typeName, "IN('0', '2')", transforms, Seq(toAdd(0), toAdd(2)))
          testQuery(ds, typeName, "bbox(geom,38,48,52,62) and dtg DURING 2014-01-01T00:00:00.000Z/2014-01-08T12:00:00.000Z", transforms, toAdd.take(8))
          testQuery(ds, typeName, "bbox(geom,42,48,52,62)", transforms, toAdd.drop(2))
          testQuery(ds, typeName, "name < 'name5'", transforms, toAdd.take(5))
          testQuery(ds, typeName, "name = 'name5'", transforms, Seq(toAdd(5)))
        }
      }
    }
  }
}
