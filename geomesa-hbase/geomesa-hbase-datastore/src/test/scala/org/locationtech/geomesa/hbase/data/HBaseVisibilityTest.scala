/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.data

import java.io.Serializable
import java.security.PrivilegedExceptionAction
import java.util

import com.typesafe.scalalogging.LazyLogging
import org.locationtech.jts.geom.Envelope
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.security.SecurityCapability
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}
import org.apache.hadoop.hbase.security.User
import org.apache.hadoop.hbase.security.visibility.VisibilityClient
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.data.simple.SimpleFeatureStore
import org.geotools.data.{DataStoreFinder, Query, Transaction}
import org.geotools.factory.Hints
import org.geotools.filter.text.ecql.ECQL
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.filter.FilterHelper
import org.locationtech.geomesa.hbase.data.HBaseDataStoreParams._
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.index.iterators.DensityScan
import org.locationtech.geomesa.security.AuthorizationsProvider
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.Filter

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

class HBaseVisibilityTest extends HBaseTest with LazyLogging {

  sequential

  var adminUser: User = _
  var user1:     User = _
  var user2:     User = _
  var privUser:  User = _
  var dynUser:   User = _

  var adminConn: Connection = _
  var user1Conn: Connection = _
  var user2Conn: Connection = _
  var privConn:  Connection = _
  var dynConn:   Connection = _

  def setAuths(user: String, auths: Array[String]): Unit = {
    adminUser.runAs(new PrivilegedExceptionAction[Unit]() {
      override def run(): Unit = {
        VisibilityClient.setAuths(adminConn, auths, user)
      }
    })
  }

  def getAuths(user: String): Seq[String] = {
    adminUser.runAs(new PrivilegedExceptionAction[Seq[String]]() {
      override def run(): Seq[String] = {
        VisibilityClient.getAuths(adminConn, user).getAuthList.map(_.toStringUtf8)
      }
    })
  }

  def clearAllAuths(user: String): Unit = {
    adminUser.runAs(new PrivilegedExceptionAction[Unit]() {
      override def run(): Unit = {
        VisibilityClient.clearAuths(adminConn, getAuths(user).toArray, user)
      }
    })
  }

  step {
    logger.info("Starting Visibility Test")
    adminUser = User.createUserForTesting(cluster.getConfiguration, "admin",    Array[String]("supergroup"))
    user1     = User.createUserForTesting(cluster.getConfiguration, "user1",    Array.empty[String])
    user2     = User.createUserForTesting(cluster.getConfiguration, "user2",    Array.empty[String])
    privUser  = User.createUserForTesting(cluster.getConfiguration, "privUser", Array.empty[String])
    dynUser   = User.createUserForTesting(cluster.getConfiguration, "dynUser",  Array.empty[String])

    adminUser.runAs(new PrivilegedExceptionAction[Unit]() {
      override def run(): Unit = {
        cluster.waitTableAvailable(TableName.valueOf("hbase:labels"), 50000)
        val labels = Array[String]("extra", "admin", "vis1", "vis2", "vis3", "super")
        adminConn = ConnectionFactory.createConnection(cluster.getConfiguration)
        VisibilityClient.addLabels(adminConn, labels)
        cluster.waitLabelAvailable(10000, labels: _*)
        setAuths("user1", Array[String]("vis1"))
        setAuths("user2", Array[String]("vis2"))
        setAuths("privUser", Array[String]("super", "vis3"))
      }
    })

    user1.runAs(new PrivilegedExceptionAction[Unit]() {
      override def run(): Unit = {
        user1Conn = ConnectionFactory.createConnection(cluster.getConfiguration)
      }
    })

    user2.runAs(new PrivilegedExceptionAction[Unit]() {
      override def run(): Unit = {
        user2Conn = ConnectionFactory.createConnection(cluster.getConfiguration)
      }
    })

    privUser.runAs(new PrivilegedExceptionAction[Unit]() {
      override def run(): Unit = {
        privConn = ConnectionFactory.createConnection(cluster.getConfiguration)
      }
    })

    dynUser.runAs(new PrivilegedExceptionAction[Unit]() {
      override def run(): Unit = {
        dynConn = ConnectionFactory.createConnection(cluster.getConfiguration)
      }
    })

    logger.info("Successfully created authorizations")
  }

  "HBase cluster" should {
    "have vis enabled" >> {
      cluster.getHBaseAdmin.getSecurityCapabilities.asScala must contain(SecurityCapability.CELL_VISIBILITY)
    }
  }

  "HBaseDataStore" should {

    def idQuery(conn: Connection, tableName: String, typeName: String): Seq[String] = {
      val params = Map(
        ConnectionParam.getName -> conn,
        HBaseCatalogParam.getName -> tableName)
      val ds = DataStoreFinder.getDataStore(params).asInstanceOf[HBaseDataStore]
      idQueryWithDS(ds, typeName)
    }

    def idQueryWithDS(ds: HBaseDataStore, typeName: String): Seq[String] = {
      val query = new Query(typeName, Filter.INCLUDE)

      val fr = ds.getFeatureReader(query, Transaction.AUTO_COMMIT)
      val features = SelfClosingIterator(fr).toList

      features.map(_.getID)
    }

    "properly filter vis" >> {
      val typeName = "vistest1"
      val tableName = "vistest1"
      val params = Map(
        ConnectionParam.getName -> adminConn,
        HBaseCatalogParam.getName -> tableName)
      val writeDS = DataStoreFinder.getDataStore(params).asInstanceOf[HBaseDataStore]

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
        sf.visibility = d.vis
        sf
      }

      fs.addFeatures(new ListFeatureCollection(sft, toAdd))
      val expect = Seq(
        (user1Conn, Seq("1", "1-2", "1-2-3")),
        (user2Conn, Seq("2", "1-2", "1-2-3")),
        (privConn,  Seq("1-2-3", "super"))
      )

      forall(expect) { vals =>
        idQuery(vals._1, tableName, typeName) must containTheSameElementsAs(vals._2)
      }
    }

    "work with an auth provider argument and dynamic visibilities" >> {
      var auths = List.empty[String]
      val authsProvider = new AuthorizationsProvider {
        override def getAuthorizations: util.List[String] = auths
        override def configure(params: util.Map[String, Serializable]): Unit = {}
      }

      val typeName = "vistest1"
      val tableName = "vistest1"
      val params = Map(
        ConnectionParam.getName -> dynConn,
        HBaseCatalogParam.getName -> tableName,
        org.locationtech.geomesa.security.AuthProviderParam.getName -> authsProvider,
        EnableSecurityParam.getName -> "true")
      val ds = DataStoreFinder.getDataStore(params).asInstanceOf[HBaseDataStore]

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
        ConnectionParam.getName -> user1Conn,
        HBaseCatalogParam.getName -> catalogTableName)
      val ds = DataStoreFinder.getDataStore(params).asInstanceOf[HBaseDataStore]

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
        if (i < 5) sf.visibility = "admin|vis1|super" else sf.visibility = "(admin|vis1)&super"
        sf
      }

      val ids = fs.addFeatures(new ListFeatureCollection(sft, toAdd))
      ids.asScala.map(_.getID) must containTheSameElementsAs((0 until 10).map(_.toString))

      val adminParams = Map(
        ConnectionParam.getName -> adminConn,
        HBaseCatalogParam.getName -> catalogTableName)

      def getDensity(typeName: String, query: String, fs: SimpleFeatureStore): Double = {
        val filter = ECQL.toFilter(query)
        val envelope = FilterHelper.extractGeometries(filter, "geom").values.headOption match {
          case None => ReferencedEnvelope.create(new Envelope(-180, 180, -90, 90), DefaultGeographicCRS.WGS84)
          case Some(g) => ReferencedEnvelope.create(g.getEnvelopeInternal, DefaultGeographicCRS.WGS84)
        }
        val q = new Query(typeName, filter)
        q.getHints.put(QueryHints.DENSITY_BBOX, envelope)
        q.getHints.put(QueryHints.DENSITY_WIDTH, 500)
        q.getHints.put(QueryHints.DENSITY_HEIGHT, 500)
        val decode = DensityScan.decodeResult(envelope, 500, 500)
        SelfClosingIterator(fs.getFeatures(q).features).flatMap(decode).toList.map(_._3).sum
      }

      def testQuery(ds: HBaseDataStore, typeName: String, filter: String, transforms: Array[String], results: Seq[SimpleFeature]) = {
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

        val densitySize = getDensity(typeName, filter, ds.getFeatureSource(typeName))
        densitySize mustEqual results.length
        SelfClosingIterator(ds.getFeatureSource(typeName).getFeatures(query).features()).size mustEqual results.length
      }

      forall(Seq(true, false)) { loose =>
        val ds = DataStoreFinder.getDataStore(params ++ Map(LooseBBoxParam.getName -> loose)).asInstanceOf[HBaseDataStore]
        forall(Seq(null, Array("geom"), Array("geom", "dtg"), Array("geom", "name"))) { transforms =>
          testQuery(ds, typeName, "INCLUDE", transforms, toAdd.take(5))
          testQuery(ds, typeName, "IN('0', '2')", transforms, Seq(toAdd(0), toAdd(2)))
          testQuery(ds, typeName, "bbox(geom,38,48,52,62) and dtg DURING 2014-01-01T00:00:00.000Z/2014-01-08T12:00:00.000Z", transforms, toAdd.take(5))
          testQuery(ds, typeName, "bbox(geom,42,48,52,62)", transforms, Seq(toAdd(2), toAdd(3), toAdd(4)))
          testQuery(ds, typeName, "name < 'name5'", transforms, toAdd.take(5))
          testQuery(ds, typeName, "name = 'name5'", transforms, Seq.empty)
        }
      }

      forall(Seq(true, false)) { loose =>
        val ds = DataStoreFinder.getDataStore(adminParams ++ Map(LooseBBoxParam.getName -> loose)).asInstanceOf[HBaseDataStore]
        forall(Seq(null, Array("geom"), Array("geom", "dtg"), Array("geom", "name"))) { transforms =>
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
