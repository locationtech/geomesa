/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.data

import java.io.Serializable
import java.util

import org.apache.accumulo.core.security.Authorizations
import org.geotools.data._
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.data.simple.SimpleFeatureStore
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithDataStore
import org.locationtech.geomesa.accumulo.security.AccumuloAuthsProvider
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.security.{AuthorizationsProvider, DefaultAuthorizationsProvider, FilteringAuthorizationsProvider, SecurityUtils}
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class AccumuloDataStoreAuthTest extends Specification with TestWithDataStore {

  sequential

  val spec = "name:String:index=true,dtg:Date,*geom:Point:srid=4326"

  addFeatures((0 until 2).map { i =>
    val sf = new ScalaSimpleFeature(i.toString, sft)
    sf.setAttribute(0, i.toString)
    sf.setAttribute(1, s"2016-01-01T01:0$i:00.000Z")
    sf.setAttribute(2, s"POINT (45 5$i)")
    if (i == 0) {
      SecurityUtils.setFeatureVisibility(sf, "user")
    } else {
      SecurityUtils.setFeatureVisibility(sf, "admin")
    }
    sf
  })

  val threadedAuths = new ThreadLocal[Authorizations]

  val authProvider = new AccumuloAuthsProvider(new AuthorizationsProvider {
    override def getAuthorizations: java.util.List[String] = threadedAuths.get.getAuthorizations.map(new String(_))
    override def configure(params: util.Map[String, Serializable]): Unit = {}
  })

  "AccumuloDataStore" should {
    "provide ability to configure authorizations" >> {
      "by static auths" >> {
        // create the data store
        val ds = DataStoreFinder.getDataStore(Map(
          "connector" -> connector,
          "tableName" -> sftName,
          "auths"     -> "user")).asInstanceOf[AccumuloDataStore]
        ds must not beNull;
        ds.config.authProvider.authProvider must beAnInstanceOf[FilteringAuthorizationsProvider]
        ds.config.authProvider.authProvider.asInstanceOf[FilteringAuthorizationsProvider].wrappedProvider must beAnInstanceOf[DefaultAuthorizationsProvider]
        ds.config.authProvider.getAuthorizations mustEqual new Authorizations("user")
      }

      "by comma-delimited static auths" >> {
        // create the data store
        val ds = DataStoreFinder.getDataStore(Map(
          "connector" -> connector,
          "tableName" -> sftName,
          "auths"     -> "user,admin,test")).asInstanceOf[AccumuloDataStore]
        ds must not beNull;
        ds.config.authProvider.authProvider must beAnInstanceOf[FilteringAuthorizationsProvider]
        ds.config.authProvider.authProvider.asInstanceOf[FilteringAuthorizationsProvider].wrappedProvider must beAnInstanceOf[DefaultAuthorizationsProvider]
        ds.config.authProvider.getAuthorizations mustEqual new Authorizations("user", "admin", "test")
      }

      "fail when auth provider system property does not match an actual class" >> {
        System.setProperty(AuthorizationsProvider.AUTH_PROVIDER_SYS_PROPERTY, "my.fake.Clas")
        try {
          // create the data store
          DataStoreFinder.getDataStore(Map(
            "connector" -> connector,
            "tableName" -> sftName,
            "auths"     -> "user,admin,test")) should throwAn[IllegalArgumentException]
        } finally {
          System.clearProperty(AuthorizationsProvider.AUTH_PROVIDER_SYS_PROPERTY)
        }
      }

      "fail when authorizations are explicitly provided, but the flag to force using authorizations is not set" >> {
        // create the data store
        DataStoreFinder.getDataStore(Map(
          "connector" -> connector,
          "tableName" -> sftName,
          "auths"     -> "user,admin,test",
          "forceEmptyAuths" -> java.lang.Boolean.TRUE)) should throwAn[IllegalArgumentException]
      }

      "replace empty authorizations with the Accumulo user's full authorizations (without the override)" >> {
        val ds = DataStoreFinder.getDataStore(Map(
          "connector" -> connector,
          "tableName" -> sftName,
          "auths"     -> "")).asInstanceOf[AccumuloDataStore]
        ds must not beNull;
        ds.config.authProvider.authProvider must beAnInstanceOf[FilteringAuthorizationsProvider]
        ds.config.authProvider.authProvider.asInstanceOf[FilteringAuthorizationsProvider].wrappedProvider must beAnInstanceOf[DefaultAuthorizationsProvider]
        ds.config.authProvider.getAuthorizations mustEqual MockUserAuthorizations
      }

      "use empty authorizations (with the override)" >> {
        val ds = DataStoreFinder.getDataStore(Map(
          "connector" -> connector,
          "tableName" -> sftName,
          "auths"     -> "",
          "forceEmptyAuths" -> java.lang.Boolean.TRUE)).asInstanceOf[AccumuloDataStore]
        ds must not beNull;
        ds.config.authProvider.authProvider must beAnInstanceOf[FilteringAuthorizationsProvider]
        ds.config.authProvider.authProvider.asInstanceOf[FilteringAuthorizationsProvider].wrappedProvider must beAnInstanceOf[DefaultAuthorizationsProvider]
        ds.config.authProvider.getAuthorizations mustEqual EmptyUserAuthorizations
      }

      "query with a threaded auth provider against various indices" >> {
        // create the data store
        val ds = DataStoreFinder.getDataStore(Map(
          "connector"    -> connector,
          "tableName"    -> sftName,
          "auths"        -> "user,admin",
          "authProvider" -> authProvider.authProvider)).asInstanceOf[AccumuloDataStore]
        ds should not be null

        val user  = Seq("IN('0')", "name = '0'", "bbox(geom, 44, 49.1, 46, 50.1)", "bbox(geom, 44, 49.1, 46, 50.1) AND dtg DURING 2016-01-01T00:59:30.000Z/2016-01-01T01:00:30.000Z")
        val admin = Seq("IN('1')", "name = '1'", "bbox(geom, 44, 50.1, 46, 51.1)", "bbox(geom, 44, 50.1, 46, 51.1) AND dtg DURING 2016-01-01T01:00:30.000Z/2016-01-01T01:01:30.000Z")
        val both  = Seq("INCLUDE", "IN('0', '1')", "name < '2'", "bbox(geom, 44, 49.1, 46, 51.1)", "bbox(geom, 44, 49.1, 46, 51.1) AND dtg DURING 2016-01-01T00:59:30.000Z/2016-01-01T01:01:30.000Z")

        forall(user) { filter =>
          val q = new Query(sftName, ECQL.toFilter(filter))
          threadedAuths.set(new Authorizations("user"))
          try {
            val results = SelfClosingIterator(ds.getFeatureReader(q, Transaction.AUTO_COMMIT)).toList
            results must haveLength(1)
            results.head.getID mustEqual "0"
          } finally {
            threadedAuths.remove()
          }
          threadedAuths.set(new Authorizations("admin"))
          try {
            val results = SelfClosingIterator(ds.getFeatureReader(q, Transaction.AUTO_COMMIT)).toList
            results must beEmpty
          } finally {
            threadedAuths.remove()
          }
        }

        forall(admin) { filter =>
          val q = new Query(sftName, ECQL.toFilter(filter))
          threadedAuths.set(new Authorizations("admin"))
          try {
            val results = SelfClosingIterator(ds.getFeatureReader(q, Transaction.AUTO_COMMIT)).toList
            results must haveLength(1)
            results.head.getID mustEqual "1"
          } finally {
            threadedAuths.remove()
          }
          threadedAuths.set(new Authorizations("user"))
          try {
            val results = SelfClosingIterator(ds.getFeatureReader(q, Transaction.AUTO_COMMIT)).toList
            results must beEmpty
          } finally {
            threadedAuths.remove()
          }
        }

        forall(both) { filter =>
          val q = new Query(sftName, ECQL.toFilter(filter))
          threadedAuths.set(new Authorizations("user"))
          try {
            val results = SelfClosingIterator(ds.getFeatureReader(q, Transaction.AUTO_COMMIT)).toList
            results must haveLength(1)
            results.head.getID mustEqual "0"
          } finally {
            threadedAuths.remove()
          }
          threadedAuths.set(new Authorizations("admin"))
          try {
            val results = SelfClosingIterator(ds.getFeatureReader(q, Transaction.AUTO_COMMIT)).toList
            results must haveLength(1)
            results.head.getID mustEqual "1"
          } finally {
            threadedAuths.remove()
          }
          threadedAuths.set(new Authorizations("user", "admin"))
          try {
            val results = SelfClosingIterator(ds.getFeatureReader(q, Transaction.AUTO_COMMIT)).toList
            results must haveLength(2)
            results.map(_.getID).sorted mustEqual Seq("0", "1")
          } finally {
            threadedAuths.remove()
          }
        }
      }
    }

    "allow users with sufficient auths to write data" >> {
      // create the data store
      val ds = DataStoreFinder.getDataStore(Map(
        "connector"    -> connector,
        "tableName"    -> sftName,
        "auths"        -> "user,admin",
        "visibilities" -> "user&admin")).asInstanceOf[AccumuloDataStore]
      ds should not be null

      val sft = SimpleFeatureTypes.createType("canwrite", spec)
      ds.createSchema(sft)

      // write some data
      val fs = ds.getFeatureSource("canwrite").asInstanceOf[SimpleFeatureStore]
      val feat = new ScalaSimpleFeature("1", sft)
      feat.setAttribute("geom", "POINT(45 55)")
      val written = fs.addFeatures(new ListFeatureCollection(sft, List(feat)))

      written must not(beNull)
      written.length mustEqual 1
    }
  }
}
