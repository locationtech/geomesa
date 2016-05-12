/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.data

import org.apache.accumulo.core.security.Authorizations
import org.geotools.data._
import org.geotools.data.collection.ListFeatureCollection
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithDataStore
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.security.{AuthorizationsProvider, DefaultAuthorizationsProvider, FilteringAuthorizationsProvider}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class AccumuloDataStoreAuthTest extends Specification with TestWithDataStore {

  sequential

  val spec = "name:String,dtg:Date,*geom:Point:srid=4326"

  "AccumuloDataStore" should {
    "provide ability to configure authorizations" >> {
      "by static auths" >> {
        // create the data store
        val ds = DataStoreFinder.getDataStore(Map(
          "connector" -> connector,
          "tableName" -> sftName,
          "auths"     -> "user")).asInstanceOf[AccumuloDataStore]
        ds must not beNull;
        ds.authProvider must beAnInstanceOf[FilteringAuthorizationsProvider]
        ds.authProvider.asInstanceOf[FilteringAuthorizationsProvider].wrappedProvider must beAnInstanceOf[DefaultAuthorizationsProvider]
        ds.authProvider.asInstanceOf[AuthorizationsProvider].getAuthorizations mustEqual new Authorizations("user")
      }

      "by comma-delimited static auths" >> {
        // create the data store
        val ds = DataStoreFinder.getDataStore(Map(
          "connector" -> connector,
          "tableName" -> sftName,
          "auths"     -> "user,admin,test")).asInstanceOf[AccumuloDataStore]
        ds must not beNull;
        ds.authProvider must beAnInstanceOf[FilteringAuthorizationsProvider]
        ds.authProvider.asInstanceOf[FilteringAuthorizationsProvider].wrappedProvider must beAnInstanceOf[DefaultAuthorizationsProvider]
        ds.authProvider.asInstanceOf[AuthorizationsProvider].getAuthorizations mustEqual new Authorizations("user", "admin", "test")
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
        ds.authProvider must beAnInstanceOf[FilteringAuthorizationsProvider]
        ds.authProvider.asInstanceOf[FilteringAuthorizationsProvider].wrappedProvider must beAnInstanceOf[DefaultAuthorizationsProvider]
        ds.authProvider.asInstanceOf[AuthorizationsProvider].getAuthorizations mustEqual MockUserAuthorizations
      }

      "use empty authorizations (with the override)" >> {
        val ds = DataStoreFinder.getDataStore(Map(
          "connector" -> connector,
          "tableName" -> sftName,
          "auths"     -> "",
          "forceEmptyAuths" -> java.lang.Boolean.TRUE)).asInstanceOf[AccumuloDataStore]
        ds must not beNull;
        ds.authProvider must beAnInstanceOf[FilteringAuthorizationsProvider]
        ds.authProvider.asInstanceOf[FilteringAuthorizationsProvider].wrappedProvider must beAnInstanceOf[DefaultAuthorizationsProvider]
        ds.authProvider.asInstanceOf[AuthorizationsProvider].getAuthorizations mustEqual EmptyUserAuthorizations
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
      val fs = ds.getFeatureSource("canwrite").asInstanceOf[AccumuloFeatureStore]
      val feat = new ScalaSimpleFeature("1", sft)
      feat.setAttribute("geom", "POINT(45 55)")
      val written = fs.addFeatures(new ListFeatureCollection(sft, List(feat)))

      written must not beNull;
      written.length mustEqual 1
    }
  }
}
