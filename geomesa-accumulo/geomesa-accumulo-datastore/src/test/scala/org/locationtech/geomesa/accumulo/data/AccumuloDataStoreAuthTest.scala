/*
 * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.accumulo.data

import java.text.SimpleDateFormat
import java.util.Date

import com.vividsolutions.jts.geom.Coordinate
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.client.mock.MockInstance
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.data.{Mutation, Range}
import org.apache.accumulo.core.iterators.user.VersioningIterator
import org.apache.accumulo.core.security.Authorizations
import org.apache.commons.codec.binary.Hex
import org.apache.hadoop.io.Text
import org.geotools.data._
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.data.simple.SimpleFeatureStore
import org.geotools.factory.Hints
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.feature.{DefaultFeatureCollection, NameImpl}
import org.geotools.filter.text.cql2.CQL
import org.geotools.filter.text.ecql.ECQL
import org.geotools.geometry.jts.JTSFactoryFinder
import org.geotools.referencing.CRS
import org.joda.time.DateTime
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.{TestWithMultipleSfts, TestWithDataStore}
import org.locationtech.geomesa.accumulo.data.tables._
import org.locationtech.geomesa.accumulo.index.QueryHints._
import org.locationtech.geomesa.accumulo.index.Strategy.StrategyType
import org.locationtech.geomesa.accumulo.index._
import org.locationtech.geomesa.accumulo.iterators.{BinAggregatingIterator, IndexIterator, TestData}
import org.locationtech.geomesa.accumulo.util.{CloseableIterator, GeoMesaBatchWriterConfig, SelfClosingIterator}
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.features.avro.AvroSimpleFeatureFactory
import org.locationtech.geomesa.filter.function.Convert2ViewerFunction
import org.locationtech.geomesa.security.{AuthorizationsProvider, DefaultAuthorizationsProvider, FilteringAuthorizationsProvider}
import org.locationtech.geomesa.utils.geotools.Conversions._
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes._
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.filter.Filter
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.specs2.time.Duration

import scala.collection.JavaConversions._
import scala.util.Random

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
        ds.authorizationsProvider must beAnInstanceOf[FilteringAuthorizationsProvider]
        ds.authorizationsProvider.asInstanceOf[FilteringAuthorizationsProvider].wrappedProvider must beAnInstanceOf[DefaultAuthorizationsProvider]
        ds.authorizationsProvider.asInstanceOf[AuthorizationsProvider].getAuthorizations mustEqual new Authorizations("user")
      }

      "by comma-delimited static auths" >> {
        // create the data store
        val ds = DataStoreFinder.getDataStore(Map(
          "connector" -> connector,
          "tableName" -> sftName,
          "auths"     -> "user,admin,test")).asInstanceOf[AccumuloDataStore]
        ds must not beNull;
        ds.authorizationsProvider must beAnInstanceOf[FilteringAuthorizationsProvider]
        ds.authorizationsProvider.asInstanceOf[FilteringAuthorizationsProvider].wrappedProvider must beAnInstanceOf[DefaultAuthorizationsProvider]
        ds.authorizationsProvider.asInstanceOf[AuthorizationsProvider].getAuthorizations mustEqual new Authorizations("user", "admin", "test")
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

    "restrict users with insufficient auths from writing data" >> {
      // create the data store
      val ds = DataStoreFinder.getDataStore(Map(
        "connector"    -> connector,
        "tableName"    -> sftName,
        "auths"        -> "user",
        "visibilities" -> "user&admin")).asInstanceOf[AccumuloDataStore]
      ds must not beNull

      val sft = SimpleFeatureTypes.createType("cantwrite", spec)
      ds.createSchema(sft)

      // write some data
      val fs = ds.getFeatureSource("cantwrite").asInstanceOf[AccumuloFeatureStore]
      val feat = new ScalaSimpleFeature("1", sft)
      feat.setAttribute("geom", "POINT(45 55)")
      fs.addFeatures(new ListFeatureCollection(sft, List(feat))) must throwA[RuntimeException]
    }
  }
}
