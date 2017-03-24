/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.data

import org.apache.accumulo.core.client.mock.MockInstance
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.security.Authorizations
import org.geotools.data._
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.data.simple.SimpleFeatureStore
import org.geotools.factory.{CommonFactoryFinder, Hints}
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.filter.identity.FeatureIdImpl
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.avro.AvroSimpleFeatureFactory
import org.locationtech.geomesa.security.SecurityUtils
import org.locationtech.geomesa.utils.geotools.Conversions._
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.feature.simple.SimpleFeatureType
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class VisibilitiesTest extends Specification {

  sequential

  "handle per feature visibilities" should {
    val mockInstance = new MockInstance("perfeatureinstance")
    val conn = mockInstance.getConnector("myuser", new PasswordToken("mypassword".getBytes("UTF8")))
    conn.securityOperations().changeUserAuthorizations("myuser", new Authorizations("user", "admin"))
    conn.securityOperations().createLocalUser("nonpriv", new PasswordToken("nonpriv".getBytes("UTF8")))
    conn.securityOperations().changeUserAuthorizations("nonpriv", new Authorizations("user"))

    // create the data store
    val ds = DataStoreFinder.getDataStore(Map(
      "instanceId"        -> "perfeatureinstance",
      "zookeepers"        -> "zoo1:2181,zoo2:2181,zoo3:2181",
      "user"              -> "myuser",
      "password"          -> "mypassword",
      "tableName"         -> "testwrite",
      "useMock"           -> "true",
      "featureEncoding"   -> "avro")).asInstanceOf[AccumuloDataStore]

    val sftName = "perfeatureauthtest"
    val sft = SimpleFeatureTypes.createType(sftName, s"name:String,dtg:Date,*geom:Point:srid=4326")
    sft.setDtgField("dtg")
    ds.createSchema(sft)

    // write some data
    val fs = ds.getFeatureSource(sftName)

    val features = getFeatures(sft).toList
    val privFeatures = features.take(3)
    privFeatures.foreach { f => f.getUserData.put(SecurityUtils.FEATURE_VISIBILITY, "user&admin") }

    val nonPrivFeatures = features.drop(3)
    nonPrivFeatures.foreach { f => f.getUserData.put(SecurityUtils.FEATURE_VISIBILITY, "user") }

    fs.addFeatures(new ListFeatureCollection(sft, privFeatures ++ nonPrivFeatures))
    fs.flush()

    val ff = CommonFactoryFinder.getFilterFactory2
    import ff.{literal => lit, property => prop, _}

    val unprivDS = DataStoreFinder.getDataStore(Map(
      "instanceId"        -> "perfeatureinstance",
      "zookeepers"        -> "zoo1:2181,zoo2:2181,zoo3:2181",
      "user"              -> "nonpriv",
      "password"          -> "nonpriv",
      "tableName"         -> "testwrite",
      "useMock"           -> "true",
      "featureEncoding"   -> "avro")).asInstanceOf[AccumuloDataStore]

    "nonpriv should only be able to read a subset of features" in {

      "using ALL queries" in {
        val reader = unprivDS.getFeatureReader(new Query(sftName), Transaction.AUTO_COMMIT)
        val readFeatures = reader.toIterator.toList

        readFeatures.size must be equalTo 3
      }

      "using ST queries" in {
        val filter = bbox(prop("geom"), 44.0, 44.0, 46.0, 46.0, "EPSG:4326")
        val reader = unprivDS.getFeatureReader(new Query(sftName, filter), Transaction.AUTO_COMMIT)
        reader.toIterator.toList.size must be equalTo 3
      }

      "using attribute queries" in {
        val filter = or(
          ff.equals(prop("name"), lit("1")),
          ff.equals(prop("name"), lit("4")))

        val reader = unprivDS.getFeatureReader(new Query(sftName, filter), Transaction.AUTO_COMMIT)
        reader.toIterator.toList.size must be equalTo 1
      }

    }

    "priv should be able to read all 6 features" in {

      "using ALL queries" in {
        val reader = ds.getFeatureReader(new Query(sftName), Transaction.AUTO_COMMIT)
        val readFeatures = reader.toIterator.toList
        readFeatures.size must be equalTo 6
      }
      "using ST queries" in {
        val filter = bbox(prop("geom"), 44.0, 44.0, 46.0, 46.0, "EPSG:4326")
        val reader = ds.getFeatureReader(new Query(sftName, filter), Transaction.AUTO_COMMIT)
        reader.toIterator.toList.size must be equalTo 6
      }

      "using attribute queries" in {
        val filter = or(
          ff.equals(prop("name"), lit("1")),
          ff.equals(prop("name"), lit("4")))

        val reader = ds.getFeatureReader(new Query(sftName, filter), Transaction.AUTO_COMMIT)
        reader.toIterator.toList.size must be equalTo 2
      }
    }
  }

  "remove should continue to work as expected" in {

    val instanceId = "removeviz"
    val mockInstance = new MockInstance(instanceId)
    val conn = mockInstance.getConnector("myuser", new PasswordToken("mypassword".getBytes("UTF8")))
    conn.securityOperations().changeUserAuthorizations("myuser", new Authorizations("user", "admin"))
    conn.securityOperations().createLocalUser("nonpriv", new PasswordToken("nonpriv".getBytes("UTF8")))
    conn.securityOperations().changeUserAuthorizations("nonpriv", new Authorizations("user"))

    // create the data store
    val ds = DataStoreFinder.getDataStore(Map(
      "instanceId"        -> instanceId,
      "zookeepers"        -> "zoo1:2181,zoo2:2181,zoo3:2181",
      "user"              -> "myuser",
      "password"          -> "mypassword",
      "tableName"         -> "testwrite",
      "useMock"           -> "true",
      "featureEncoding"   -> "avro")).asInstanceOf[AccumuloDataStore]

    val sftName = "perfeatureauthtest"
    val sft = SimpleFeatureTypes.createType(sftName, s"name:String,dtg:Date,*geom:Point:srid=4326")
    sft.setDtgField("dtg")
    ds.createSchema(sft)

    // write some data
    val fs = ds.getFeatureSource(sftName)

    val features = getFeatures(sft).toList
    val privFeatures = features.take(3)
    privFeatures.foreach { f => f.getUserData.put(SecurityUtils.FEATURE_VISIBILITY, "user&admin") }

    val nonPrivFeatures = features.drop(3)
    nonPrivFeatures.foreach { f => f.getUserData.put(SecurityUtils.FEATURE_VISIBILITY, "user") }

    fs.addFeatures(new ListFeatureCollection(sft, privFeatures ++ nonPrivFeatures))
    fs.flush()

    val ff = CommonFactoryFinder.getFilterFactory2
    import ff.{literal => lit, property => prop, _}

    val unprivDS = DataStoreFinder.getDataStore(Map(
      "instanceId"        -> instanceId,
      "zookeepers"        -> "zoo1:2181,zoo2:2181,zoo3:2181",
      "user"              -> "nonpriv",
      "password"          -> "nonpriv",
      "tableName"         -> "testwrite",
      "useMock"           -> "true",
      "featureEncoding"   -> "avro")).asInstanceOf[AccumuloDataStore]

    "priv should be able to delete a feature" in {
      fs.removeFeatures(ff.id(new FeatureIdImpl("1")))
      fs.flush()

      "using ALL queries" in {
        fs.getFeatures(new Query(sftName)).features().toList.size must be equalTo 5
      }

      "using record id queries" in {
        fs.getFeatures(ff.id(ff.featureId("1"))).features().hasNext must beFalse
      }

      "using attribute queries" in {
        val filter = or(
          ff.equals(prop("name"), lit("1")),
          ff.equals(prop("name"), lit("4")))

        val reader = ds.getFeatureReader(new Query(sftName, filter), Transaction.AUTO_COMMIT)
        reader.toIterator.toList.size must be equalTo 1
      }
    }

    "nonpriv should not be able to delete a priv feature" in {
      val unprivFS = unprivDS.getFeatureSource(sftName).asInstanceOf[SimpleFeatureStore]
      unprivFS.removeFeatures(ff.id(new FeatureIdImpl("2")))
      unprivFS.flush()

      "priv should still see the feature that was attempted to be deleted" in {
        fs.getFeatures(ff.id(ff.featureId("2"))).features().hasNext must beTrue
      }
    }
  }


  val hints = new Hints(Hints.FEATURE_FACTORY, classOf[AvroSimpleFeatureFactory])
  val featureFactory = CommonFactoryFinder.getFeatureFactory(hints)

  def getFeatures(sft: SimpleFeatureType) = (0 until 6).map { i =>
    val builder = new SimpleFeatureBuilder(sft, featureFactory)
    builder.set("geom", WKTUtils.read("POINT(45.0 45.0)"))
    builder.set("dtg", "2012-01-02T05:06:07.000Z")
    builder.set("name",i.toString)
    val sf = builder.buildFeature(i.toString)
    sf.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE
    sf
  }

}
