/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
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
import org.geotools.factory.Hints
import org.geotools.feature.DefaultFeatureCollection
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.AccumuloFeatureIndexType
import org.locationtech.geomesa.accumulo.index.{AttributeIndex, RecordIndex, Z2Index, Z3Index}
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.security.SecurityUtils
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeature
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AccumuloDataStoreAttributeVisibilityTest extends Specification {

  import scala.collection.JavaConversions._

  sequential

  val sftName = getClass.getSimpleName
  val spec = "name:String:index=full,age:Int,dtg:Date,*geom:Point:srid=4326;geomesa.visibility.level='attribute'"

  val connector = {
    val mockInstance = new MockInstance("mycloud")
    val mockConnector = mockInstance.getConnector("user", new PasswordToken("password"))
    mockConnector.securityOperations().changeUserAuthorizations("user", new Authorizations("admin", "user"))
    mockConnector
  }

  val (ds, sft) = {
    val sft = SimpleFeatureTypes.createType(sftName, spec)
    val ds = DataStoreFinder.getDataStore(Map(
      "connector" -> connector,
      // note the table needs to be different to prevent testing errors
      "tableName" -> sftName)).asInstanceOf[AccumuloDataStore]
    ds.createSchema(sft)
    (ds, ds.getSchema(sftName)) // reload the sft from the ds to ensure all user data is set properly
  }

  val user = {
    val sf = new ScalaSimpleFeature("user", sft)
    sf.setAttribute(0, "name-user")
    sf.setAttribute(1, "10")
    sf.setAttribute(2, "2014-01-01T01:00:00.000Z")
    sf.setAttribute(3, "POINT (-120 45)")
    SecurityUtils.setFeatureVisibility(sf, "user,user,user,user")
    sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
    sf
  }
  val admin = {
    val sf = new ScalaSimpleFeature("admin", sft)
    sf.setAttribute(0, "name-admin")
    sf.setAttribute(1, "11")
    sf.setAttribute(2, "2014-01-02T01:00:00.000Z")
    sf.setAttribute(3, "POINT (-120 46)")
    SecurityUtils.setFeatureVisibility(sf, "admin,admin,admin,admin")
    sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
    sf
  }
  val mixed = {
    val sf = new ScalaSimpleFeature("mixed", sft)
    sf.setAttribute(0, "name-mixed")
    sf.setAttribute(1, "12")
    sf.setAttribute(2, "2014-01-03T01:00:00.000Z")
    sf.setAttribute(3, "POINT (-120 47)")
    SecurityUtils.setFeatureVisibility(sf, "admin,user,admin,user")
    sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
    sf
  }

  val featureCollection = new DefaultFeatureCollection(sftName, sft)
  featureCollection.add(user)
  featureCollection.add(admin)
  featureCollection.add(mixed)

  // write the feature to the store
  val fs = ds.getFeatureSource(sftName)
  fs.addFeatures(featureCollection)

  def queryByAuths(auths: String, filter: String, expectedStrategy: AccumuloFeatureIndexType): Seq[SimpleFeature] = {
    val ds = DataStoreFinder.getDataStore(Map(
      "connector"    -> connector,
      "tableName"    -> sftName,
      "auths"        -> auths)).asInstanceOf[AccumuloDataStore]
    val query = new Query(sftName, ECQL.toFilter(filter))
    val plans = ds.getQueryPlan(query)
    forall(plans)(_.filter.index mustEqual expectedStrategy)
    SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toSeq
  }

  val filters = Seq(
    ("IN ('user', 'admin', 'mixed')", RecordIndex),
    ("bbox(geom, -121, 44, -119, 48)", Z2Index),
    ("bbox(geom, -121, 44, -119, 48) AND dtg DURING 2014-01-01T00:00:00.000Z/2014-01-04T00:00:00.000Z", Z3Index),
    ("name = 'name-user' OR name = 'name-admin' OR name = 'name-mixed'", AttributeIndex)
  )

  "AccumuloDataStore" should {
    "correctly return all features with appropriate auths" in {
      forall(filters) { case (filter, strategy) =>
        val features = queryByAuths("admin,user", filter, strategy)
        features must haveLength(3)
        features must containTheSameElementsAs(Seq(user, admin, mixed))
      }
    }
    "correctly return some features with appropriate auths" in {
      forall(filters) { case (filter, strategy) =>
        val features = queryByAuths("user", filter, strategy)
        features must haveLength(2)
        features must contain(user)
        val m = features.find(_.getID == "mixed")
        m must beSome
        m.get.getAttribute(0) must beNull
        m.get.getAttribute(1) mustEqual 12
        m.get.getAttribute(2) must beNull
        m.get.getAttribute(3) mustEqual mixed.getAttribute(3)
      }
    }
  }
}
