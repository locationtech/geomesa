/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.data

import org.apache.accumulo.core.client.mock.MockInstance
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.security.Authorizations
import org.geotools.data._
import org.geotools.factory.Hints
import org.geotools.feature.DefaultFeatureCollection
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.index.{ExplainPrintln, QueryHints}
import org.locationtech.geomesa.accumulo.index.Strategy.StrategyType
import org.locationtech.geomesa.accumulo.util.SelfClosingIterator
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.locationtech.geomesa.security.SecurityUtils
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.Filter
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AccumuloDataStoreAttributeVisibilityTest extends Specification {

  import scala.collection.JavaConversions._

  sequential

  val sftName = getClass.getSimpleName
  val spec = "name:String,age:Int,dtg:Date,*geom:Point:srid=4326;geomesa.visibility.level='attribute',geomesa.indexes.enabled='records'"

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
    sf.setAttribute(2, "2014-01-01T00:00:00.000Z")
    sf.setAttribute(3, "POINT (-120 45)")
    SecurityUtils.setFeatureVisibility(sf, "user,user,user,user")
    sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
    sf
  }
  val admin = {
    val sf = new ScalaSimpleFeature("admin", sft)
    sf.setAttribute(0, "name-admin")
    sf.setAttribute(1, "11")
    sf.setAttribute(2, "2014-01-02T00:00:00.000Z")
    sf.setAttribute(3, "POINT (-120 46)")
    SecurityUtils.setFeatureVisibility(sf, "admin,admin,admin,admin")
    sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
    sf
  }
  val mixed = {
    val sf = new ScalaSimpleFeature("mixed", sft)
    sf.setAttribute(0, "name-mixed")
    sf.setAttribute(1, "12")
    sf.setAttribute(2, "2014-01-03T00:00:00.000Z")
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

  def queryByAuths(auths: String): Seq[SimpleFeature] = {
    val ds = DataStoreFinder.getDataStore(Map(
      "connector"    -> connector,
      "tableName"    -> sftName,
      "auths"        -> auths)).asInstanceOf[AccumuloDataStore]
    val query = new Query(sftName, Filter.INCLUDE)
    query.getHints.put(QueryHints.QUERY_STRATEGY_KEY, StrategyType.RECORD) // only index right now that supports vis
//    ds.getQueryPlan(query, explainer = new ExplainPrintln)
    SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toSeq
  }

  "AccumuloDataStore" should {
    "correctly return all features with appropriate auths" in {
      val features = queryByAuths("admin,user")
      features must haveLength(3)
      features must containTheSameElementsAs(Seq(user, admin, mixed))
    }
    "correctly return some features with appropriate auths" in {
      val features = queryByAuths("user")
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
