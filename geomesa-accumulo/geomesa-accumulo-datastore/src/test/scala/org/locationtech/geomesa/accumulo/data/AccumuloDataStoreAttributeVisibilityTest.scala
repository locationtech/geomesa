/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.data

import org.geotools.data._
import org.geotools.factory.Hints
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithDataStore
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.index.index.attribute.AttributeIndex
import org.locationtech.geomesa.index.index.id.IdIndex
import org.locationtech.geomesa.index.index.z2.Z2Index
import org.locationtech.geomesa.index.index.z3.Z3Index
import org.locationtech.geomesa.security.SecurityUtils
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.opengis.feature.simple.SimpleFeature
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AccumuloDataStoreAttributeVisibilityTest extends TestWithDataStore {

  import scala.collection.JavaConversions._

  sequential

  override val spec = "name:String:index=full,age:Int,dtg:Date,*geom:Point:srid=4326;geomesa.visibility.level='attribute'"

  val user = {
    val sf = new ScalaSimpleFeature(sft, "user")
    sf.setAttribute(0, "name-user")
    sf.setAttribute(1, "10")
    sf.setAttribute(2, "2014-01-01T01:00:00.000Z")
    sf.setAttribute(3, "POINT (-120 45)")
    SecurityUtils.setFeatureVisibility(sf, "user,user,user,user")
    sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
    sf
  }
  val admin = {
    val sf = new ScalaSimpleFeature(sft, "admin")
    sf.setAttribute(0, "name-admin")
    sf.setAttribute(1, "11")
    sf.setAttribute(2, "2014-01-02T01:00:00.000Z")
    sf.setAttribute(3, "POINT (-120 46)")
    SecurityUtils.setFeatureVisibility(sf, "admin,admin,admin,admin")
    sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
    sf
  }
  val mixed = {
    val sf = new ScalaSimpleFeature(sft, "mixed")
    sf.setAttribute(0, "name-mixed")
    sf.setAttribute(1, "12")
    sf.setAttribute(2, "2014-01-03T01:00:00.000Z")
    sf.setAttribute(3, "POINT (-120 47)")
    SecurityUtils.setFeatureVisibility(sf, "admin,user,admin,user")
    sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
    sf
  }

  // write the feature to the store
  step {
    addFeatures(Seq(user, admin, mixed))
  }

  def queryByAuths(auths: String, filter: String, expectedStrategy: String): Seq[SimpleFeature] = {
    val ds = DataStoreFinder.getDataStore(dsParams ++ Map(AccumuloDataStoreParams.AuthsParam.key -> auths)).asInstanceOf[AccumuloDataStore]
    val query = new Query(sftName, ECQL.toFilter(filter))
    val plans = ds.getQueryPlan(query)
    forall(plans)(_.filter.index.name mustEqual expectedStrategy)
    SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toSeq
  }

  val filters = Seq(
    ("IN ('user', 'admin', 'mixed')", IdIndex.name),
    ("bbox(geom, -121, 44, -119, 48)", Z2Index.name),
    ("bbox(geom, -121, 44, -119, 48) AND dtg DURING 2014-01-01T00:00:00.000Z/2014-01-04T00:00:00.000Z", Z3Index.name),
    ("name = 'name-user' OR name = 'name-admin' OR name = 'name-mixed'", AttributeIndex.name)
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
