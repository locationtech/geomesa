/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.data

import org.geotools.api.data._
import org.geotools.api.feature.simple.SimpleFeature
import org.geotools.filter.text.ecql.ECQL
import org.geotools.util.factory.Hints
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithFeatureType
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.index.index.attribute.AttributeIndex
import org.locationtech.geomesa.index.index.id.IdIndex
import org.locationtech.geomesa.index.index.z2.Z2Index
import org.locationtech.geomesa.index.index.z3.Z3Index
import org.locationtech.geomesa.security.SecurityUtils
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.io.WithClose
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AccumuloDataStoreAttributeVisibilityTest extends TestWithFeatureType {

  import scala.collection.JavaConverters._

  sequential

  override val spec = "name:String:index=full,age:Int,dtg:Date,*geom:Point:srid=4326;geomesa.visibility.level='attribute'"

  val userFeature = {
    val sf = new ScalaSimpleFeature(sft, "user")
    sf.setAttribute(0, "name-user")
    sf.setAttribute(1, "10")
    sf.setAttribute(2, "2014-01-01T01:00:00.000Z")
    sf.setAttribute(3, "POINT (-120 45)")
    SecurityUtils.setFeatureVisibility(sf, "user,user,user,user")
    sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
    sf
  }
  val adminFeature = {
    val sf = new ScalaSimpleFeature(sft, "admin")
    sf.setAttribute(0, "name-admin")
    sf.setAttribute(1, "11")
    sf.setAttribute(2, "2014-01-02T01:00:00.000Z")
    sf.setAttribute(3, "POINT (-120 46)")
    SecurityUtils.setFeatureVisibility(sf, "admin,admin,admin,admin")
    sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
    sf
  }
  val mixedFeature = {
    val sf = new ScalaSimpleFeature(sft, "mixed")
    sf.setAttribute(0, "name-mixed")
    sf.setAttribute(1, "12")
    sf.setAttribute(2, "2014-01-03T01:00:00.000Z")
    sf.setAttribute(3, "POINT (-120 47)")
    SecurityUtils.setFeatureVisibility(sf, "admin,user,admin,user")
    sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
    sf
  }

  step {
    // write the features to the store
    addFeatures(Seq(userFeature, adminFeature, mixedFeature))
  }

  def queryByAuths(auths: String, filter: String, strategy: String): Seq[SimpleFeature] = {
    val params = dsParams ++ Map(AccumuloDataStoreParams.AuthsParam.key -> auths)
    WithClose(DataStoreFinder.getDataStore(params.asJava).asInstanceOf[AccumuloDataStore]) { ds =>
      val query = new Query(sftName, ECQL.toFilter(filter))
      query.getHints.put(QueryHints.QUERY_INDEX, strategy)
      val plans = ds.getQueryPlan(query)
      forall(plans)(_.filter.index.name mustEqual strategy)
      SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList
    }
  }

  val filters = Seq(
    VisTestCase("IN ('user', 'admin', 'mixed')", Seq(IdIndex.name), returnMixed = true),
    VisTestCase("bbox(geom, -121, 44, -119, 48)", Seq(Z2Index.name), returnMixed = true),
    // these two cases filter on a non-visible attribute so shouldn't return the 'mixed' feature
    VisTestCase("bbox(geom, -121, 44, -119, 48) AND dtg DURING 2014-01-01T00:00:00.000Z/2014-01-04T00:00:00.000Z", Seq(Z3Index.name), returnMixed = false),
    VisTestCase("(name = 'name-user' OR name = 'name-admin' OR name = 'name-mixed')", Seq(AttributeIndex.name, Z3Index.name), returnMixed = false)
  )

  "AccumuloDataStore" should {
    "correctly return all features with appropriate auths" in {
      foreach(filters) { case VisTestCase(filter, strategies, _) =>
        foreach(strategies) { strategy =>
          val features = queryByAuths("admin,user", filter, strategy)
          features must haveLength(3)
          features must containTheSameElementsAs(Seq(userFeature, adminFeature, mixedFeature))
        }
      }
    }
    "correctly return some features with appropriate auths" in {
      foreach(filters) { case VisTestCase(filter, strategies, returnMixed) =>
        foreach(strategies) { strategy =>
          val features = queryByAuths("user", filter, strategy)
          features must contain(userFeature)
          if (!returnMixed) {
            features must haveLength(1)
          } else {
            features must haveLength(2)
            val m = features.find(_.getID == "mixed")
            m must beSome
            m.get.getAttribute(0) must beNull
            m.get.getAttribute(1) mustEqual 12
            m.get.getAttribute(2) must beNull
            m.get.getAttribute(3) mustEqual mixedFeature.getAttribute(3)
          }
        }
      }
    }

    "delete one record" in {
      val fs = ds.getFeatureSource(sftName)

      val queryBefore = new Query(sftName, ECQL.toFilter("IN('user')"))
      val resultsBefore = SelfClosingIterator(ds.getFeatureReader(queryBefore, Transaction.AUTO_COMMIT)).toSeq
      resultsBefore must haveLength(1)

      fs.removeFeatures(ECQL.toFilter("IN('user')"))

      val queryAfter = new Query(sftName, ECQL.toFilter("IN('user')"))
      val resultsAfter = SelfClosingIterator(ds.getFeatureReader(queryAfter, Transaction.AUTO_COMMIT)).toSeq
      resultsAfter must beEmpty
    }

    "delete all records" in {
      val fs = ds.getFeatureSource(sftName)

      val queryBefore = new Query(sftName, ECQL.toFilter("INCLUDE"))
      val resultsBefore = SelfClosingIterator(ds.getFeatureReader(queryBefore, Transaction.AUTO_COMMIT)).toSeq
      resultsBefore must haveLength(2) // We deleted one record in the prior test

      fs.removeFeatures(ECQL.toFilter("INCLUDE"))

      val queryAfter = new Query(sftName, ECQL.toFilter("INCLUDE"))
      val resultsAfter = SelfClosingIterator(ds.getFeatureReader(queryAfter, Transaction.AUTO_COMMIT)).toSeq
      resultsAfter must beEmpty
    }
  }

  case class VisTestCase(filter: String, indices: Seq[String], returnMixed: Boolean)
}
