/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.data

import org.geotools.data._
import org.geotools.data.simple.SimpleFeatureStore
import org.geotools.filter.text.ecql.ECQL
import org.geotools.util.factory.Hints
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithFeatureType
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.security.SecurityUtils
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class VisibilitiesTest extends TestWithFeatureType {
  
  sequential

  override val spec = "name:String:index=full,dtg:Date,*geom:Point:srid=4326;geomesa.vis.required='true'"

  val privFeatures = (0 until 3).map { i =>
    val sf = ScalaSimpleFeature.create(sft, s"$i", s"name$i", "2012-01-02T05:06:07.000Z", "POINT(45.0 45.0)")
    sf.getUserData.put(SecurityUtils.FEATURE_VISIBILITY, "user&admin")
    sf.getUserData.put(Hints.USE_PROVIDED_FID, Boolean.box(true))
    sf
  }
  val unprivFeatures = (3 until 6).map { i =>
    val sf = ScalaSimpleFeature.create(sft, s"$i", s"name$i", "2012-01-02T05:06:07.000Z", "POINT(45.0 45.0)")
    sf.getUserData.put(SecurityUtils.FEATURE_VISIBILITY, "user")
    sf.getUserData.put(Hints.USE_PROVIDED_FID, Boolean.box(true))
    sf
  }
  val privDS = DataStoreFinder.getDataStore(dsParams ++ Map(AccumuloDataStoreParams.UserParam.key -> admin.name))
  val unprivDS = DataStoreFinder.getDataStore(dsParams ++ Map(AccumuloDataStoreParams.UserParam.key -> user.name))
  
  step {
    addFeatures(privFeatures ++ unprivFeatures)
  }

  val filters = Seq(
    "INCLUDE",
    "bbox(geom,44,44,46,46)",
    "bbox(geom,44,44,46,46) and dtg DURING 2012-01-02T05:00:00.000Z/2012-01-02T05:10:00.000Z",
    "name in ('name0', 'name1', 'name2', 'name3', 'name4', 'name5')",
    "IN ('0', '1', '2', '3', '4', '5')"
  ).map(ECQL.toFilter)

  "AccumuloDataStore" should {

    "keep unprivileged from reading secured features" in {
      foreach(filters) { filter =>
        val reader = unprivDS.getFeatureReader(new Query(sftName, filter), Transaction.AUTO_COMMIT)
        SelfClosingIterator(reader).toList must containTheSameElementsAs(unprivFeatures)
      }
    }

    "allow privileged to read secured features" in {
      foreach(filters) { filter =>
        val reader = privDS.getFeatureReader(new Query(sftName, filter), Transaction.AUTO_COMMIT)
        SelfClosingIterator(reader).toList must containTheSameElementsAs(privFeatures ++ unprivFeatures)
      }
    }

    "keep unprivileged from deleting secured features" in {
      unprivDS.getFeatureSource(sftName).asInstanceOf[SimpleFeatureStore].removeFeatures(ECQL.toFilter("IN('2')"))
      foreach(filters) { filter =>
        val reader = privDS.getFeatureReader(new Query(sftName, filter), Transaction.AUTO_COMMIT)
        SelfClosingIterator(reader).toList must containTheSameElementsAs(privFeatures ++ unprivFeatures)
      }
    }

    "allow privileged to delete secured features" in {
      privDS.getFeatureSource(sftName).asInstanceOf[SimpleFeatureStore].removeFeatures(ECQL.toFilter("IN('2')"))
      foreach(filters) { filter =>
        val reader = privDS.getFeatureReader(new Query(sftName, filter), Transaction.AUTO_COMMIT)
        SelfClosingIterator(reader).toList must containTheSameElementsAs(privFeatures.take(2) ++ unprivFeatures)
      }
    }

    "allow privileged to update secured features" in {
      WithClose(privDS.getFeatureWriter(sftName, ECQL.toFilter("IN('0')"), Transaction.AUTO_COMMIT)) { writer =>
        writer.hasNext must beTrue
        val f = writer.next
        f.getUserData.put(SecurityUtils.FEATURE_VISIBILITY, "user")
        writer.write()
        writer.hasNext must beFalse
      }
      foreach(filters) { filter =>
        val reader = privDS.getFeatureReader(new Query(sftName, filter), Transaction.AUTO_COMMIT)
        SelfClosingIterator(reader).toList must containTheSameElementsAs(privFeatures.take(2) ++ unprivFeatures)
        val unprivReader = unprivDS.getFeatureReader(new Query(sftName, filter), Transaction.AUTO_COMMIT)
        SelfClosingIterator(unprivReader).toList must containTheSameElementsAs(privFeatures.take(1) ++ unprivFeatures)
      }
    }

    "require visibilities when writing data" in {
      val unsecured = ScalaSimpleFeature.create(sft, "6", "name6", "2012-01-02T05:06:07.000Z", "POINT(45.0 45.0)")
      addFeature(unsecured) must throwAn[IllegalArgumentException]
      val updated = SimpleFeatureTypes.copy(sft)
      updated.getUserData.put("geomesa.vis.required", "false")
      ds.updateSchema(updated.getTypeName, updated)
      addFeature(unsecured) // should be ok
      val query = new Query(sftName, ECQL.toFilter("IN('6')"))
      // verify we can query the feature back out
      SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList mustEqual Seq(unsecured)
      updated.getUserData.put("geomesa.vis.required", "true")
      ds.updateSchema(updated.getTypeName, updated)
      // verify ReqVisFilter prevents the feature from coming back even though it exists in the table
      SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toList must beEmpty
    }
  }
}
