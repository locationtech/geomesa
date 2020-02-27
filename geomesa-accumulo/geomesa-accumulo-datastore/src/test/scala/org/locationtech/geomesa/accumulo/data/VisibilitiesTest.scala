/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.data

import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.security.Authorizations
import org.geotools.data._
import org.geotools.data.simple.SimpleFeatureStore
import org.geotools.util.factory.Hints
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithDataStore
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.security.SecurityUtils
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.specs2.runner.JUnitRunner
import org.locationtech.geomesa.accumulo.data.MiniCluster

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class VisibilitiesTest extends TestWithDataStore {

  sequential

  override val spec = "name:String:index=full,dtg:Date,*geom:Point:srid=4326"

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
  val rootConnector = MiniCluster.getConnector()
  val privDS = {
    rootConnector.securityOperations().createLocalUser( "priv", new PasswordToken(mockPassword))
    rootConnector.securityOperations().changeUserAuthorizations("priv", new Authorizations("user", "admin"))
    System.out.println("DEBUG - root has created user authorizations: " + rootConnector.securityOperations().getUserAuthorizations("priv"))
    val connector = MiniCluster.getConnector("priv", mockPassword)
    DataStoreFinder.getDataStore(dsParams ++ Map(AccumuloDataStoreParams.ConnectorParam.key -> connector))
    
  }
  System.out.println("DEBUG - data store retrieved for priv")
  val unprivDS = {
    rootConnector.securityOperations().createLocalUser( "unpriv", new PasswordToken(mockPassword))
    
    rootConnector.securityOperations().changeUserAuthorizations("unpriv", new Authorizations("user"))
    val connector = MiniCluster.getConnector("unpriv", mockPassword)
    DataStoreFinder.getDataStore(dsParams ++ Map(AccumuloDataStoreParams.ConnectorParam.key -> connector))
  }

    System.out.println("DEBUG - data store retrieved for unpriv")
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
  }
}
