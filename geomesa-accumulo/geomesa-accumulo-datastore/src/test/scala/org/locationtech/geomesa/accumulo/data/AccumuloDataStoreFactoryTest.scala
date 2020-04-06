/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * Portions Crown Copyright (c) 2016-2020 Dstl
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.data

import java.io.IOException

import org.geotools.data.DataStoreFinder
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.MiniCluster
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AccumuloDataStoreFactoryTest extends Specification {

  import scala.collection.JavaConverters._

  // we use class name to prevent spillage between unit tests
  lazy val catalog = s"${MiniCluster.namespace}.${getClass.getSimpleName}"

  "AccumuloDataStoreFactory" should {

    "create a password authenticated store" in {
      val params = Map(
        AccumuloDataStoreParams.InstanceIdParam.key -> MiniCluster.cluster.getInstanceName,
        AccumuloDataStoreParams.ZookeepersParam.key -> MiniCluster.cluster.getZooKeepers,
        AccumuloDataStoreParams.UserParam.key       -> MiniCluster.Users.root.name,
        AccumuloDataStoreParams.PasswordParam.key   -> MiniCluster.Users.root.password,
        AccumuloDataStoreParams.CatalogParam.key    -> catalog
      ).asJava
      AccumuloDataStoreFactory.canProcess(params) must beTrue
      val ds = DataStoreFinder.getDataStore(params)
      try {
        ds must beAnInstanceOf[AccumuloDataStore]
      } finally {
        if (ds != null) {
          ds.dispose()
        }
      }
    }

    "create a keytab authenticated store" in {
      val params = Map(
        AccumuloDataStoreParams.InstanceIdParam.key -> MiniCluster.cluster.getInstanceName,
        AccumuloDataStoreParams.ZookeepersParam.key -> MiniCluster.cluster.getZooKeepers,
        AccumuloDataStoreParams.UserParam.key       -> MiniCluster.Users.root.name,
        AccumuloDataStoreParams.KeytabPathParam.key -> "/path/to/keytab",
        AccumuloDataStoreParams.CatalogParam.key    -> catalog
      ).asJava
      AccumuloDataStoreFactory.canProcess(params) must beTrue
      // TODO GEOMESA-2797 test kerberos
    }

    "not accept password and keytab" in {
      val params = Map(
        AccumuloDataStoreParams.InstanceIdParam.key -> MiniCluster.cluster.getInstanceName,
        AccumuloDataStoreParams.ZookeepersParam.key -> MiniCluster.cluster.getZooKeepers,
        AccumuloDataStoreParams.UserParam.key       -> MiniCluster.Users.root.name,
        AccumuloDataStoreParams.PasswordParam.key   -> MiniCluster.Users.root.password,
        AccumuloDataStoreParams.KeytabPathParam.key -> "/path/to/keytab",
        AccumuloDataStoreParams.CatalogParam.key    -> catalog
      ).asJava
      AccumuloDataStoreFactory.canProcess(params) must beTrue
      DataStoreFinder.getDataStore(params) must throwAn[IllegalArgumentException]
    }

    "not accept a missing instanceId" in {
      val params = Map(
        AccumuloDataStoreParams.ZookeepersParam.key -> MiniCluster.cluster.getZooKeepers,
        AccumuloDataStoreParams.UserParam.key       -> MiniCluster.Users.root.name,
        AccumuloDataStoreParams.PasswordParam.key   -> MiniCluster.Users.root.password,
        AccumuloDataStoreParams.CatalogParam.key    -> catalog
      ).asJava
      AccumuloDataStoreFactory.canProcess(params) must beTrue
      DataStoreFinder.getDataStore(params) must throwAn[IOException]
    }

    "not accept a missing zookeepers" in {
      val params = Map(
        AccumuloDataStoreParams.InstanceIdParam.key -> MiniCluster.cluster.getInstanceName,
        AccumuloDataStoreParams.UserParam.key       -> MiniCluster.Users.root.name,
        AccumuloDataStoreParams.PasswordParam.key   -> MiniCluster.Users.root.password,
        AccumuloDataStoreParams.CatalogParam.key    -> catalog
      ).asJava
      AccumuloDataStoreFactory.canProcess(params) must beTrue
      DataStoreFinder.getDataStore(params) must throwAn[IOException]
    }

    "not accept a missing user" in {
      val params = Map(
        AccumuloDataStoreParams.InstanceIdParam.key -> MiniCluster.cluster.getInstanceName,
        AccumuloDataStoreParams.ZookeepersParam.key -> MiniCluster.cluster.getZooKeepers,
        AccumuloDataStoreParams.PasswordParam.key   -> MiniCluster.Users.root.password,
        AccumuloDataStoreParams.CatalogParam.key    -> catalog
      ).asJava
      AccumuloDataStoreFactory.canProcess(params) must beTrue
      DataStoreFinder.getDataStore(params) must throwAn[IOException]
    }

    "not accept a missing password and keytab" in {
      val params = Map(
        AccumuloDataStoreParams.InstanceIdParam.key -> MiniCluster.cluster.getInstanceName,
        AccumuloDataStoreParams.ZookeepersParam.key -> MiniCluster.cluster.getZooKeepers,
        AccumuloDataStoreParams.UserParam.key       -> MiniCluster.Users.root.name,
        AccumuloDataStoreParams.CatalogParam.key    -> catalog
      ).asJava
      AccumuloDataStoreFactory.canProcess(params) must beTrue
      DataStoreFinder.getDataStore(params) must throwAn[IOException]
    }
  }
}
