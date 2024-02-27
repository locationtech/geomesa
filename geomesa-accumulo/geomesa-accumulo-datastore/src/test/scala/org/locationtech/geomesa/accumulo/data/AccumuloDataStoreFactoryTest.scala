/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * Portions Crown Copyright (c) 2016-2024 Dstl
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.data

import org.geotools.api.data.DataStoreFinder
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.AccumuloContainer
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import java.io.IOException

@RunWith(classOf[JUnitRunner])
class AccumuloDataStoreFactoryTest extends Specification {

  import scala.collection.JavaConverters._

  // we use class name to prevent spillage between unit tests
  lazy val catalog = s"${AccumuloContainer.Namespace}.${getClass.getSimpleName}"

  "AccumuloDataStoreFactory" should {

    "create a password authenticated store" in {
      val params = Map(
        AccumuloDataStoreParams.InstanceNameParam.key -> AccumuloContainer.instanceName,
        AccumuloDataStoreParams.ZookeepersParam.key   -> AccumuloContainer.zookeepers,
        AccumuloDataStoreParams.UserParam.key         -> AccumuloContainer.user,
        AccumuloDataStoreParams.PasswordParam.key     -> AccumuloContainer.password,
        AccumuloDataStoreParams.CatalogParam.key      -> catalog
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
        AccumuloDataStoreParams.InstanceNameParam.key -> AccumuloContainer.instanceName,
        AccumuloDataStoreParams.ZookeepersParam.key   -> AccumuloContainer.zookeepers,
        AccumuloDataStoreParams.UserParam.key         -> AccumuloContainer.user,
        AccumuloDataStoreParams.KeytabPathParam.key   -> "/path/to/keytab",
        AccumuloDataStoreParams.CatalogParam.key      -> catalog
      ).asJava
      AccumuloDataStoreFactory.canProcess(params) must beTrue
      // TODO GEOMESA-2797 test kerberos
    }

    "not accept password and keytab" in {
      val params = Map(
        AccumuloDataStoreParams.InstanceNameParam.key -> AccumuloContainer.instanceName,
        AccumuloDataStoreParams.ZookeepersParam.key   -> AccumuloContainer.zookeepers,
        AccumuloDataStoreParams.UserParam.key         -> AccumuloContainer.user,
        AccumuloDataStoreParams.PasswordParam.key     -> AccumuloContainer.password,
        AccumuloDataStoreParams.KeytabPathParam.key   -> "/path/to/keytab",
        AccumuloDataStoreParams.CatalogParam.key      -> catalog
      ).asJava
      AccumuloDataStoreFactory.canProcess(params) must beTrue
      DataStoreFinder.getDataStore(params) must throwAn[IllegalArgumentException]
    }

    "not accept a missing instanceId" in {
      val params = Map(
        AccumuloDataStoreParams.ZookeepersParam.key -> AccumuloContainer.zookeepers,
        AccumuloDataStoreParams.UserParam.key       -> AccumuloContainer.user,
        AccumuloDataStoreParams.PasswordParam.key   -> AccumuloContainer.password,
        AccumuloDataStoreParams.CatalogParam.key    -> catalog
      ).asJava
      AccumuloDataStoreFactory.canProcess(params) must beTrue
      DataStoreFinder.getDataStore(params) must throwAn[IOException]
    }

    "not accept a missing zookeepers" in {
      val params = Map(
        AccumuloDataStoreParams.InstanceNameParam.key -> AccumuloContainer.instanceName,
        AccumuloDataStoreParams.UserParam.key         -> AccumuloContainer.user,
        AccumuloDataStoreParams.PasswordParam.key     -> AccumuloContainer.password,
        AccumuloDataStoreParams.CatalogParam.key      -> catalog
      ).asJava
      AccumuloDataStoreFactory.canProcess(params) must beTrue
      DataStoreFinder.getDataStore(params) must throwAn[IOException]
    }

    "not accept a missing user" in {
      val params = Map(
        AccumuloDataStoreParams.InstanceNameParam.key -> AccumuloContainer.instanceName,
        AccumuloDataStoreParams.ZookeepersParam.key   -> AccumuloContainer.zookeepers,
        AccumuloDataStoreParams.PasswordParam.key     -> AccumuloContainer.password,
        AccumuloDataStoreParams.CatalogParam.key      -> catalog
      ).asJava
      AccumuloDataStoreFactory.canProcess(params) must beTrue
      DataStoreFinder.getDataStore(params) must throwAn[IOException]
    }

    "not accept a missing password and keytab" in {
      val params = Map(
        AccumuloDataStoreParams.InstanceNameParam.key -> AccumuloContainer.instanceName,
        AccumuloDataStoreParams.ZookeepersParam.key   -> AccumuloContainer.zookeepers,
        AccumuloDataStoreParams.UserParam.key         -> AccumuloContainer.user,
        AccumuloDataStoreParams.CatalogParam.key      -> catalog
      ).asJava
      AccumuloDataStoreFactory.canProcess(params) must beTrue
      DataStoreFinder.getDataStore(params) must throwAn[IOException]
    }
  }
}
