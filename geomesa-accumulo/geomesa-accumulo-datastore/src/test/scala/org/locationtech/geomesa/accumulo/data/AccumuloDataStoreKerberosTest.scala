/***********************************************************************
 * Crown Copyright (c) 2016 Dstl
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.data

import org.geotools.data.DataStoreFinder
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class AccumuloDataStoreKerberosTest extends Specification {

  // Note that these tests all specify a mock connector, which means they don't
  // actually authenticate against a KDC, but they do test parameter validation.

  import AccumuloDataStoreParams._

  "AccumuloDataStore" should {

    "create a password authenticated store" in {
      DataStoreFinder.getDataStore(Map(
        MockParam.key       -> "true",
        InstanceIdParam.key -> "my-instance",
        ZookeepersParam.key -> "zoo:2181",
        UserParam.key       -> "me",
        PasswordParam.key   -> "secret",
        CatalogParam.key    -> "tableName").asJava).asInstanceOf[AccumuloDataStore] must not(beNull)
    }

    "create a keytab authenticated store" in {
      val ds = DataStoreFinder.getDataStore(Map(
        MockParam.key       -> "true",
        InstanceIdParam.key -> "my-instance",
        ZookeepersParam.key -> "zoo:2181",
        UserParam.key       -> "user@EXAMPLE.COM",
        KeytabPathParam.key -> "/path/to/keytab",
        CatalogParam.key    -> "tableName").asJava).asInstanceOf[AccumuloDataStore]
      if (AccumuloDataStoreFactory.isKerberosAvailable) {
        ds must not(beNull)
      } else {
        ds must beNull
      }
    }

    "not accept password and keytab" in {
      DataStoreFinder.getDataStore(Map(
        MockParam.key -> "true",
        InstanceIdParam.key -> "my-instance",
        ZookeepersParam.key -> "zoo:2181",
        UserParam.key -> "me",
        PasswordParam.key -> "password",
        KeytabPathParam.key -> "/path/to/keytab",
        CatalogParam.key -> "tableName").asJava) must beNull
    }

    "not accept a missing instanceId" in {
      DataStoreFinder.getDataStore(Map(
        MockParam.key          -> "true",
        // InstanceIdParam.key -> "my-instance",
        ZookeepersParam.key    -> "zoo:2181",
        UserParam.key          -> "me",
        PasswordParam.key      -> "password",
        CatalogParam.key       -> "tableName").asJava) must beNull
    }

    "not accept a missing zookeepers" in {
      DataStoreFinder.getDataStore(Map(
        MockParam.key          -> "true",
        InstanceIdParam.key    -> "my-instance",
        // ZookeepersParam.key -> "zoo:2181",
        UserParam.key          -> "me",
        PasswordParam.key      -> "password",
        CatalogParam.key       -> "tableName").asJava) must beNull
    }

    "not accept a missing user" in {
      DataStoreFinder.getDataStore(Map(
        MockParam.key       -> "true",
        InstanceIdParam.key -> "my-instance",
        ZookeepersParam.key -> "zoo:2181",
        // UserParam.key    -> "me",
        PasswordParam.key   -> "password",
        CatalogParam.key    -> "tableName").asJava) must beNull
    }

    "not accept a missing password and keytab" in {
      DataStoreFinder.getDataStore(Map(
        MockParam.key        -> "true",
        InstanceIdParam.key  -> "my-instance",
        ZookeepersParam.key  -> "zoo:2181",
        UserParam.key        -> "me",
        // PasswordParam.key -> "password",
        CatalogParam.key     -> "tableName").asJava) must beNull
    }

  }

}
