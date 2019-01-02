/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * Portions Crown Copyright (c) 2016-2019 Dstl
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

@RunWith(classOf[JUnitRunner])
class AccumuloDataStoreKerberosTest extends Specification {

  // Note that these tests all specify a mock connector, which means they don't
  // actually authenticate against a KDC, but they do test parameter validation.

  import AccumuloDataStoreParams._

  def maps(tuples: (String, String)*): (java.util.Map[String, java.io.Serializable], java.util.Map[String, java.io.Serializable]) = {
    import scala.collection.JavaConverters._
    val map = tuples.toMap[String, java.io.Serializable]
    (map.asJava, (map + (MockParam.key -> "true")).asJava)
  }

  "AccumuloDataStore" should {

    "create a password authenticated store" in {
      val (real, mock) = maps(
        InstanceIdParam.key -> "my-instance",
        ZookeepersParam.key -> "zoo:2181",
        UserParam.key       -> "me",
        PasswordParam.key   -> "secret",
        CatalogParam.key    -> "tableName")
      AccumuloDataStoreFactory.canProcess(real) must beTrue
      DataStoreFinder.getDataStore(mock) must not(beNull)
    }

    "create a keytab authenticated store" in {
      val (real, mock) = maps(
        InstanceIdParam.key -> "my-instance",
        ZookeepersParam.key -> "zoo:2181",
        UserParam.key       -> "user@EXAMPLE.COM",
        KeytabPathParam.key -> "/path/to/keytab",
        CatalogParam.key    -> "tableName")
      if (AccumuloDataStoreFactory.isKerberosAvailable) {
        AccumuloDataStoreFactory.canProcess(real) must beTrue
        DataStoreFinder.getDataStore(mock).asInstanceOf[AccumuloDataStore] must not(beNull)
      } else {
        AccumuloDataStoreFactory.canProcess(real) must beFalse
        DataStoreFinder.getDataStore(mock).asInstanceOf[AccumuloDataStore] must beNull
      }
    }

    "not accept password and keytab" in {
      val (real, mock) = maps(
        InstanceIdParam.key -> "my-instance",
        ZookeepersParam.key -> "zoo:2181",
        UserParam.key       -> "me",
        PasswordParam.key   -> "password",
        KeytabPathParam.key -> "/path/to/keytab",
        CatalogParam.key    -> "tableName")
      AccumuloDataStoreFactory.canProcess(real) must beFalse
      DataStoreFinder.getDataStore(mock) must beNull
    }

    "not accept a missing instanceId" in {
      val (real, mock) = maps(
        // InstanceIdParam.key -> "my-instance",
        ZookeepersParam.key    -> "zoo:2181",
        UserParam.key          -> "me",
        PasswordParam.key      -> "password",
        CatalogParam.key       -> "tableName")
      AccumuloDataStoreFactory.canProcess(real) must beFalse
      DataStoreFinder.getDataStore(mock) must beNull
    }

    "not accept a missing zookeepers" in {
      val (real, mock) = maps(
        InstanceIdParam.key    -> "my-instance",
        // ZookeepersParam.key -> "zoo:2181",
        UserParam.key          -> "me",
        PasswordParam.key      -> "password",
        CatalogParam.key       -> "tableName")
      AccumuloDataStoreFactory.canProcess(real) must beFalse
      // can't test actual data store since mock without zookeepers is ok
    }

    "not accept a missing user" in {
      val (real, mock) = maps(
        InstanceIdParam.key -> "my-instance",
        ZookeepersParam.key -> "zoo:2181",
        // UserParam.key    -> "me",
        PasswordParam.key   -> "password",
        CatalogParam.key    -> "tableName")
      AccumuloDataStoreFactory.canProcess(real) must beFalse
      DataStoreFinder.getDataStore(mock) must beNull
    }

    "not accept a missing password and keytab" in {
      val (real, mock) = maps(
        InstanceIdParam.key  -> "my-instance",
        ZookeepersParam.key  -> "zoo:2181",
        UserParam.key        -> "me",
        // PasswordParam.key -> "password",
        CatalogParam.key     -> "tableName")
      AccumuloDataStoreFactory.canProcess(real) must beFalse
      DataStoreFinder.getDataStore(mock) must beNull
    }
  }
}
