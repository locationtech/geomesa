/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * Portions Crown Copyright (c) 2016-2020 Dstl
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.data

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AccumuloDataStoreFactoryTest extends Specification {

  import AccumuloDataStoreParams._

  import scala.collection.JavaConverters._

  "AccumuloDataStoreFactory" should {

    "create a password authenticated store" in {
      val paramsMap = Map(
        InstanceIdParam.key -> "my-instance",
        ZookeepersParam.key -> "zoo:2181",
        UserParam.key       -> "user@EXAMPLE.COM",
        KeytabPathParam.key -> "/path/to/keytab",
        CatalogParam.key    -> "tableName").asJava
      AccumuloDataStoreFactory.canProcess(paramsMap) must beTrue
    }

    "create a keytab authenticated store" in {
      val real = Map(
        InstanceIdParam.key -> "my-instance",
        ZookeepersParam.key -> "zoo:2181",
        UserParam.key       -> "user@EXAMPLE.COM",
        KeytabPathParam.key -> "/path/to/keytab",
        CatalogParam.key    -> "tableName").asJava
      if (AccumuloDataStoreFactory.isKerberosAvailable) {
        AccumuloDataStoreFactory.canProcess(real) must beTrue
      } else {
        AccumuloDataStoreFactory.canProcess(real) must beFalse
      }
    }

    "not accept password and keytab" in {
      val real = Map(
        InstanceIdParam.key -> "my-instance",
        ZookeepersParam.key -> "zoo:2181",
        UserParam.key       -> "me",
        PasswordParam.key   -> "password",
        KeytabPathParam.key -> "/path/to/keytab",
        CatalogParam.key    -> "tableName").asJava
      AccumuloDataStoreFactory.canProcess(real) must beFalse
    }

    "not accept a missing instanceId" in {
       val real = Map(
        // InstanceIdParam.key -> "my-instance",
        ZookeepersParam.key    -> "zoo:2181",
        UserParam.key          -> "me",
        PasswordParam.key      -> "password",
        CatalogParam.key       -> "tableName").asJava
      AccumuloDataStoreFactory.canProcess(real) must beFalse
    }

    "not accept a missing zookeepers" in {
      val real = Map(
        InstanceIdParam.key    -> "my-instance",
        // ZookeepersParam.key -> "zoo:2181",
        UserParam.key          -> "me",
        PasswordParam.key      -> "password",
        CatalogParam.key       -> "tableName").asJava
      AccumuloDataStoreFactory.canProcess(real) must beFalse
    }

    "not accept a missing user" in {
      val real = Map(
        InstanceIdParam.key -> "my-instance",
        ZookeepersParam.key -> "zoo:2181",
        // UserParam.key    -> "me",
        PasswordParam.key   -> "password",
        CatalogParam.key    -> "tableName").asJava
      AccumuloDataStoreFactory.canProcess(real) must beFalse
    }

    "not accept a missing password and keytab" in {
      val real = Map(
        InstanceIdParam.key  -> "my-instance",
        ZookeepersParam.key  -> "zoo:2181",
        UserParam.key        -> "me",
        // PasswordParam.key -> "password",
        CatalogParam.key     -> "tableName").asJava
      AccumuloDataStoreFactory.canProcess(real) must beFalse
    }
  }
}