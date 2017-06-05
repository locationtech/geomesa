/***********************************************************************
 * Crown Copyright (c) 2016 Dstl
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.data

import org.locationtech.geomesa.accumulo.data.AccumuloDataStoreFactory

import org.geotools.data.DataStoreFinder

import org.specs2.runner.JUnitRunner
import org.junit.runner.RunWith

import org.specs2.mutable.Specification

import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class AccumuloDataStoreKerberosTest extends Specification {

  // Note that these tests all specify a mock connector, which means they don't
  // actually authenticate against a KDC, but they do test parameter validation.

  "AccumuloDataStore" should {

    "create a password authenticated store" in {
      val ds = DataStoreFinder.getDataStore(Map(
        "useMock" -> "true",
        "instanceId" -> "my-instance",
        "zookeepers" -> "zoo:2181",
        "user" -> "me",
        "password" -> "secret",
        "tableName" -> "tableName").asJava).asInstanceOf[AccumuloDataStore]
      ds must not(beNull)
    }

    "create a keytab authenticated store" in {
      val ds = DataStoreFinder.getDataStore(Map(
        "useMock" -> "true",
        "instanceId" -> "my-instance",
        "zookeepers" -> "zoo:2181",
        "user" -> "user@EXAMPLE.COM",
        "keytabPath" -> "/path/to/keytab",
        "tableName" -> "tableName").asJava).asInstanceOf[AccumuloDataStore]
      if (AccumuloDataStoreFactory.isKerberosAvailable) ds must not(beNull) else ds must (beNull)
    }

    "not accept password and keytab" in {
      val ds = DataStoreFinder.getDataStore(Map(
        "useMock" -> "true",
        "instanceId" -> "my-instance",
        "zookeepers" -> "zoo:2181",
        "user" -> "me",
        "password" -> "password",
        "keytabPath" -> "/path/to/keytab",
        "tableName" -> "tableName").asJava).asInstanceOf[AccumuloDataStore]
      ds must (beNull)
    }

    "not accept a missing instanceId" in {
      val ds = DataStoreFinder.getDataStore(Map(
        "useMock" -> "true",
        //"instanceId" -> "my-instance",
        "zookeepers" -> "zoo:2181",
        "user" -> "me",
        "password" -> "password",
        "tableName" -> "tableName").asJava).asInstanceOf[AccumuloDataStore]
      ds must (beNull)
    }

    "not accept a missing zookeepers" in {
      val ds = DataStoreFinder.getDataStore(Map(
        "useMock" -> "true",
        "instanceId" -> "my-instance",
        //"zookeepers" -> "zoo:2181",
        "user" -> "me",
        "password" -> "password",
        "tableName" -> "tableName").asJava).asInstanceOf[AccumuloDataStore]
      ds must (beNull)
    }

    "not accept a missing user" in {
      val ds = DataStoreFinder.getDataStore(Map(
        "useMock" -> "true",
        "instanceId" -> "my-instance",
        "zookeepers" -> "zoo:2181",
        //"user" -> "me",
        "password" -> "password",
        "tableName" -> "tableName").asJava).asInstanceOf[AccumuloDataStore]
      ds must (beNull)
    }

    "not accept a missing password and keytab" in {
      val ds = DataStoreFinder.getDataStore(Map(
        "useMock" -> "true",
        "instanceId" -> "my-instance",
        "zookeepers" -> "zoo:2181",
        "user" -> "me",
        //"password" -> "password",
        "tableName" -> "tableName").asJava).asInstanceOf[AccumuloDataStore]
      ds must (beNull)
    }

  }

}
