/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.gt.partition.postgis

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class PartitionedPostgisDataStoreFactoryTest extends Specification {

  import scala.collection.JavaConverters._

  "PartitionedPostgisDataStoreFactory" should {

    "advertise the prepare_threshold parameter" in {
      // guards the setupParameters registration: if the param is dropped from that Seq,
      // GeoTools silently stops exposing it and this fails
      val factory = new PartitionedPostgisDataStoreFactory()
      val keys = factory.getParametersInfo.map(_.key).toSeq
      keys must contain(PartitionedPostgisDataStoreParams.PrepareThreshold.key)
      PartitionedPostgisDataStoreParams.PrepareThreshold.key mustEqual "prepare_threshold"
    }

    "emit prepare_threshold as the pgjdbc driver property prepareThreshold" in {
      val factory = new PartitionedPostgisDataStoreFactory()
      val params = Map[String, AnyRef](PartitionedPostgisDataStoreParams.PrepareThreshold.key -> Int.box(-1))
      val options = factory.createConnectionOptions(params.asJava)
      // the datastore key is snake_case but the emitted pgjdbc connection property keeps its own spelling
      options.get("prepareThreshold") must beSome("-1")
      // it's a driver property, not a server GUC, so it must not leak into the '-c' options string
      options.get("options") must beNone
    }

    "not emit prepareThreshold when the parameter is unset" in {
      val factory = new PartitionedPostgisDataStoreFactory()
      val options = factory.createConnectionOptions(Map.empty[String, AnyRef].asJava)
      options.get("prepareThreshold") must beNone
    }

    "route idle_in_transaction_session_timeout to a server GUC, not a driver property" in {
      // sibling param: proves the driver-property vs. '-c <guc>' split is behaving as intended
      val factory = new PartitionedPostgisDataStoreFactory()
      val params =
        Map[String, AnyRef](PartitionedPostgisDataStoreParams.IdleInTransactionTimeout.key -> "2 minutes")
      val options = factory.createConnectionOptions(params.asJava)
      options.get("options") must beSome
      options("options") must contain("-c idle_in_transaction_session_timeout=120000")
      options.get("prepareThreshold") must beNone
    }
  }
}
