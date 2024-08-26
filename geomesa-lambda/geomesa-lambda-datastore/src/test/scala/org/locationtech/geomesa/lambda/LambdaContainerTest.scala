/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.lambda

import org.locationtech.geomesa.accumulo.AccumuloContainer
import org.locationtech.geomesa.kafka.KafkaContainerTest
import org.locationtech.geomesa.lambda.LambdaContainerTest.TestClock

import java.time.{Clock, Instant, ZoneId, ZoneOffset}

class LambdaContainerTest extends KafkaContainerTest {

  val sftName = getClass.getSimpleName

  val clock = new TestClock()
  val offsetManager = new InMemoryOffsetManager

  lazy val dsParams = Map(
    "lambda.accumulo.instance.name" -> AccumuloContainer.instanceName,
    "lambda.accumulo.zookeepers"    -> AccumuloContainer.zookeepers,
    "lambda.accumulo.user"          -> AccumuloContainer.user,
    "lambda.accumulo.password"      -> AccumuloContainer.password,
    // note the table needs to be different to prevent testing errors
    "lambda.accumulo.catalog"       -> sftName,
    "lambda.kafka.brokers"          -> brokers,
    "lambda.kafka.zookeepers"       -> zookeepers,
    "lambda.kafka.partitions"       -> 2,
    "lambda.expiry"                 -> "100ms",
    "lambda.clock"                  -> clock,
    "lambda.offset-manager"         -> offsetManager
  )
}

object LambdaContainerTest {

  class TestClock extends Clock with java.io.Serializable {

    var tick: Long = 0L

    override def withZone(zone: ZoneId): Clock = null
    override def getZone: ZoneId = ZoneOffset.UTC
    override def instant(): Instant = Instant.ofEpochMilli(tick)
  }
}
