/*
 * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.kafka.consumer.offsets

import java.util.Properties

import kafka.consumer.ConsumerConfig
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner

@RunWith(classOf[runner.JUnitRunner])
class OffsetManagerTest extends Specification {

  // TODO use mock kafka

  "OffsetManager" should {
    "find offsets" >> {
      skipped("integration")
      val props = new Properties
      props.put("group.id", "mygroup")
      props.put("metadata.broker.list", "kafka1:9092,kafka1:9093,kafka1:9094")
      props.put("zookeeper.connect", "zoo1,zoo2,zoo3")
//      props.put("num.consumer.fetchers", "3")
      val config = new ConsumerConfig(props)
      val offsetManager = new OffsetManager(config)

      val topic = "consume-1"

      val when = GroupOffset
//      val when = EarliestOffset
//      val when = LatestOffset
//      val when = LastReadOffset("mygroup")
//      val when = DateOffset(1429206515955L) // test 9
//      1429208754000 8
//      1429211031093 19
//      val when = DateOffset(1429211031092L) // test 16
//      val decoder = new StringDecoder()
//      val when = FindOffset((m) => {
//        val bb = Array.ofDim[Byte](m.payload.remaining())
//        m.payload.get(bb)
//        decoder.fromBytes(bb).substring(5).toInt.compareTo(28)
//      })
//      val when = DateOffset(1429207361392L) // test 18

      println(offsetManager.getOffsets(topic, when))

      success
    }
  }
}


