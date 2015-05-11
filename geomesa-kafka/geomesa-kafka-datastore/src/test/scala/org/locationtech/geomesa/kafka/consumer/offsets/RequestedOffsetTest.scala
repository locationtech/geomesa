/*
 * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.kafka.consumer.offsets

import java.util.Properties

import com.typesafe.config.{ConfigFactory, Config}
import kafka.common.{OffsetAndMetadata, TopicAndPartition}
import kafka.consumer.ConsumerConfig
import kafka.message.Message
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import kafka.serializer.StringDecoder
import org.junit.runner.RunWith
import org.locationtech.geomesa.kafka.HasEmbeddedZookeeper
import org.specs2.mutable.Specification
import org.specs2.runner

@RunWith(classOf[runner.JUnitRunner])
class RequestedOffsetTest extends Specification {

  "RequestedOffset" should {
    "be configurable via typesafe config" >> {
      "for earliest" >> {
        val conf = ConfigFactory.parseString("{ offset = \"earliest\" }")
        val offset = RequestedOffset(conf)
        offset must beSome(EarliestOffset)
      }
      "for latest" >> {
        val conf = ConfigFactory.parseString("{ offset = \"latest\" }")
        val offset = RequestedOffset(conf)
        offset must beSome(LatestOffset)
      }
      "for empty" >> {
        val conf = ConfigFactory.parseString("{}")
        val offset = RequestedOffset(conf)
        offset must beNone
      }
      "for others" >> {
        val conf = ConfigFactory.parseString("{ offset = \"bogus\" }")
        RequestedOffset(conf) must throwA[IllegalArgumentException]
      }
    }
  }
}


