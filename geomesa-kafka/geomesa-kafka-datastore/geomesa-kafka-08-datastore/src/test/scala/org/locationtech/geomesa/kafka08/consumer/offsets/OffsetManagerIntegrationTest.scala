/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka08.consumer.offsets

import java.util.Properties

import kafka.common.{OffsetAndMetadata, TopicAndPartition}
import kafka.consumer.ConsumerConfig
import kafka.message.Message
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import kafka.serializer.StringDecoder
import org.junit.runner.RunWith
import org.locationtech.geomesa.kafka08.{HasEmbeddedKafka, HasEmbeddedZookeeper, KafkaUtils08}
import org.specs2.mutable.Specification
import org.specs2.runner

@RunWith(classOf[runner.JUnitRunner])
class OffsetManagerIntegrationTest extends Specification with HasEmbeddedKafka {

  sequential // this doesn't really need to be sequential, but we're trying to reduce zk load

  // skip embedded kafka tests unless explicitly enabled, they often fail randomly
  skipAllUnless(sys.props.get(SYS_PROP_RUN_TESTS).exists(_.toBoolean))

  val props = new Properties
  props.put("group.id", "mygroup")
  props.put(KafkaUtils08.brokerParam, brokerConnect)
  props.put("zookeeper.connect", zkConnect)
  val config = new ConsumerConfig(props)
  val offsetManager = new OffsetManager(config)

  val topic = "test"

  step {
    val producerProps = new Properties()
    producerProps.put(KafkaUtils08.brokerParam, brokerConnect)
    producerProps.put("serializer.class", "kafka.serializer.DefaultEncoder")
    val producer = new Producer[Array[Byte], Array[Byte]](new ProducerConfig(producerProps))
    for (i <- 0 until 10) {
      producer.send(new KeyedMessage(topic, i.toString.getBytes("UTF-8"), s"test $i".getBytes("UTF-8")))
    }
    producer.close()
  }

  "OffsetManager" should {
    "find offsets by number" in {
      offsetManager.getOffsets(topic, SpecificOffset(1)) mustEqual Map(TopicAndPartition(topic, 0) -> 1)
    }
    "find offsets by earliest" in {
      offsetManager.getOffsets(topic, EarliestOffset) mustEqual Map(TopicAndPartition(topic, 0) -> 0)
    }
    "find offsets by latest" in {
      offsetManager.getOffsets(topic, LatestOffset) mustEqual Map(TopicAndPartition(topic, 0) -> 10)
    }
    "find offsets by group" in {
      offsetManager.commitOffsets(Map(TopicAndPartition(topic, 0) -> OffsetAndMetadata(5)))
      offsetManager.getOffsets(topic, GroupOffset) mustEqual Map(TopicAndPartition(topic, 0) -> 5)
    }
    "find offsets by binary search" in {
      val decoder = new StringDecoder()
      val offset = FindOffset((m: Message) => {
        val bb = Array.ofDim[Byte](m.payload.remaining())
        m.payload.get(bb)
        decoder.fromBytes(bb).substring(5).toInt.compareTo(7)
      })
      offsetManager.getOffsets(topic, offset) mustEqual Map(TopicAndPartition(topic, 0) -> 7)
    }
  }

  step { shutdown() }
}


