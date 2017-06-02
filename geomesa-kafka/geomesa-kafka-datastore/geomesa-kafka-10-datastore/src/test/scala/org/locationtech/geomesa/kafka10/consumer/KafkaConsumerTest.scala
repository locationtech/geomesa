/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka10.consumer

import java.io.IOException
import java.util.Properties

import kafka.common.{OffsetAndMetadata, TopicAndPartition}
import kafka.consumer.{ConsumerConfig, ConsumerTimeoutException}
import kafka.message.Message
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.junit.runner.RunWith
import org.locationtech.geomesa.kafka10.consumer.offsets._
import org.locationtech.geomesa.kafka10.{HasEmbeddedKafka, KafkaUtils10}
import org.specs2.mutable.Specification
import org.specs2.runner

import scala.collection.mutable.ArrayBuffer

@RunWith(classOf[runner.JUnitRunner])
class KafkaConsumerTest extends Specification with HasEmbeddedKafka {

  sequential

  // skip embedded kafka tests unless explicitly enabled, they often fail randomly
  skipAllUnless(sys.props.get(SYS_PROP_RUN_TESTS).exists(_.toBoolean))

  def getConsumerConfig(group: String) = {
    val consumerProps = new Properties
    consumerProps.put("group.id", group)
    consumerProps.put(KafkaUtils10.brokerParam, brokerConnect)
    consumerProps.put("zookeeper.connect", zkConnect)
    consumerProps.put("num.consumer.fetchers", "1")
    consumerProps.put("auto.commit.enable", "false")
    consumerProps.put("consumer.timeout.ms", "1000")
    new ConsumerConfig(consumerProps)
  }

  "KafkaConsumer" should {
    val producerProps = new Properties()
    producerProps.put(KafkaUtils10.brokerParam, brokerConnect)
    producerProps.put("retry.backoff.ms", "100")
    //producerProps.put("message.send.max.retries", "20") // we have to bump this up as zk is pretty flaky
    producerProps.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
    producerProps.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")

    def produceMessages(topic: String) = {
      val producer = new KafkaProducer[Array[Byte], Array[Byte]](producerProps)
      for (i <- 0 until 10) {
        producer.send(new ProducerRecord[Array[Byte], Array[Byte]](topic, i.toString.getBytes("UTF-8"), s"test $i".getBytes("UTF-8")))
      }
      producer.close()
    }

    "read messages and shutdown appropriately" >> {
      val topic = "read-1"
      val config = getConsumerConfig(topic)
      produceMessages(topic)
      val consumer = new KafkaConsumer[String, String](topic, config, new StringDecoder, new StringDecoder)
      var stream: KafkaStreamLike[String, String] = null
      try {
        stream = consumer.createMessageStreams(1, EarliestOffset).head
      } catch {
        case a: IOException => println("IOE")
        case b: ConsumerTimeoutException => b.printStackTrace
        case c: Throwable => c.printStackTrace
      } finally {
        val messages = stream.iterator.take(10).toList
        messages must haveLength(10)
        stream.iterator.hasNext must throwA[ConsumerTimeoutException]
        consumer.shutdown()
        stream.iterator.hasNext must beFalse
        for (i <- 0 until 10) {
          messages(i).key() mustEqual i.toString
          messages(i).message() mustEqual s"test $i"
        }
      }
        success
    }

    "balance consumers across threads" >> {
      val topic = "balance"
      val config = getConsumerConfig(topic)
      produceMessages(topic)

      val consumer1 = new KafkaConsumer[String, String](topic, config, new StringDecoder, new StringDecoder)
      val consumer2 = new KafkaConsumer[String, String](topic, config, new StringDecoder, new StringDecoder)

      val messages = ArrayBuffer.empty[String]
      val stream1 = consumer1.createMessageStreams(1, EarliestOffset).head
      val stream2 = consumer2.createMessageStreams(1, EarliestOffset).head

      try {
        while(stream1.iterator.hasNext) {
          messages.append(stream1.iterator.next.key())
        }
      } catch {
        case e: ConsumerTimeoutException => // end of stream
      }

      try {
        while(stream2.iterator.hasNext) {
          messages.append(stream2.iterator.next.key())
        }
      } catch {
        case e: ConsumerTimeoutException => // end of stream
      }

      messages must haveLength(10)

      consumer1.shutdown()
      consumer2.shutdown()

      stream1.iterator.hasNext must beFalse
      stream2.iterator.hasNext must beFalse

      for (i <- 0 until 10) {
        messages(i) mustEqual i.toString
      }

      success
    }

    "read messages from various offsets" >> {

      "by group" >> {
        val topic = "group"
        val config = getConsumerConfig(topic)
        produceMessages(topic)

        // set up the initial group offset
        val offsetManager = new OffsetManager(config)
        offsetManager.commitOffsets(Map(TopicAndPartition(topic, 0) -> OffsetAndMetadata(3)))

        val consumer = new KafkaConsumer[String, String](topic, config, new StringDecoder, new StringDecoder)
        val stream = consumer.createMessageStreams(1, GroupOffset).head
        stream.iterator.hasNext must beTrue
        val message = stream.iterator.next()
        consumer.shutdown()
        message.key() mustEqual "3"
      }

      "by earliest" >> {
        val topic = "earliest"
        val config = getConsumerConfig(topic)
        produceMessages(topic)

        // set up the initial group offset
        val offsetManager = new OffsetManager(config)
        offsetManager.commitOffsets(Map(TopicAndPartition(topic, 0) -> OffsetAndMetadata(3)))

        val consumer = new KafkaConsumer[String, String](topic, config, new StringDecoder, new StringDecoder)
        val stream = consumer.createMessageStreams(1, EarliestOffset).head
        stream.iterator.hasNext must beTrue
        val message = stream.iterator.next()
        consumer.shutdown()
        message.key() mustEqual "0"
      }

      "by latest" >> {
        val topic = "latest"
        val config = getConsumerConfig(topic)
        produceMessages(topic)
        val consumer = new KafkaConsumer[String, String](topic, config, new StringDecoder, new StringDecoder)
        val stream = consumer.createMessageStreams(1, LatestOffset).head
        stream.iterator.hasNext must throwA[ConsumerTimeoutException]
        consumer.shutdown()
        stream.iterator.hasNext must beFalse
      }

      "by binary search" >> {
        val topic = "search"
        val config = getConsumerConfig(topic)
        produceMessages(topic)
        val decoder = new StringDecoder()
        val offset = FindOffset((m: Message) => {
          val bb = Array.ofDim[Byte](m.payload.remaining())
          m.payload.get(bb)
          decoder.fromBytes(bb).substring(5).toInt.compareTo(7)
        })
        val consumer = new KafkaConsumer[String, String](topic, config, new StringDecoder, new StringDecoder)
        val stream = consumer.createMessageStreams(1, offset).head
        stream.iterator.hasNext must beTrue
        val message = stream.iterator.next()
        consumer.shutdown()
        message.key() mustEqual "7"
      }
    }
  }

  step { shutdown() }
}


