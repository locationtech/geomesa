/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka09.consumer.offsets

import java.util.Properties
import java.util.concurrent.atomic.AtomicLong

import kafka.common.TopicAndPartition
import kafka.consumer.{ConsumerConfig, SimpleConsumer}
import kafka.message.{ByteBufferMessageSet, Message, NoCompressionCodec}
import org.junit.runner.RunWith
import org.locationtech.geomesa.kafka09.consumer.WrappedConsumer
import org.locationtech.geomesa.kafka09.consumer.offsets.FindOffset.MessagePredicate
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.util.{Failure, Success, Try}

@RunWith(classOf[JUnitRunner])
class OffsetTest extends Specification with Mockito {

  import OffsetTest._

  "OffsetManager" should {

    val properties = new Properties()
    properties.put("zookeeper.connect", "mockZoo")
    properties.put("group.id", "offsetTest")

    val config = new ConsumerConfig(properties)

    val consumer = mock[WrappedConsumer]
    consumer.config returns config

    val tap = new TopicAndPartition("offsetTest", 0)
    consumer.tap returns tap

    def keys(range: Range): Array[Byte] = range.map(_.toByte).toArray

    def offsetManager(messagesPerSet: Int, keys: Array[Byte]) =
      new OffsetManagerWithTestFetch(config, messagesPerSet, keys)

    
    "be able to binary search for a MessagePredicate" >> {

      "when start equal end" >> {
        val om = offsetManager(3, keys(0 until 15))
        val predicate = findKey(7)
        val bounds = (11L, 11L)

        val result = om.binaryOffsetSearch(consumer, predicate, bounds)
        result must beOffset(11)
        om.fetchCount mustEqual 0
      }

      "when the target is the head of the message set (i.e. the midpoint of the range)" >> {
        val om = offsetManager(3, keys(0 until 15))
        val predicate = findKey(7)
        val bounds = (0L, 15L)

        val result = om.binaryOffsetSearch(consumer, predicate, bounds)
        result must beOffset(7)
        om.fetchCount mustEqual 1
      }

      "when the target is before the head of the message set" >> {

        "and the search is exhausted" >> {
          val om = offsetManager(3, keys(0 until 16 by 2))
          val predicate = findKey(7)
          val bounds = (4L, 5L)

          val result = om.binaryOffsetSearch(consumer, predicate, bounds)
          result must beOffset(4)
          om.fetchCount mustEqual 1
        }

        "and the search is not exhausted" >> {
          val om = offsetManager(3, keys(0 until 30 by 2))
          val predicate = findKey(8)
          val bounds = (0L, 15L)

          val result = om.binaryOffsetSearch(consumer, predicate, bounds)
          result must beOffset(4)
          om.fetchCount mustEqual 2 // in message set from second fetch
        }
      }

      "when the target is the last of the message set" >> {
        val om = offsetManager(3, keys (0 until 15))
        val predicate = findKey(9)
        val bounds = (0L, 15L)

        val result = om.binaryOffsetSearch(consumer, predicate, bounds)
        result must beOffset(9)
        om.fetchCount mustEqual 1
      }
      
      "when the target is after the last of the message set" >> {

        "and the search is exhausted" >> {
          val om = offsetManager(4, keys(0 until 16 by 2))
          val predicate = findKey(11)
          val bounds = (4L, 6L)

          val result = om.binaryOffsetSearch(consumer, predicate, bounds)
          result must beOffset(6)
          om.fetchCount mustEqual 1
        }

        "and the search is not exhausted" >> {
          val om = offsetManager(3, keys(0 until 30 by 2))
          val predicate = findKey(26)
          val bounds = (4L, 15L)

          val result = om.binaryOffsetSearch(consumer, predicate, bounds)
          result must beOffset(13)
          om.fetchCount mustEqual 2 // in message set from second fetch
        }
      }

      "when the target is in the message set" >> {
        val om = offsetManager(3, keys(0 until 15))
        val predicate = findKey(11)
        val bounds = (10L, 12L)

        val result = om.binaryOffsetSearch(consumer, predicate, bounds)
        result must beOffset(11)
        om.fetchCount mustEqual 1
      }

      "when the target is bounded by the message set" >> {
        val om = offsetManager(3, keys(0 until 30 by 2))
        val predicate = findKey(11)
        val bounds = (5L, 6L)

        val result = om.binaryOffsetSearch(consumer, predicate, bounds)
        result must beOffset(6)
        om.fetchCount mustEqual 1
      }

      "when no messages are returned" >> {
        val om = offsetManager(3, Array.empty[Byte])
        val predicate = findKey(5)
        val bounds = (0L, 11L)

        val result = om.binaryOffsetSearch(consumer, predicate, bounds)
        result must beNone
        om.fetchCount mustEqual 1
      }

      "when fetch fails" >> {
        val om = new OffsetManagerWithFailingFetch(config)
        val predicate = findKey(5)
        val bounds = (0L, 11L)

        om.binaryOffsetSearch(consumer, predicate, bounds) must throwA[Exception]
      }
    }

    "be able to trim MessageSet" >> {

      def messages(range: Range) = messageSet(range.head, keys(range).map(toMessage))

      "when end is after the message set" >> {
        val msgs = messages(5 to 7)
        val end = 10

        val om = new OffsetManager(config)
        val result = om.trim(msgs, end)

        result.map(_.offset) mustEqual Array(5, 6, 7)
      }

      "when end one more than the last message offset in the set" >> {
        val msgs = messages(5 to 7)
        val end = 8

        val om = new OffsetManager(config)
        val result = om.trim(msgs, end)

        result.map(_.offset) mustEqual Array(5, 6, 7)
      }

      "when end is the last message offset in the set" >> {
        val msgs = messages(5 to 7)
        val end = 7

        val om = new OffsetManager(config)
        val result = om.trim(msgs, end)

        // end is exclusive so 7 must be trimmed
        result.map(_.offset) mustEqual Array(5, 6)
      }

      "when end is within the message set" >> {
        val msgs = messages(5 to 7)
        val end = 6

        val om = new OffsetManager(config)
        val result = om.trim(msgs, end)

        result.map(_.offset) mustEqual Array(5)
      }

      "when end is the first message offset in the set" >> {
        val msgs = messages(5 to 7)
        val end = 5

        val om = new OffsetManager(config)
        val result = om.trim(msgs, end)

        result.map(_.offset) mustEqual Array.empty[Long]
      }

      "when end is before the first message offset in the set" >> {
        val msgs = messages(5 to 7)
        val end = 4

        val om = new OffsetManager(config)
        val result = om.trim(msgs, end)

        result.map(_.offset) mustEqual Array.empty[Long]
      }
    }
  }

  def findKey(target: Int): MessagePredicate = msg => msg.key.get() - target

  def beOffset(o: Long) = beSome(o)
}

object OffsetTest {

  def toMessage(key: Byte): Message = new Message(Array.empty[Byte], Array(key))

  def messageSet(offset: Long, msgs: Array[Message]): ByteBufferMessageSet = {
    val offsetCounter = new AtomicLong(offset)
    new ByteBufferMessageSet(NoCompressionCodec, offsetCounter, msgs: _*)
  }
}

class OffsetManagerWithTestFetch(config: ConsumerConfig, messagesPerSet: Int, keys: Array[Byte])
  extends OffsetManager(config) {

  import OffsetTest._

  var fetchCount = 0

  override def fetch(consumer: SimpleConsumer,
                     topic: String,
                     partition: Int,
                     offset: Long,
                     maxBytes: Int): Try[ByteBufferMessageSet] = {

    fetchCount += 1

    val msgs = keys.slice(offset.toInt, offset.toInt + messagesPerSet).map(toMessage)
    Success(messageSet(offset, msgs))
  }
}

class OffsetManagerWithFailingFetch(config: ConsumerConfig)
  extends OffsetManager(config) {

  var fetchCount = 0

  override def fetch(consumer: SimpleConsumer,
                     topic: String,
                     partition: Int,
                     offset: Long,
                     maxBytes: Int): Try[ByteBufferMessageSet] = {

    fetchCount += 1

    new Failure(new RuntimeException())
  }
}