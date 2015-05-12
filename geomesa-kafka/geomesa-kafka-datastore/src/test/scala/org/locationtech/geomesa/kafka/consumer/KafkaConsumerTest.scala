/*
 * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.kafka.consumer

import java.util.Properties

import kafka.consumer.ConsumerConfig
import kafka.serializer.StringDecoder
import org.junit.runner.RunWith
import org.locationtech.geomesa.kafka.consumer.offsets.{EarliestOffset, GroupOffset, FindOffset}
import org.specs2.mutable.Specification
import org.specs2.runner

@RunWith(classOf[runner.JUnitRunner])
class KafkaConsumerTest extends Specification {

  // TODO use mock kafka

  "KafkaConsumer" should {
    "read messages" >> {
      skipped("integration")
      val topic = "test"

      val offset = EarliestOffset
      //      val offset = LatestOffset
      //      val offset = GroupOffset
      //      val offset = DateOffset(1430339772974L) // test 3
      //      val offset = DateOffset(1430339784576L) // test 5
      //      val decoder = new StringDecoder()
      //      val offset = FindOffset((m: Message) => {
      //        val bb = Array.ofDim[Byte](m.payload.remaining())
      //        m.payload.get(bb)
      //        decoder.fromBytes(bb).substring(5).toInt.compareTo(3)
      //      })
      //      val offset = FindOffset(new StringDecoder(), (m: String) => m.substring(5).toInt.compareTo(3))

      val numFetchersPerConsumer = 1
      val numConsumers = 2
      val streamsPerConsumer = 1

      val props = new Properties
      props.put("group.id", "mygroup")
      props.put("metadata.broker.list", "kafka1:9092,kafka1:9093,kafka1:9094")
      props.put("zookeeper.connect", "zoo1,zoo2,zoo3")
      props.put("num.consumer.fetchers", numFetchersPerConsumer.toString)
      val config = new ConsumerConfig(props)

      val consumers = for (i <- 0 until numConsumers) yield {
        new KafkaConsumer(topic, config, new StringDecoder, new StringDecoder)
      }

      val streamReaders = consumers.flatMap { c =>
        val streams = c.createMessageStreams(streamsPerConsumer, offset)
        val readers = (0 until streamsPerConsumer).map { i =>
          new Thread(new Runnable() {
            override def run() = {
              val iter = streams(i).iterator
              while (iter.hasNext) {
                val mam = iter.next()
                println(s"[${mam.offset}] ${mam.message()}")
              }
            }
          })
        }
        readers.foreach(_.start)
        Thread.sleep(500)
        readers
      }

      try {
        while(true) { Thread.sleep(10000) }
      } catch {
        case e: InterruptedException => println("interrupted")
      }

      consumers.foreach(_.shutdown())
      streamReaders.foreach(_.join)

      success
    }
  }
}


