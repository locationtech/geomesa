/***********************************************************************
<<<<<<< HEAD
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
=======
<<<<<<< HEAD
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
=======
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 1463162d60 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 9f430502b2 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
>>>>>>> dce8c58b44 (GEOMESA-3254 Add Bloop build support)
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.versions

import org.apache.kafka.clients.consumer.{Consumer, ConsumerRebalanceListener, ConsumerRecords, OffsetAndTimestamp}
import org.apache.kafka.common.TopicPartition

import java.time.Duration
import java.util.Collections

/**
  * Reflection wrapper for KafkaConsumer methods between kafka versions 0.9, 0.10, 1.0, 1.1, 2.0, and 2.1
  */
object KafkaConsumerVersions {

  private val methods = classOf[Consumer[_, _]].getDeclaredMethods

  def poll[K, V](consumer: Consumer[K, V], timeout: Duration): ConsumerRecords[K, V] =
    _poll(consumer, timeout).asInstanceOf[ConsumerRecords[K, V]]

  def seekToBeginning(consumer: Consumer[_, _], topic: TopicPartition): Unit = _seekToBeginning(consumer, topic)

  def pause(consumer: Consumer[_, _], topic: TopicPartition): Unit = _pause(consumer, topic)

  def resume(consumer: Consumer[_, _], topic: TopicPartition): Unit = _resume(consumer, topic)

  def subscribe(consumer: Consumer[_, _], topic: String): Unit = _subscribe(consumer, topic)

  def subscribe(consumer: Consumer[_, _], topic: String, listener: ConsumerRebalanceListener): Unit =
    _subscribeWithListener(consumer, topic, listener)

  def beginningOffsets(consumer: Consumer[_, _], topic: String, partitions: Seq[Int]): Map[Int, Long] =
    _beginningOffsets(consumer, topic, partitions)

  def endOffsets(consumer: Consumer[_, _], topic: String, partitions: Seq[Int]): Map[Int, Long] =
    _endOffsets(consumer, topic, partitions)

  def offsetsForTimes(consumer: Consumer[_, _], topic: String, partitions: Seq[Int], time: Long): Map[Int, Long] =
    _offsetsForTimes(consumer, topic, partitions, time)

  // this will return ConsumerRecords, but the signature is AnyRef to avoid a second .asInstanceOf
  private val _poll: (Consumer[_, _], Duration) => AnyRef = {
    val polls = methods.filter(m => m.getName == "poll" && m.getParameterCount == 1)

    def fromDuration: Option[(Consumer[_, _], Duration) => AnyRef] = polls.collectFirst {
      case m if m.getParameterTypes.apply(0) == classOf[Duration] =>
        (c: Consumer[_, _], d: Duration) => tryInvocation(m.invoke(c, d))
    }
    def fromLong: Option[(Consumer[_, _], Duration) => AnyRef] = polls.collectFirst {
      case m if m.getParameterTypes.apply(0) == java.lang.Long.TYPE =>
        (c: Consumer[_, _], d: Duration) => tryInvocation(m.invoke(c, Long.box(d.toMillis)))
    }

    fromDuration.orElse(fromLong).getOrElse {
      throw new NoSuchMethodException(s"Couldn't find Consumer.poll method")
    }
  }

  private val _seekToBeginning: (Consumer[_, _], TopicPartition) => Unit = consumerTopicInvocation("seekToBeginning")

  private val _pause: (Consumer[_, _], TopicPartition) => Unit = consumerTopicInvocation("pause")

  private val _resume: (Consumer[_, _], TopicPartition) => Unit = consumerTopicInvocation("resume")

  private val _subscribe: (Consumer[_, _], String) => Unit = {
    val method = methods.find(m => m.getName == "subscribe" && m.getParameterCount == 1 &&
        m.getParameterTypes.apply(0).isAssignableFrom(classOf[java.util.List[_]])).getOrElse {
      throw new NoSuchMethodException(s"Couldn't find Consumer.subscribe method")
    }
    (consumer, topic) => tryInvocation(method.invoke(consumer, Collections.singletonList(topic)))
  }

  private val _subscribeWithListener: (Consumer[_, _], String, ConsumerRebalanceListener) => Unit = {
    val method = methods.find(m => m.getName == "subscribe" && m.getParameterCount == 2 &&
        m.getParameterTypes.apply(0).isAssignableFrom(classOf[java.util.List[_]]) &&
        m.getParameterTypes.apply(1).isAssignableFrom(classOf[ConsumerRebalanceListener])).getOrElse {
      throw new NoSuchMethodException(s"Couldn't find Consumer.subscribe method")
    }
    (consumer, topic, listener) => tryInvocation(method.invoke(consumer, Collections.singletonList(topic), listener))
  }

  private val _beginningOffsets: (Consumer[_, _], String, Seq[Int]) => Map[Int, Long] = {
    import scala.collection.JavaConverters._
    // note: this method doesn't exist in 0.9, so may be null
    val method = methods.find(m => m.getName == "beginningOffsets" && m.getParameterCount == 1).orNull
    (consumer, topic, partitions) => {
      if (method == null) {
        throw new NoSuchMethodException(s"Couldn't find Consumer.beginningOffsets method")
      }
      val topicAndPartitions = new java.util.ArrayList[TopicPartition](partitions.length)
      partitions.foreach(p => topicAndPartitions.add(new TopicPartition(topic, p)))
      val offsets = tryInvocation(method.invoke(consumer, topicAndPartitions).asInstanceOf[java.util.Map[TopicPartition, Long]])
      val result = Map.newBuilder[Int, Long]
      result.sizeHint(offsets.size())
      offsets.asScala.foreach { case (tp, o) => result += (tp.partition -> o) }
      result.result()
    }
  }

  private val _endOffsets: (Consumer[_, _], String, Seq[Int]) => Map[Int, Long] = {
    import scala.collection.JavaConverters._
    // note: this method doesn't exist in 0.9, so may be null
    val method = methods.find(m => m.getName == "endOffsets" && m.getParameterCount == 1).orNull
    (consumer, topic, partitions) => {
      if (method == null) {
        throw new NoSuchMethodException(s"Couldn't find Consumer.endOffsets method")
      }
      val topicAndPartitions = new java.util.ArrayList[TopicPartition](partitions.length)
      partitions.foreach(p => topicAndPartitions.add(new TopicPartition(topic, p)))
      val offsets = tryInvocation(method.invoke(consumer, topicAndPartitions).asInstanceOf[java.util.Map[TopicPartition, Long]])
      val result = Map.newBuilder[Int, Long]
      result.sizeHint(offsets.size())
      offsets.asScala.foreach { case (tp, o) => result += (tp.partition -> o) }
      result.result()
    }
  }

  private val _offsetsForTimes: (Consumer[_, _], String, Seq[Int], Long) => Map[Int, Long] = {
    import scala.collection.JavaConverters._
    // note: this method doesn't exist until 0.10.1, so may be null
    val method = methods.find(m => m.getName == "offsetsForTimes" && m.getParameterCount == 1).orNull
    (consumer, topic, partitions, time) => {
      if (method == null) {
        throw new NoSuchMethodException(s"Couldn't find Consumer.offsetsForTimes method")
      }
      val timestamps = new java.util.HashMap[TopicPartition, java.lang.Long](partitions.length)
      partitions.foreach(p => timestamps.put(new TopicPartition(topic, p), Long.box(time)))
      val offsets = tryInvocation(method.invoke(consumer, timestamps).asInstanceOf[java.util.Map[TopicPartition, OffsetAndTimestamp]])
      val result = Map.newBuilder[Int, Long]
      result.sizeHint(offsets.size())
      offsets.asScala.foreach { case (tp, o) => if (o != null) { result += (tp.partition -> o.offset()) } }
      result.result()
    }
  }

  private def consumerTopicInvocation(name: String): (Consumer[_, _], TopicPartition) => Unit = {
    val method = methods.find(m => m.getName == name && m.getParameterCount == 1).getOrElse {
      throw new NoSuchMethodException(s"Couldn't find Consumer.$name method")
    }
    val binding = method.getParameterTypes.apply(0)

    if (binding == classOf[Array[TopicPartition]]) {
      (consumer, tp) => tryInvocation(method.invoke(consumer, Array(tp)))
    } else if (binding == classOf[java.util.Collection[TopicPartition]]) {
      (consumer, tp) => tryInvocation(method.invoke(consumer, Collections.singletonList(tp)))
    } else {
      throw new NoSuchMethodException(s"Couldn't find Consumer.$name method with correct parameters: $method")
    }
  }
}
