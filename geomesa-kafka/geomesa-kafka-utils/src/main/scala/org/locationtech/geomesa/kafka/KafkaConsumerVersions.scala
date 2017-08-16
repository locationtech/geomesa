/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka

import java.util.Collections

import org.apache.kafka.clients.consumer.{Consumer, ConsumerRebalanceListener}
import org.apache.kafka.common.TopicPartition

/**
  * Reflection wrapper for KafkaConsumer methods between kafka versions 0.9 and 0.10
  */
object KafkaConsumerVersions {

  private val methods = classOf[Consumer[_, _]].getDeclaredMethods

  def seekToBeginning(consumer: Consumer[_, _], topic: TopicPartition): Unit = _seekToBeginning(consumer, topic)

  def pause(consumer: Consumer[_, _], topic: TopicPartition): Unit = _pause(consumer, topic)

  def resume(consumer: Consumer[_, _], topic: TopicPartition): Unit = _resume(consumer, topic)

  def subscribe(consumer: Consumer[_, _], topic: String): Unit = _subscribe(consumer, topic)

  def subscribe(consumer: Consumer[_, _], topic: String, listener: ConsumerRebalanceListener): Unit =
    _subscribeWithListener(consumer, topic, listener)

  private val _seekToBeginning: (Consumer[_, _], TopicPartition) => Unit = consumerTopicInvocation("seekToBeginning")

  private val _pause: (Consumer[_, _], TopicPartition) => Unit = consumerTopicInvocation("pause")

  private val _resume: (Consumer[_, _], TopicPartition) => Unit = consumerTopicInvocation("resume")

  private val _subscribe: (Consumer[_, _], String) => Unit = {
    val method = methods.find(m => m.getName == "subscribe" && m.getParameterCount == 1).getOrElse {
      throw new NoSuchMethodException(s"Couldn't find Consumer.subscribe method")
    }
    (consumer, topic) => method.invoke(consumer, Collections.singletonList(topic))
  }

  private val _subscribeWithListener: (Consumer[_, _], String, ConsumerRebalanceListener) => Unit = {
    val method = methods.find(m => m.getName == "subscribe" && m.getParameterCount == 2 &&
        m.getParameterTypes.apply(0).isAssignableFrom(classOf[java.util.List[_]])).getOrElse {
      throw new NoSuchMethodException(s"Couldn't find Consumer.subscribe method")
    }
    (consumer, topic, listener) => method.invoke(consumer, Collections.singletonList(topic), listener)
  }

  private def consumerTopicInvocation(name: String): (Consumer[_, _], TopicPartition) => Unit = {
    val method = methods.find(_.getName == name).getOrElse {
      throw new NoSuchMethodException(s"Couldn't find Consumer.$name method")
    }
    val parameterTypes = method.getParameterTypes
    if (parameterTypes.length != 1) {
      throw new NoSuchMethodException(s"Couldn't find Consumer.$name method with correct parameters")
    }
    val binding = method.getParameterTypes.apply(0)

    if (binding == classOf[Array[TopicPartition]]) {
      (consumer, tp) => method.invoke(consumer, Array(tp))
    } else if (binding == classOf[java.util.Collection[TopicPartition]]) {
      (consumer, tp) => method.invoke(consumer, Collections.singletonList(tp))
    } else {
      throw new NoSuchMethodException(s"Couldn't find Consumer.$name method with correct parameters: $method")
    }
  }
}
