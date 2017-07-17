/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.lambda.stream.kafka

import java.util.Collections

import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.common.TopicPartition

// use reflection to work with kafak 0.9 or 0.10
object KafkaConsumerVersions {

  val seekToBeginning: (Consumer[_, _], TopicPartition) => Unit = consumerTopicInvocation("seekToBeginning")

  val pause: (Consumer[_, _], TopicPartition) => Unit = consumerTopicInvocation("pause")

  val resume: (Consumer[_, _], TopicPartition) => Unit = consumerTopicInvocation("resume")

  private def consumerTopicInvocation(name: String): (Consumer[_, _], TopicPartition) => Unit = {
    val method = classOf[Consumer[_, _]].getDeclaredMethods.find(_.getName == name).getOrElse {
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
