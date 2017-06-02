/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka10.consumer

import kafka.consumer.ConsumerConfig
import org.locationtech.geomesa.kafka10.KafkaUtils10

case class Broker(host: String, port: Int) {
  override def toString = s"[$host,$port]"
}

object Broker {

  val defaultPort = 9092

  def apply(broker: String): Broker = {
    val colon = broker.lastIndexOf(':')
    if (colon == -1) {
      Broker(broker, defaultPort)
    } else {
      try {
        Broker(broker.substring(0, colon), broker.substring(colon + 1).toInt)
      } catch {
        case e: Exception => throw new IllegalArgumentException(s"Invalid broker string '$broker'", e)
      }
    }
  }
}

object Brokers {
  def apply(brokers: String): Seq[Broker] = brokers.split(",").map(Broker.apply)
  def apply(config: ConsumerConfig): Seq[Broker] = {
    val brokers : String = config.props.getString(KafkaUtils10.brokerParam)
    apply(brokers)
  }
}

