/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.kafka

import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig}
import org.apache.kafka.clients.producer.Producer
import org.locationtech.geomesa.kafka.data.KafkaDataStore.KafkaDataStoreConfig
import org.locationtech.geomesa.utils.concurrent.LazyCloseable
import org.locationtech.geomesa.utils.io.WithClose

import java.util.Properties

package object data {

  val DefaultCatalog: String = "geomesa-catalog"
  val DefaultZkPath: String = "geomesa/ds/kafka"

  class LazyProducer(create: => Producer[Array[Byte], Array[Byte]])
      extends LazyCloseable[Producer[Array[Byte], Array[Byte]]](create)

  private[data] def adminClientOp[V](config: KafkaDataStoreConfig)(fn: AdminClient => V): V = {
    val props = new Properties()
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.brokers)
    config.consumers.properties.foreach { case (k, v) => props.put(k, v) }
    config.producers.properties.foreach { case (k, v) => props.put(k, v) }
    WithClose(AdminClient.create(props)) { admin => fn(admin) }
  }
}
