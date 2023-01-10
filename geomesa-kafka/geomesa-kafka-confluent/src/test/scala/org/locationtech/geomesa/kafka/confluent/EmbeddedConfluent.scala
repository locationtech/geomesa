/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.confluent

import io.confluent.kafka.schemaregistry.RestApp
import org.apache.curator.test.InstanceSpec
import org.locationtech.geomesa.kafka.EmbeddedKafka

class EmbeddedConfluent extends EmbeddedKafka {

  private val schemaRegistryApp =
    new RestApp(InstanceSpec.getRandomPort, zookeepers, brokers, "_schemas", "NONE", true, null)
  schemaRegistryApp.start()

  val schemaRegistryUrl: String = schemaRegistryApp.restConnect

  override def close(): Unit = {
    try { schemaRegistryApp.stop() } catch { case _: Throwable => }
    super.close()
  }
}
