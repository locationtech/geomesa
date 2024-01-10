/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.confluent

import io.confluent.kafka.schemaregistry.RestApp
import org.apache.curator.test.InstanceSpec
import org.locationtech.geomesa.kafka.KafkaContainerTest

class ConfluentContainerTest extends KafkaContainerTest {

  private var schemaRegistryApp: RestApp = _

  lazy val schemaRegistryUrl: String = schemaRegistryApp.restConnect

  override def beforeAll(): Unit = { super.beforeAll()
    schemaRegistryApp = new RestApp(InstanceSpec.getRandomPort, zookeepers, brokers, "_schemas", "NONE", true, null)
    schemaRegistryApp.start()
  }

  override def afterAll(): Unit = {
    try {
      if (schemaRegistryApp != null) {
        schemaRegistryApp.stop()
      }
    } finally {
      super.afterAll()
    }
  }
}
