/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka

import org.apache.kafka.clients.producer.Producer

import java.io.Closeable

package object data {

  class LazyProducer(create: => Producer[Array[Byte], Array[Byte]]) extends Closeable {

    @volatile
    private var initialized = false

    lazy val producer: Producer[Array[Byte], Array[Byte]] = {
      initialized = true
      create
    }

    override def close(): Unit = {
      if (initialized) {
        producer.close()
      }
    }
  }
}
