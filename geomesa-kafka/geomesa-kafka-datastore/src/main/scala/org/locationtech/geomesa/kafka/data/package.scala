/***********************************************************************
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka

import org.apache.kafka.clients.producer.Producer
import org.locationtech.geomesa.utils.concurrent.LazyCloseable

package object data {

  class LazyProducer(create: => Producer[Array[Byte], Array[Byte]])
      extends LazyCloseable[Producer[Array[Byte], Array[Byte]]](create)
}
