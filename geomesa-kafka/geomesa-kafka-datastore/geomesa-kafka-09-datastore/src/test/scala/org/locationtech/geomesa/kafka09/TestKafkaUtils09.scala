/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka09

import java.util.Properties

import kafka.utils.TestUtils

class TestKafkaUtils09{
  def createBrokerConfig(nodeId: Int, zkConnect: String): Properties = TestUtils.createBrokerConfig(nodeId, zkConnect)
  def choosePort: Int = TestUtils.RandomPort
}
