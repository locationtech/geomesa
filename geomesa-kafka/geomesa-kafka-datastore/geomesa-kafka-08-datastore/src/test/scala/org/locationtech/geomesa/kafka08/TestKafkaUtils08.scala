/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka08

import java.util.Properties

import kafka.utils.TestUtils

class TestKafkaUtils08{
  def createBrokerConfig(nodeId: Int, zkConnect: String): Properties = TestUtils.createBrokerConfig(nodeId)
  def choosePort: Int = TestUtils.choosePort()
}
