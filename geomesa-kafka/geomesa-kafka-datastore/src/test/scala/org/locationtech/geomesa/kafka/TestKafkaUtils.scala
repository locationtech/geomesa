/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.kafka

import java.util.{Properties, ServiceLoader}

import com.typesafe.scalalogging.LazyLogging
import kafka.server.{KafkaConfig, KafkaServer}
import kafka.utils.TestUtils

trait TestKafkaUtils {
  def createBrokerConfig(nodeId: Int, zkConnect: String): Properties
  def choosePort: Int
  def createServer(props: Properties): KafkaServer
}

object TestKafkaUtilsLoader extends LazyLogging {
  lazy val testKafkaUtils: TestKafkaUtils = {
    val tkuIter = ServiceLoader.load(classOf[TestKafkaUtils]).iterator()
    if (tkuIter.hasNext) {
      val first = tkuIter.next()
      if (tkuIter.hasNext) {
        logger.warn(s"Multiple geomesa TestKafkaUtils found.  Should only have one. Using the first: '$first'")
      }
      first
    } else {
      logger.debug(s"No geomesa TestKafkaUtils found.  Using default one for 0.8.")
      TestKafkaUtils08
    }
  }
}

object TestKafkaUtils08 extends TestKafkaUtils {
  override def createBrokerConfig(nodeId: Int, zkConnect: String): Properties = TestUtils.createBrokerConfig(nodeId)
  override def choosePort: Int = TestUtils.choosePort()
  override def createServer(props: Properties): KafkaServer = TestUtils.createServer(new KafkaConfig(props))
}
