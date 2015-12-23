/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.kafka.plugin

import java.util.concurrent.ScheduledThreadPoolExecutor

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.springframework.context.support.ClassPathXmlApplicationContext
import org.springframework.scheduling.concurrent.ScheduledExecutorTask

@RunWith(classOf[JUnitRunner])
class KafkaPluginSpringTest extends Specification {

  sequential

  "Spring" should {

    "Work with the Kafka Processes" in {
      val ctx = new ClassPathXmlApplicationContext("kafkaPluginTestApplicationContext.xml","applicationContext.xml")
      ctx.isActive must beTrue
      ctx.getBean("replayKafkaLayerReaper") must beAnInstanceOf[ReplayKafkaLayerReaperProcess]
      ctx.getBean("replayKafkaDataStoreProcess") must beAnInstanceOf[ReplayKafkaDataStoreProcess]
      ctx.getBean("replayKafkaCleanupTask") must beAnInstanceOf[ScheduledExecutorTask]
      ctx.getBean("replayKafkaCleanupTaskFactory") must beAnInstanceOf[ScheduledThreadPoolExecutor]
    }


  }

}