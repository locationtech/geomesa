/*
 * Copyright 2015 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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