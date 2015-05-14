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
package org.locationtech.geomesa.kafka

import org.junit.runner.RunWith
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ReplayKafkaConsumerFeatureSourceTest extends Specification with Mockito with SimpleFeatureMatchers {

  import KafkaConsumerTestData._

  val topic = "testTopic"
  val mockKafka = new MockKafka
  val consumerFactory = mockKafka.kafkaConsumerFactory

  sendMessages()

  "feature source" should {

    val entry = null

    val replayConfig = ReplayConfig(10000L, 15000L, 1000L)

    "read messages from kafka" >> {
      val fs = new ReplayKafkaConsumerFeatureSource(entry, sft, topic, consumerFactory, replayConfig)
      val expected = messages.dropWhile(_.timestamp.isBefore(9000L))
                             .takeWhile(_.timestamp.isBefore(15001L))
                             .reverse.toArray

      fs.messages.toSeq must containGeoMessages(expected)
    }
  }

  def sendMessages(): Unit = {
    val encoder = new KafkaGeoMessageEncoder(sft)
    messages.foreach(msg => mockKafka.send(encoder.encodeMessage(topic, msg)))
  }
}
