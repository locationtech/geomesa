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
package org.locationtech.geomesa.kafka.consumer

import java.util.Properties

import kafka.consumer._
import kafka.serializer.{Decoder, DefaultDecoder}
import org.apache.commons.lang3.RandomStringUtils
import org.locationtech.geomesa.kafka.consumer.offsets.OffsetManager

import scala.collection.JavaConverters._
import scala.collection.Seq
import scala.language.implicitConversions

/** @param brokers a comma separated list of broker host names and ports
  * @param zookeepers the zookeeper connection string
  */
class KafkaConsumerFactory(brokers: String, zookeepers: String) {

  import KafkaConsumerFactory._

  private val props = {
    val groupId = RandomStringUtils.randomAlphanumeric(5)

    val props = new Properties()
    props.put("zookeeper.connect", zookeepers)
    props.put("metadata.broker.list", brokers)
    props.put("group.id", groupId)
    props.put("zookeeper.session.timeout.ms", "2000")
    props.put("zookeeper.sync.time.ms", "1000")
    props.put("auto.commit.interval.ms", "1000")

    props
  }

  /** The [[ConsumerConfig]] containing the given ``zookeepers``, a random Group ID and other fixed
    * properties.
    */
  val config: ConsumerConfig  = new ConsumerConfig(props)

  /** @return a new high level [[ConsumerConfig]]
    */
  def consumerConnector: ConsumerConnector = Consumer.create(config)

  /** @return a new low level [[KafkaConsumer]]
    */
  def kafkaConsumer(topic: String, extraConfig: Map[String, String] = Map.empty): RawKafkaConsumer = {

    val fullConfig = config(extraConfig)

    val decoder: DefaultDecoder = new DefaultDecoder(null)
    new KafkaConsumer[Array[Byte], Array[Byte]](topic, fullConfig, decoder, decoder)
  }

  /** @return a new [[OffsetManager]]
   */
  def offsetManager: OffsetManager = new OffsetManager(config)

  def messageStreams(topic: String, numStreams: Int = 1): Seq[RawKafkaStream] = {

    val client = consumerConnector
    val whiteList = new Whitelist(topic)
    val decoder = KafkaConsumerFactory.defaultDecoder
    client.createMessageStreamsByFilter(whiteList, 1, decoder, decoder)
  }

  private def config(extraConfig: Map[String, String]): ConsumerConfig = {
    if (extraConfig.isEmpty) {
      config
    } else {
      val allProps = new Properties()
      allProps.putAll(props)
      allProps.putAll(extraConfig.asJava)
      new ConsumerConfig(allProps)
    }
  }
}

object KafkaConsumerFactory {

  type RawKafkaConsumer = KafkaConsumer[Array[Byte], Array[Byte]]
  type RawKafkaStream = KafkaStream[Array[Byte], Array[Byte]]

  val defaultDecoder: Decoder[Array[Byte]] = new DefaultDecoder(null)
}
