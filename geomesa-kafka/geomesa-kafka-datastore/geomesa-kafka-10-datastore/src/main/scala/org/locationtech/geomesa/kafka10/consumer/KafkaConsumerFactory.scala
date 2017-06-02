/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka10.consumer

import java.util.Properties

import kafka.consumer._
import kafka.serializer.{Decoder, DefaultDecoder}
import org.apache.commons.lang3.RandomStringUtils
import org.locationtech.geomesa.kafka10.KafkaUtils10
import org.locationtech.geomesa.kafka10.consumer.offsets.OffsetManager

import scala.collection.JavaConverters._
import scala.collection.Seq
import scala.language.implicitConversions

/** @param brokers a comma separated list of broker host names and ports
  * @param zookeepers the zookeeper connection string
  * @param autoOffsetReset what offset to reset to when there is no initial offset in ZooKeeper 
  * ("largest" or "smallest")
  */
class KafkaConsumerFactory(brokers: String, zookeepers: String, autoOffsetReset: String) {

  import KafkaConsumerFactory._

  private val props = {
    val groupId = RandomStringUtils.randomAlphanumeric(5)

    val props = new Properties()
    props.put("zookeeper.connect", zookeepers)
    props.put(KafkaUtils10.brokerParam, brokers)
    props.put("group.id", groupId)
    props.put("zookeeper.session.timeout.ms", "2000")
    props.put("zookeeper.sync.time.ms", "1000")
    props.put("auto.commit.interval.ms", "1000")
    props.put("auto.offset.reset", autoOffsetReset)

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

  def messageStreams(topic: String, numStreams: Int = 1): (ConsumerConnector, Seq[RawKafkaStream]) = {
    val client = consumerConnector
    val whiteList = new Whitelist(topic)
    val decoder = KafkaConsumerFactory.defaultDecoder
    (client, client.createMessageStreamsByFilter(whiteList, numStreams, decoder, decoder))
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
