/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.kafka.tools.status

import com.beust.jcommander.Parameters
import com.typesafe.scalalogging.LazyLogging
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.exception.ZkNoNodeException
import org.locationtech.geomesa.kafka.tools.{KafkaDataStoreCommand, OptionalZkPathParams}
import org.locationtech.geomesa.kafka09.KafkaUtils09

class KafkaGetTypeNamesCommand extends KafkaDataStoreCommand with LazyLogging {

  override val name = "get-names"
  override val params = new KafkaGetTypeNamesParams()

  override def execute() = {
    if (params.zkPath == null) {
      logger.info(s"Running List Features without zkPath...")
      logger.info(s"zkPath - schema")

      val zkUtils = KafkaUtils09.createZkUtils(params.zookeepers, Int.MaxValue, Int.MaxValue)
      try {
        zkUtils.getAllTopics.filter(_.contains('-')).foreach(printZkPathAndTopicString(zkUtils.zkClient, _))
      } finally {
        zkUtils.close()
      }
    } else {
      logger.info(s"Running List Features using zkPath ${params.zkPath}...")
      withDataStore(_.getTypeNames.foreach(println))
    }
  }

  /**
    * Fetches schema info from zookeeper to check if the topic is one created by GeoMesa.
    * Prints zkPath and SFT name if valid.
    *
    * @param topic The kafka topic
    */
  def printZkPathAndTopicString(zkClient: ZkClient, topic: String): Unit = {
    val sb = new StringBuilder()

    var tokenizedTopic = topic.split("-")
    var tokenizedTopicCount = tokenizedTopic.length

    while (tokenizedTopicCount > 1) {
      try {
        val topicName = zkClient.readData[String](getTopicNamePath(tokenizedTopic)) // throws ZkNoNodeException if not valid
        if (topicName.equals(topic)) {
          println(s"/${tokenizedTopic.take(tokenizedTopicCount-1).mkString("/")} - ${tokenizedTopic.last}")
          return
        }
      } catch {
        case e: ZkNoNodeException =>
          // wrong zkPath and schema name combo
      } finally {
        tokenizedTopicCount -= 1
        tokenizedTopic = topic.split("-", tokenizedTopicCount)
      }
    }
  }

  private def getTopicNamePath(tokenizedTopic: Array[String]): String = {
    s"/${tokenizedTopic.mkString("/")}/Topic"
  }
}

@Parameters(commandDescription = "List GeoMesa features for a given zkPath")
class KafkaGetTypeNamesParams extends OptionalZkPathParams {
  override val isProducer: Boolean = false
  override var partitions: String = null
  override var replication: String = null
}
