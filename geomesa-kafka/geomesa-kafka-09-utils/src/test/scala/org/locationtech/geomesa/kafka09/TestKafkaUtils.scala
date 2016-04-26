package org.locationtech.geomesa.kafka09

import java.util.Properties

import kafka.server.{KafkaServer, KafkaConfig}
import kafka.utils.TestUtils
import org.locationtech.geomesa.kafka.AbstractTestKafkaUtils

class TestKafkaUtils extends AbstractTestKafkaUtils {
  def createBrokerConfig(nodeId: Int, zkConnect: String): Properties = TestUtils.createBrokerConfig(nodeId, zkConnect)
  def choosePort: Int = TestUtils.RandomPort
  def createServer(props: Properties): KafkaServer = TestUtils.createServer(new KafkaConfig(props))
}
