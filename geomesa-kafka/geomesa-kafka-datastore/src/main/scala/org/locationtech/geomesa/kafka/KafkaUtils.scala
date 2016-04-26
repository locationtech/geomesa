package org.locationtech.geomesa.kafka

import java.io.File
import java.nio.ByteBuffer
import java.util.ServiceLoader

import com.typesafe.scalalogging.LazyLogging
import kafka.admin.AdminUtils
import kafka.api.{OffsetCommitRequest, PartitionMetadata, RequestOrResponse}
import kafka.client.ClientUtils
import kafka.common.{OffsetAndMetadata, TopicAndPartition}
import kafka.consumer.{ConsumerThreadId, PartitionAssignor, AssignmentContext, ConsumerConfig}
import kafka.network.BlockingChannel
import kafka.utils.{ZKStringSerializer, Utils}
import org.I0Itec.zkclient.ZkClient
import org.apache.zookeeper.data.Stat
import org.locationtech.geomesa.kafka.consumer.Broker

import scala.collection.{immutable, Map}

trait AbstractKafkaUtils {
  def channelToPayload: (BlockingChannel) => ByteBuffer
  def channelSend(bc: BlockingChannel, requestOrResponse: RequestOrResponse): Long
  def leaderBrokerForPartition: PartitionMetadata => Option[Broker]
  def assign(partitionAssignor: PartitionAssignor, ac: AssignmentContext): Map[TopicAndPartition, ConsumerThreadId]
  def createZkUtils(config: ConsumerConfig): AbstractZkUtils =
    createZkUtils(config.zkConnect, config.zkSessionTimeoutMs, config.zkConnectionTimeoutMs)
  def createZkUtils(zkConnect: String, sessionTimeout: Int, connectTimeout: Int): AbstractZkUtils
  def tryFindNewLeader(tap: TopicAndPartition,
                       partitions: Option[Seq[PartitionMetadata]],
                       oldLeader: Option[Broker],
                       tries: Int): Option[Broker]
  def rm(file: File): Unit
  def createOffsetAndMetadata(offset: Long, time: Long): OffsetAndMetadata
  def createOffsetCommitRequest(groupId: String,
                                requestInfo: immutable.Map[TopicAndPartition, OffsetAndMetadata],
                                versionId: Short,
                                correlationId: Int,
                                clientId: String): OffsetCommitRequest
}

object KafkaUtilsLoader extends LazyLogging {
  lazy val kafkaUtils: AbstractKafkaUtils = {
    val kuIter = ServiceLoader.load(classOf[AbstractKafkaUtils]).iterator()
    if (kuIter.hasNext) {
      val first = kuIter.next()
      if (kuIter.hasNext) {
        logger.warn(s"Multiple geomesa KafkaUtils found.  Should only have one. Using the first: '$first'")
      }
      first
    } else {
      logger.debug(s"No geomesa KafkaUtils found.  Using default one for 0.8.")
      DefaultKafkaUtils
    }
  }
}

/**
  * Default KafkaUtils for kafka 0.8
  */
object DefaultKafkaUtils extends AbstractKafkaUtils {
  def channelToPayload: (BlockingChannel) => ByteBuffer = _.receive().buffer
  def channelSend(bc: BlockingChannel, requestOrResponse: RequestOrResponse): Long = bc.send(requestOrResponse).toLong
  def leaderBrokerForPartition: PartitionMetadata => Option[Broker] = _.leader.map(l => Broker(l.host, l.port))
  def assign(partitionAssignor: PartitionAssignor, ac: AssignmentContext) = partitionAssignor.assign(ac)
  def createZkUtils(zkConnect: String, sessionTimeout: Int, connectTimeout: Int): AbstractZkUtils = {
    // zkStringSerializer is required - otherwise topics won't be created correctly
    DefaultZkUtils(new ZkClient(zkConnect, sessionTimeout, connectTimeout, ZKStringSerializer))
  }
  def tryFindNewLeader(tap: TopicAndPartition,
                       partitions: Option[Seq[PartitionMetadata]],
                       oldLeader: Option[Broker],
                       tries: Int): Option[Broker] = {
    val maybeLeader = partitions.flatMap(_.find(_.partitionId == tap.partition)).flatMap(_.leader)
    val leader = oldLeader match {
      // first time through if the leader hasn't changed give ZooKeeper a second to recover
      // second time, assume the broker did recover before failover, or it was a non-Broker issue
      case Some(old) => maybeLeader.filter(m => (m.host != old.host && m.port != old.port) || tries > 1)
      case None      => maybeLeader
    }

    leader.map(l => Broker(l.host, l.port))
  }
  def rm(file: File): Unit = Utils.rm(file)
  def createOffsetAndMetadata(offset: Long, time: Long): OffsetAndMetadata = OffsetAndMetadata(offset, timestamp = time)
  def createOffsetCommitRequest(groupId: String,
                                requestInfo: immutable.Map[TopicAndPartition, OffsetAndMetadata],
                                versionId: Short,
                                correlationId: Int,
                                clientId: String): OffsetCommitRequest =
    new OffsetCommitRequest(groupId, requestInfo, versionId, correlationId, clientId)
}

/**
  * Default ZkUtils for kafka 0.8
  */
case class DefaultZkUtils(zkClient: ZkClient) extends AbstractZkUtils {
  def channelToOffsetManager(groupId: String, socketTimeoutMs: Int, retryBackOffMs: Int): BlockingChannel =
    ClientUtils.channelToOffsetManager(groupId, zkClient, socketTimeoutMs, retryBackOffMs)
  def deleteTopic(topic: String): Unit = AdminUtils.deleteTopic(zkClient, topic)
  def topicExists(topic: String): Boolean = AdminUtils.topicExists(zkClient, topic)
  def createTopic(topic: String, partitions: Int, replication: Int) = AdminUtils.createTopic(zkClient, topic, partitions, replication)
  def getLeaderForPartition(topic: String, partition: Int): Option[Int] = kafka.utils.ZkUtils.getLeaderForPartition(zkClient, topic, partition)
  def createEphemeralPathExpectConflict(path: String, data: String): Unit = kafka.utils.ZkUtils.createEphemeralPathExpectConflict(zkClient, path, data)
  def createEphemeralPathExpectConflictHandleZKBug(path: String,
                                                   data: String,
                                                   expectedCallerData: Any,
                                                   checker: (String, Any) => Boolean,
                                                   backoffTime: Int): Unit =
    kafka.utils.ZkUtils.createEphemeralPathExpectConflictHandleZKBug(zkClient, path, data, expectedCallerData, checker, backoffTime)
  def deletePath(path: String) = kafka.utils.ZkUtils.deletePath(zkClient, path)
  def getConsumerPartitionOwnerPath(groupId: String, topic: String, partition: Int): String =
    kafka.utils.ZkUtils.getConsumerPartitionOwnerPath(groupId, topic, partition)
  def getChildrenParentMayNotExist(path: String): Seq[String] = kafka.utils.ZkUtils.getChildrenParentMayNotExist(zkClient, path)
  def getAllBrokersInCluster: Seq[kafka.cluster.Broker] = kafka.utils.ZkUtils.getAllBrokersInCluster(zkClient)
  def createAssignmentContext(group: String, consumerId: String, excludeInternalTopics: Boolean): AssignmentContext =
    new AssignmentContext(group, consumerId, excludeInternalTopics, zkClient)
  def readData(path: String): (String, Stat) = kafka.utils.ZkUtils.readData(zkClient, path)
  def close(): Unit = zkClient.close()
}

trait AbstractZkUtils {
  def zkClient: ZkClient
  def channelToOffsetManager(groupId: String, socketTimeoutMs: Int, retryBackOffMs: Int): BlockingChannel
  def deleteTopic(topic: String): Unit
  def topicExists(topic: String): Boolean
  def createTopic(topic: String, partitions: Int, replication: Int): Unit
  def getLeaderForPartition(topic: String, partition: Int): Option[Int]
  def createEphemeralPathExpectConflict(path: String, data: String): Unit
  def createEphemeralPathExpectConflictHandleZKBug(path: String,
                                                   data: String,
                                                   expectedCallerData: Any,
                                                   checker: (String, Any) => Boolean,
                                                   backoffTime: Int): Unit
  def deletePath(path: String): Unit
  def getConsumerPartitionOwnerPath(groupId: String, topic: String, partition: Int): String
  def getChildrenParentMayNotExist(path: String): Seq[String]
  def getAllBrokersInCluster: Seq[kafka.cluster.Broker]
  def createAssignmentContext(group: String, consumerId: String, excludeInternalTopics: Boolean): AssignmentContext
  def readData(path: String): (String, Stat)
  def close(): Unit
}
