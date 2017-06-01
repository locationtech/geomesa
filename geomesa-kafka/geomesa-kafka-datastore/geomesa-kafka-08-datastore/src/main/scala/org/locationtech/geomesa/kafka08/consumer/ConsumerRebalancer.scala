/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka08.consumer

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock
import java.util.{List => jList}

import com.typesafe.scalalogging.LazyLogging
import kafka.common.{ConsumerRebalanceFailedException, TopicAndPartition}
import kafka.consumer._
import kafka.utils.ZkUtils._
import kafka.utils._
import org.I0Itec.zkclient.exception.{ZkInterruptedException, ZkNodeExistsException}
import org.I0Itec.zkclient.{IZkChildListener, IZkDataListener, IZkStateListener}
import org.apache.zookeeper.Watcher.Event.KeeperState
import org.locationtech.geomesa.kafka08.consumer.offsets.RequestedOffset
import org.locationtech.geomesa.kafka08.{KafkaUtils08, ZkUtils08}

import scala.annotation.tailrec
import scala.collection.{Map, mutable}

/**
 * Manages rebalancing of consumers across jvms
 *
 * Consumer info is kept in zookeeper - changes to partitions or number of consumers triggers a
 * zookeeper listener, which causes a rebalance to be run.
 *
 * During rebalance, each partition will be assigned to a single consumer - if there are more
 * consumers than partitions, some consumers will not receive any data.
 */
class ConsumerRebalancer[K, V](consumer: KafkaConsumer[K, V], config: ConsumerConfig)
    extends IZkStateListener with IZkDataListener with IZkChildListener with LazyLogging {

  private val zkUtils = KafkaUtils08.createZkUtils(config)
  private val partitionAssignor = PartitionAssignor.createInstance(config.partitionAssignmentStrategy)

  private val topicStreamCount = mutable.Map.empty[String, Int] // used to register the consumer in zk
  private val topicPartitions = mutable.Map.empty[String, Set[Int]] // partitions that the consumer owns
  private var isWatcherTriggered = false
  private val watcherTriggerLock = new ReentrantLock
  private val watcherTriggerCondition = watcherTriggerLock.newCondition()
  private val rebalanceLock = new AnyRef
  private val started  = new AtomicBoolean(false)
  private val isShutdown = new AtomicBoolean(false)
  private val topic = consumer.topic
  private val consumerId = consumer.consumerId

  private val dirs = new ZKGroupDirs(config.groupId)

  // thread to watch for rebalance notifications
  private val watcherExecutorThread = new Thread(s"${consumerId}_watcher_executor") {
    override def run() {
      logger.debug(s"starting watcher executor thread for consumer $consumerId")
      var doRebalance = false
      while (!isShutdown.get) {
        try {
          watcherTriggerLock.lock()
          try {
            if (!isWatcherTriggered) {
              // wake up periodically so that it can check the shutdown flag
              watcherTriggerCondition.await(1000, TimeUnit.MILLISECONDS)
            }
          } finally {
            doRebalance = isWatcherTriggered
            isWatcherTriggered = false
            watcherTriggerLock.unlock()
          }
          if (doRebalance) {
            syncedRebalance()
          }
        } catch {
          case _: InterruptedException => // stopped
          case e: Exception => logger.error("Error during syncedRebalance", e)
        }
      }
      logger.debug(s"stopping watcher executor thread for consumer $consumerId")
    }
  }

  @throws(classOf[Exception])
  override def handleNewSession(): Unit = {
    // When we get a SessionExpired event, we lost all ephemeral nodes and zkclient has reestablished a
    // connection for us. We need to release the ownership of the current consumer and re-register this
    // consumer in the consumer registry and trigger a rebalance.
    logger.debug(s"ZK session expired; release old partition ownership; re-register consumer $consumerId")
    registerConsumerInZK()
    registerZKListeners()
    // explicitly trigger load balancing for this consumer
    syncedRebalance()
  }

  def handleSessionEstablishmentError(throwable: Throwable): Unit = {
    logger.error("Could not establish session with zookeeper", throwable)
  }

  @throws(classOf[Exception])
  override def handleStateChanged(state: KeeperState): Unit = {
    // do nothing, since zkclient will do reconnect for us
  }

  @throws(classOf[Exception])
  override def handleDataChange(dataPath: String, data: scala.Any): Unit = {
    try {
      logger.debug(s"Topic info for path $dataPath changed to $data, triggering rebalance")
      // queue up the rebalance event
      rebalanceEventTriggered()
      // There is no need to re-subscribe the watcher since it will be automatically
      // re-registered upon firing of this event by zkClient
    } catch {
      case e: Throwable =>
        logger.error(s"Error while handling topic partition change for data path $dataPath", e)
    }
  }

  @throws(classOf[Exception])
  override def handleDataDeleted(dataPath: String): Unit = {
    // TODO: This need to be implemented when kafka supports delete topic
    logger.warn(s"Topic for path $dataPath gets deleted, which should not happen at this time")
  }

  @throws(classOf[Exception])
  override def handleChildChange(parentPath: String, children: jList[String]): Unit =
    rebalanceEventTriggered()

  /**
   * Starts this rebalancer
   */
  def start(numStreams: Int, offset: RequestedOffset): Unit = {
    if (!started.compareAndSet(false, true)) {
      throw new IllegalStateException("Rebalancer is already started")
    }
    topicStreamCount.put(topic, numStreams)
    registerConsumerInZK()
    consumer.initializeOffsets(zkUtils.zkClient, dirs, offset)
    registerZKListeners()

    // explicitly trigger load balancing for this consumer
    syncedRebalance()

    watcherExecutorThread.start()
  }

  def shutdown(): Unit = {
    isShutdown.set(true)
    watcherExecutorThread.interrupt()
    zkUtils.close()
  }

  private def registerZKListeners(): Unit = {
    // listener to consumer and partition changes
    zkUtils.zkClient.subscribeStateChanges(this)
    zkUtils.zkClient.subscribeChildChanges(dirs.consumerRegistryDir, this)
    // register on broker partition path changes
    val topicPath = s"$BrokerTopicsPath/$topic"
    zkUtils.zkClient.subscribeDataChanges(topicPath, this)
  }

  /**
   * See kafka.consumer.ZookeeperConsumerConnector#registerConsumerInZK
   */
  private def registerConsumerInZK(): Unit = {
    logger.debug(s"Begin registering consumer $consumerId in ZK")

    val timestamp = SystemTime.milliseconds.toString
    val path = s"${dirs.consumerRegistryDir}/$consumerId"
    val timeout = config.zkSessionTimeoutMs
    val consumerInfo = Json.encode {
      Map("version" -> 1, "subscription" -> topicStreamCount, "pattern" -> "static", "timestamp" -> timestamp)
    }
    zkUtils.createEphemeralPathExpectConflictHandleZKBug(path, consumerInfo, null, (_, _) => true, timeout)

    logger.debug(s"End registering consumer $consumerId in ZK")
  }

  /**
   * See kafka.consumer.ZookeeperConsumerConnector.ZKRebalancerListener#releasePartitionOwnership
   */
  private def releasePartitionOwnership(): Unit = {
    logger.debug("Releasing partition ownership")
    for ((topic, partitions) <- topicPartitions) {
      partitions.foreach(deletePartitionOwnershipFromZK(topic, _))
      topicPartitions.remove(topic)
    }
  }

  /**
   * See kafka.consumer.ZookeeperConsumerConnector.ZKRebalancerListener#deletePartitionOwnershipFromZK
   */
  private def deletePartitionOwnershipFromZK(topic: String, partition: Int): Unit = {
    val znode = zkUtils.getConsumerPartitionOwnerPath(config.groupId, topic, partition)
    zkUtils.deletePath(znode)
    logger.debug(s"Consumer $consumerId releasing $znode")
  }

  /**
   * See kafka.consumer.ZookeeperConsumerConnector.ZKRebalancerListener#rebalanceEventTriggered()
   */
  private def rebalanceEventTriggered(): Unit = {
    watcherTriggerLock.lock()
    try {
      isWatcherTriggered = true
      watcherTriggerCondition.signalAll()
    } finally {
      watcherTriggerLock.unlock()
    }
  }

  def syncedRebalance(): Unit = rebalanceLock.synchronized { rebalance(1) }

  /**
   * Releases all partitions and attempts to claim new ones based on the current available consumers
   */
  @tailrec
  private def rebalance(numTries: Int): Unit = {
    if (isShutdown.get()) {
      return
    }
    logger.debug(s"Begin rebalancing consumer $consumerId try $numTries")
    val done = try {
      rebalance(ConsumerRebalancer.getCluster(zkUtils))
    } catch {
      case _: ZkInterruptedException | _: InterruptedException =>
        false
      case e: Exception =>
        // occasionally, we may hit a ZK exception because the ZK state is changing while we are iterating.
        // For example, a ZK node can disappear between the time we get all children and the time we
        // try to get the value of a child. Just let this go since another rebalance will be triggered.
        logger.info("Exception during rebalance", e)
        false
    }
    logger.debug(s"End rebalancing consumer $consumerId try $numTries")
    if (!done) {
      if (numTries < config.rebalanceMaxRetries) {
        // Here the cache is at a risk of being stale. To take future rebalancing decisions correctly,
        // we should clear the cache
        logger.debug(
          "Rebalancing attempt failed. Clearing the cache before the next rebalancing operation is triggered")
        // stop all fetchers and clear all the queues to avoid data duplication
        consumer.closeFetchers()
        Thread.sleep(config.rebalanceBackoffMs)
        rebalance(numTries + 1)
      } else {
        throw new ConsumerRebalanceFailedException(
          s"$consumerId can't rebalance after ${config.rebalanceMaxRetries} retries")
      }
    }
  }

  /**
   * See kafka.consumer.ZookeeperConsumerConnector.ZKRebalancerListener#rebalance
   */
  private def rebalance(cluster: Seq[Broker]): Boolean = {
    val brokers = zkUtils.getAllBrokersInCluster
    if (brokers.size == 0) {
      // This can happen in a rare case when there are no brokers available in the cluster when the consumer
      // is started. We log an warning and register for child changes on brokers/id so that rebalance can
      // be triggered when the brokers are up.
      logger.warn("No brokers found when trying to rebalance.")
      zkUtils.zkClient.subscribeChildChanges(ZkUtils.BrokerIdsPath, this)
      true
    } else {
      // fetchers must be stopped to avoid data duplication, since if the current
      // rebalancing attempt fails, the partitions that are released could be owned by another consumer.
      // But if we don't stop the fetchers first, this consumer would continue returning data for released
      // partitions in parallel. So, not stopping the fetchers leads to duplicate data.
      consumer.closeFetchers()
      releasePartitionOwnership()

      val assignmentContext = zkUtils.createAssignmentContext(config.groupId, consumerId, config.excludeInternalTopics)
      val partitionOwnershipDecision = KafkaUtils08.assign(partitionAssignor, assignmentContext)

      // move the partition ownership here, since that can be used to indicate a truly successful rebalancing
      // attempt. A rebalancing attempt is completed successfully only after the fetchers have been
      // started correctly
      if (reflectPartitionOwnershipDecision(partitionOwnershipDecision)) {
        val partitions = partitionOwnershipDecision.keys.map(_.partition).toSet
        topicPartitions.put(topic, partitions) // track the partitions we own
        consumer.initializeFetchers(partitions)
        true
      } else {
        false
      }
    }
  }

  /**
   * See kafka.consumer.ZookeeperConsumerConnector.ZKRebalancerListener#reflectPartitionOwnershipDecision
   */
  private def reflectPartitionOwnershipDecision(
      partitionOwnershipDecision: Map[TopicAndPartition, ConsumerThreadId]): Boolean = {

    val successfullyOwnedPartitions = partitionOwnershipDecision.flatMap { partitionOwner =>
      val tap = partitionOwner._1
      val consumerThreadId = partitionOwner._2
      val partitionOwnerPath = zkUtils.getConsumerPartitionOwnerPath(config.groupId, tap.topic, tap.partition)
      try {
        zkUtils.createEphemeralPathExpectConflict(partitionOwnerPath, consumerThreadId.toString())
        logger.debug(s"$consumerThreadId successfully owned topic and partition $tap")
        Some(tap)
      } catch {
        case e: ZkNodeExistsException =>
          // The node hasn't been deleted by the original owner. So wait a bit and retry.
          logger.debug(s"waiting for the partition ownership to be deleted for topic and partition $tap")
          None
      }
    }
    // if even one of the partition ownership attempt has failed, return false
    if (successfullyOwnedPartitions.size == partitionOwnershipDecision.size) {
      true
    } else {
      // remove all paths that we have owned in ZK
      successfullyOwnedPartitions.foreach(tap => deletePartitionOwnershipFromZK(tap.topic, tap.partition))
      false
    }
  }
}

object ConsumerRebalancer {

  /**
   * See kafka.utils.ZkUtils$#getCluster(org.I0Itec.zkclient.ZkClient)
   */
  def getCluster(zkUtils: ZkUtils08) : Seq[Broker] =
    zkUtils.getChildrenParentMayNotExist(BrokerIdsPath).map { node =>
      val brokerZKString = zkUtils.readData(s"$BrokerIdsPath/$node")._1
      Broker(brokerZKString, node.toInt)
    }
}
