/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka10.consumer

import java.io.IOException
import java.net.InetAddress
import java.util.UUID
import java.util.concurrent._
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import com.typesafe.scalalogging.LazyLogging
import kafka.api._
import kafka.common.{ErrorMapping, OffsetAndMetadata, TopicAndPartition}
import kafka.consumer._
import kafka.message.ByteBufferMessageSet
import kafka.serializer.Decoder
import kafka.utils.ZKGroupDirs
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.exception.ZkNodeExistsException
import org.locationtech.geomesa.kafka10.consumer.offsets.{GroupOffset, OffsetManager, RequestedOffset}

import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

/**
  * Kafka consumer that allows reading a stream from various offsets (vs just the last read)
  */
case class KafkaConsumer[K, V](topic: String,
                               config: ConsumerConfig,
                               keyDecoder: Decoder[K],
                               valueDecoder: Decoder[V]) extends LazyLogging {

  import KafkaConsumer._

  // internal state
  private val topicInfoMap = mutable.Map.empty[TopicAndPartition, ConcurrentLinkedQueue[PartitionTopicInfo]]
  private val consumeCheck = new ConcurrentHashMap[TopicAndPartition, Long]()
  private val consumers = mutable.HashSet.empty[WrappedConsumer]
  private val fetchers = mutable.HashSet.empty[FetchRunnable]
  private val streams = ArrayBuffer.empty[KafkaStreamLike[K, V]]
  private val isCreated = new AtomicBoolean(false)
  private val isShutdown = new AtomicBoolean(false)

  // see kafka.consumer.ZookeeperConsumerConnector.consumerIdString
  protected[consumer] val consumerId = {
    val host = InetAddress.getLocalHost.getHostName
    val time = System.currentTimeMillis
    val uuid = UUID.randomUUID().getMostSignificantBits.toHexString.substring(0, 8)
    s"${config.groupId}_%s-%d-%s".format(host, time, uuid)
  }

  // configuration options
  private val fetchBackoff = config.props.getInt("fetch.backoff.ms", ConsumerConfig.DefaultFetcherBackoffMs)

  // services
  private val executor = Executors.newScheduledThreadPool(config.numConsumerFetchers)
  private val offsetManager = new OffsetManager(config)
  private val rebalancer = new ConsumerRebalancer(this, config)

  /**
    * Create message streams for the given topic.
    *
    * Note that number of streams does not imply number of threads - use "num.consumer.fetchers" in
    * the config for that.
    *
    * Note: requested offsets depend on ephemeral zookeeper nodes - if you have terminated a client without
    * a clean shutdown, the node might stick around for ~30 seconds, causing the requested offset to be
    * ignored if the client is restarted during that time.
    */
  def createMessageStreams(numStreams: Int, offset: RequestedOffset): List[KafkaStreamLike[K, V]] = {
    assert(numStreams > 0, "Invalid number of streams requested")
    if (isCreated.getAndSet(true)) {
      throw new IllegalStateException("Can only create message streams once")
    }

    for (i <- 0 until numStreams) {
      // each stream has a queue used for sending messages
      val queue = new LinkedBlockingQueue[FetchedDataChunk](config.queuedMaxMessages)
      streams.append(new KafkaStreamLike[K, V](queue, config.consumerTimeoutMs, keyDecoder, valueDecoder))
    }

    if (config.autoCommitEnable) {
      val interval = config.autoCommitIntervalMs
      val autoCommit = new Runnable() {
        override def run() = autoCommitOffsets()
      }
      executor.scheduleWithFixedDelay(autoCommit, interval, interval, TimeUnit.MILLISECONDS)
    }

    // listener to consumer and partition changes
    rebalancer.start(numStreams, offset)

    streams.toList
  }

  /**
    * Look up the offsets we are requested to start with and commit then as the 'last read' offset.
    *
    */
  protected[consumer] def initializeOffsets(zkClient: ZkClient,
                                            dirs: ZKGroupDirs,
                                            offset: RequestedOffset): Unit = {
    if (offset == GroupOffset) {
      // Group offsets will be used during rebalance, so we don't need to handle them here
      return
    }

    try {
      val offsetPath = s"${dirs.consumerGroupDir}/start"
      // this will throw a node exists exception if we're not the first to try it
      zkClient.createEphemeral(offsetPath, offset.toString)
      logger.debug(s"Setting up initial offsets for ${config.groupId} with consumer $consumerId")

      val partitionMetadata = findPartitions(topic, config)
      if (partitionMetadata.isEmpty) {
        throw new RuntimeException(s"There are no partitions assigned to topic [$topic]")
      }
      val offsets = offsetManager.getOffsets(topic, partitionMetadata, offset)
      logger.debug(s"Starting offsets for topic [$topic] " +
        s"[${offsets.map(o => s"${o._1.partition}->${o._2}").mkString(",")}]")

      val time = System.currentTimeMillis()
      val commits = offsets.map { case (tap, o) => (tap, OffsetAndMetadata(o, metadata = "", timestamp = time)) }
      offsetManager.commitOffsets(commits, isAutoCommit = false)
      commits.foreach { case (tap, o) => consumeCheck.put(tap, o.offset) }
    } catch {
      case e: ZkNodeExistsException =>
      // Offsets have already been set up by another consumer - we don't need to do anything
      case NonFatal(e) =>
        logger.error("Error committing initial offsets - consumers will fall back to group offsets", e)
    }
  }

  protected[consumer] def closeFetchers(): Unit = synchronized {
    fetchers.foreach { fetcher =>
      fetcher.stop()
      fetcher.queue.clear()
    }
    fetchers.clear()
    consumers.foreach(_.disconnect())
    consumers.clear()

    autoCommitOffsets()

    topicInfoMap.clear()
    consumeCheck.clear()
  }

  protected[consumer] def initializeFetchers(partitions: Set[Int]): Unit = synchronized {
    if (isShutdown.get()) {
      return
    }
    val partitionMetadata = findPartitions(topic, config).filter(pm => partitions.contains(pm.partitionId))
    if (partitionMetadata.isEmpty) {
      logger.warn(s"No partitions assigned to consumer $consumerId - you should only have " +
        "one consumer per topic partition")
      return
    }
    val offsets = offsetManager.getOffsets(topic, partitionMetadata, GroupOffset)
    logger.debug(s"found partitions and offsets for topic [$topic] " +
      s"[${offsets.map(o => s"${o._1.partition}->${o._2}").mkString(",")}]")

    val partitionsPerStream = {
      val base = partitionMetadata.length / streams.length
      if (partitionMetadata.length % streams.length == 0) base else base + 1
    }

    // group partitions to create the requested number of streams
    val groupedPartitions = partitionMetadata.grouped(partitionsPerStream).zip(streams.iterator.map(_.queue))
    groupedPartitions.foreach { case (partitionsForQueue, queue) =>
      partitionsForQueue.foreach { partition =>
        val tap = TopicAndPartition(topic, partition.partitionId)
        val clientId = config.clientId
        val leader = partition.leader.map(l => Broker(l.host, l.port)).getOrElse(findNewLeader(tap, None, config))

        val connection = createConsumer(leader.host, leader.port, config, clientId)
        val consumer = WrappedConsumer(connection, tap, config)
        val fetcher = new FetchRunnable(consumer, tap, offsets(tap), queue)
        consumers.add(consumer)
        fetchers.add(fetcher)
        topicInfoMap.put(tap, new ConcurrentLinkedQueue)
        consumeCheck.put(tap, -1)

        // kick off a fetch cycle for this partition
        executor.submit(fetcher)
      }
    }
  }

  /**
    * Manually commit the offsets that have been read so far. Set auto-commit with "auto.commit.enable" config.
    */
  def commitOffsets(): Unit = doCommitOffsets(isAutoCommit = false)

  private def autoCommitOffsets(): Unit =
    if (config.autoCommitEnable) {
      doCommitOffsets(isAutoCommit = true)
    }

  private def doCommitOffsets(isAutoCommit: Boolean): Unit = {
    val commits = topicInfoMap.flatMap { case (tap, topicInfos) =>
      var maxToConsume = -1L
      val iter = topicInfos.iterator()
      while (iter.hasNext) {
        val topicInfo = iter.next()
        val nextToConsume = topicInfo.getConsumeOffset()
        if (nextToConsume > topicInfo.getFetchOffset()) {
          // topic info has been fully processed, drop it
          iter.remove()
        }
        maxToConsume = Math.max(nextToConsume, maxToConsume)
      }
      if (maxToConsume > consumeCheck.get(tap)) {
        Some(tap -> OffsetAndMetadata(maxToConsume, metadata = "", timestamp = System.currentTimeMillis()))
      } else {
        None
      }
    }.toMap

    if (commits.nonEmpty) {
      try {
        offsetManager.commitOffsets(commits, isAutoCommit)
        commits.foreach { case (tap, o) => consumeCheck.put(tap, o.offset) }
      } catch {
        case e: Exception => logger.error(s"Error committing offset for topic $topic", e)
      }
    }
  }

  /**
    * Stop reading messages.
    */
  def shutdown(): Unit = {
    isShutdown.set(true)
    executor.shutdown()
    rebalancer.shutdown()
    closeFetchers()
    streams.foreach(_.shutdown())
    autoCommitOffsets()
    offsetManager.close()
    streams.clear()
  }

  /**
    * Local class to handle fetches for a particular partition. Will keep re-scheduling itself
    * until consumer has been shut down.
    */
  class FetchRunnable(consumer: WrappedConsumer,
                      tap: TopicAndPartition,
                      var offset: Long,
                      val queue: BlockingQueue[FetchedDataChunk])
    extends Fetcher with Runnable with LazyLogging {

    private val topic = tap.topic
    private val partition = tap.partition
    private val stopped = new AtomicBoolean(false)

    private def scheduleRun(): Unit = executor.schedule(this, fetchBackoff, TimeUnit.MILLISECONDS)

    def stop(): Unit = stopped.set(true)

    override def run(): Unit = try {
      if (stopped.get()) {
        return
      }
      consumer.connect()
      val response = fetch(consumer.consumer, topic, partition, offset, config.fetchMessageMaxBytes)
      response match {
        case Success(messages) =>
          if (messages.isEmpty) {
            scheduleRun()
          } else {
            val consumed = new AtomicLong(-1)
            val fetched = new AtomicLong(messages.last.offset)
            val clientId = consumer.consumer.clientId
            val pti = new PartitionTopicInfo(topic, partition, queue, consumed, fetched, null, clientId)
            queue.put(FetchedDataChunk(messages, pti, offset))
            topicInfoMap(tap).add(pti)
            offset = messages.last.nextOffset
            executor.submit(this)
          }

        case Failure(e) =>
          logger.warn("Fetching thread received error", e)
          consumer.disconnect()
          scheduleRun()
      }
    } catch {
      case e: Exception =>
        logger.warn("Fetching thread threw exception", e)
        try {
          consumer.disconnect()
        } catch {
          case e: Exception => logger.warn("Fetching thread threw exception trying to disconnect from consumer", e)
        }
        scheduleRun()
    }
  }

}

trait Fetcher extends LazyLogging {

  /**
    * Fetch messages
    */
  def fetch(consumer: SimpleConsumer,
            topic: String,
            partition: Int,
            offset: Long,
            maxBytes: Int): Try[ByteBufferMessageSet] = {

    val requestBuilder = new FetchRequestBuilder().clientId(consumer.clientId)
    val request = requestBuilder.addFetch(topic, partition, offset, maxBytes).build()
    val response = consumer.fetch(request)
    if (response.hasError) {
      val code = response.errorCode(topic, partition)
      logger.error(s"Error fetching data from the broker [${consumer.host}:${consumer.port}] - " +
        s"reason: ${ErrorMapping.exceptionFor(code)}")
      Failure(ErrorMapping.exceptionFor(code))
    } else {
      Success(response.messageSet(topic, partition))
    }
  }
}

object KafkaConsumer extends LazyLogging {

  import org.locationtech.geomesa.utils.geotools.RichIterator.RichIterator

  /**
    * Creates a consumer
    */
  def createConsumer(host: String, port: Int, config: ConsumerConfig, clientId: String) =
    new SimpleConsumer(host, port, config.socketTimeoutMs, config.socketReceiveBufferBytes, clientId)

  /**
    * Creates a new consumer in the case that a broker goes down
    */
  def reCreateConsumer(consumer: SimpleConsumer,
                       tap: TopicAndPartition,
                       config: ConsumerConfig): SimpleConsumer = {
    val oldLeader = Some(Broker(consumer.host, consumer.port))
    val leader = findNewLeader(tap, oldLeader, config)
    createConsumer(leader.host, leader.port, config, consumer.clientId)
  }

  /**
    * Gets the partitions and leaders for a given topic
    */
  def findPartitions(topic: String, config: ConsumerConfig): Seq[PartitionMetadata] = {
    val brokers = Brokers(config)
    // use an iterator so that we only evaluated until we get one that works
    brokers.iterator.flatMap(b => findPartitions(b, topic, config)).headOption.getOrElse {
      throw new IOException(s"Could not determine partitions for [$topic] using " +
        s"brokers ${brokers.mkString(",")}")
    }
  }

  private def findPartitions(broker: Broker,
                             topic: String,
                             config: ConsumerConfig): Option[Seq[PartitionMetadata]] = {
    var consumer: SimpleConsumer = null
    try {
      val timeout = config.offsetsChannelSocketTimeoutMs
      val buffer = config.socketReceiveBufferBytes
      consumer = new SimpleConsumer(broker.host, broker.port, timeout, buffer, "leaderLookup")
      val response = consumer.send(new TopicMetadataRequest(Seq(topic), 0))
      response.topicsMetadata.find(_.topic == topic).map(_.partitionsMetadata).filter(_.nonEmpty)
    } catch {
      case e: Exception =>
        logger.warn(s"Error communicating with broker [$broker] to find leader for [$topic]", e)
        None
    } finally {
      if (consumer != null) {
        consumer.close()
      }
    }
  }

  /**
    * Find the leader for a topic/partition - call if the old leader stops responding
    */
  def findNewLeader(tap: TopicAndPartition, oldLeader: Option[Broker], config: ConsumerConfig): Broker =
    findNewLeader(tap, oldLeader, config, 1)

  @tailrec
  private def findNewLeader(tap: TopicAndPartition,
                            oldLeader: Option[Broker],
                            config: ConsumerConfig,
                            tries: Int): Broker = {
    val partitions = Try(findPartitions(tap.topic, config)).toOption
    val maybeLeader = partitions.flatMap(_.find(_.partitionId == tap.partition)).flatMap(_.leader)
    val leader = oldLeader match {
      // first time through if the leader hasn't changed give ZooKeeper a second to recover
      // second time, assume the broker did recover before failover, or it was a non-Broker issue
      case Some(old) => maybeLeader.filter(m => (m.host != old.host && m.port != old.port) || tries > 1)
      case None => maybeLeader
    }

    leader.map(l => Broker(l.host, l.port)) match {
      case Some(l) => l
      case None =>
        if (tries < config.rebalanceMaxRetries) {
          Thread.sleep(config.refreshLeaderBackoffMs)
          findNewLeader(tap, oldLeader, config, tries + 1)
        } else {
          throw new RuntimeException(s"Could not find new leader for topic and partition $tap")
        }
    }
  }
}

/**
  * Container for passing around a consumer so that it can be rebuilt without losing the reference to it
  */
case class WrappedConsumer(var consumer: SimpleConsumer, tap: TopicAndPartition, config: ConsumerConfig) {

  private var isConnected = false

  def connect(): Unit =
    if (!isConnected) {
      consumer = KafkaConsumer.reCreateConsumer(consumer, tap, config)
      isConnected = true
    }

  def disconnect(): Unit =
    if (isConnected) {
      consumer.close()
      isConnected = false
    }
}
