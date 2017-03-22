/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka10.consumer.offsets

import java.io.{Closeable, IOException}

import com.typesafe.scalalogging.LazyLogging
import kafka.api._
import kafka.common.ErrorMapping._
import kafka.common.{OffsetAndMetadata, TopicAndPartition}
import kafka.consumer.ConsumerConfig
import kafka.message.{ByteBufferMessageSet, MessageAndOffset}
import kafka.network.BlockingChannel
import org.locationtech.geomesa.kafka10.{KafkaUtils10, ZkUtils10}
import org.locationtech.geomesa.kafka10.consumer.offsets.FindOffset.MessagePredicate
import org.locationtech.geomesa.kafka10.consumer.{Broker, Brokers, Fetcher, WrappedConsumer}
import org.locationtech.geomesa.kafka10.consumer.KafkaConsumer._

import scala.annotation.tailrec
import scala.util.{Failure, Success}

/**
  * Manages storing and retrieving of offsets for a consumer group
  */
class OffsetManager(val config: ConsumerConfig)
  extends Fetcher with Closeable with LazyLogging {

  import OffsetManager._

  lazy private val channel = WrappedChannel(zkUtils, config)
  lazy private val zkUtils = KafkaUtils10.createZkUtils(config)

  /**
    * Get a saved offset.
    *
    * @param when what offset to get (latest, earliest, etc)
    */
  def getOffsets(topic: String, when: RequestedOffset): Offsets = {
    val partitions = findPartitions(topic, config)
    assert(partitions.nonEmpty, s"Topic $topic does not exist in brokers ${Brokers(config).mkString(",")}")
    getOffsets(topic, partitions, when)
  }

  /**
    * Get a saved offset.
    *
    * @param when what offset to get (latest, earliest, etc)
    */
  def getOffsets(topic: String, partitions: Seq[PartitionMetadata], when: RequestedOffset): Offsets = {
    assert(partitions.nonEmpty, "Topic and partitions are required")
    val function: () => Offsets = when match {
      case EarliestOffset => () => getOffsetsBefore(topic, partitions, OffsetRequest.EarliestTime, config)
      case LatestOffset => () => getOffsetsBefore(topic, partitions, OffsetRequest.LatestTime, config)
      case SpecificOffset(s) => () => partitions.map(p => TopicAndPartition(topic, p.partitionId) -> s).toMap
      case DateOffset(date) => () => getOffsetsBefore(topic, partitions, date, config)
      case FindOffset(pred) => () => findOffsets(topic, partitions, pred, config)
      case GroupOffset => () =>
        val taps = partitions.map(p => TopicAndPartition(topic, p.partitionId))
        getGroupOffsets(channel.channel(), taps, config)

      case _ => throw new NotImplementedError()
    }

    val result = retryOffsets(function, 1).filterNot { case (_, offset) => offset < 0 }

    if (result.size == partitions.length) {
      result
    } else if (when != LatestOffset) {
      // fallback to latest available if nothing else works
      val remaining = partitions.filterNot(p => result.contains(TopicAndPartition(topic, p.partitionId)))
      logger.warn(s"Didn't find a valid offset for topic [$topic] partitions " +
        s"[${remaining.map(_.partitionId).mkString(",")}] looking for [$when] - defaulting to latest")
      result ++ getOffsets(topic, remaining, LatestOffset)
    } else {
      throw new RuntimeException(s"Could not find a valid offset for partitions ${partitions.mkString(",")}")
    }
  }

  /**
    * Find an offset based on a message predicate - messages are assumed to be sorted according to the predicate
    */
  def findOffsets(topic: String,
                  partitions: Seq[PartitionMetadata],
                  predicate: MessagePredicate,
                  config: ConsumerConfig): Offsets = {
    partitions.flatMap { partition =>
      val tap = TopicAndPartition(topic, partition.partitionId)
      val leader = partition.leader.map(l => Broker(l.host, l.port)).getOrElse(findNewLeader(tap, None, config))
      val consumer = WrappedConsumer(createConsumer(leader.host, leader.port, config, "offsetLookup"), tap, config)
      try {
        val consumerId = Request.OrdinaryConsumerId
        val earliest = consumer.consumer.earliestOrLatestOffset(tap, OffsetRequest.EarliestTime, consumerId)
        val latest = consumer.consumer.earliestOrLatestOffset(tap, OffsetRequest.LatestTime, consumerId)
        binaryOffsetSearch(consumer, predicate, (earliest, latest)).map(tap -> _)
      } finally {
        consumer.consumer.close()
      }
    }.toMap
  }

  @tailrec
  private[kafka10] final def binaryOffsetSearch(consumer: WrappedConsumer,
                                                predicate: MessagePredicate,
                                                bounds: (Long, Long)): Option[Long] = {

    // end is exclusive
    val (start, end) = bounds
    if (start == end) return Some(end)

    val offset = (start + end) / 2
    val maxBytes = consumer.config.fetchMessageMaxBytes

    val result = fetch(consumer.consumer, consumer.tap.topic, consumer.tap.partition, offset, maxBytes) match {
      case Success(messageSet) if messageSet.nonEmpty =>
        logger.debug(s"checking $bounds found (${messageSet.head.offset},${messageSet.last.offset})")

        val head = messageSet.head
        val headCheck = predicate(head.message)

        lazy val messages = trim(messageSet, end)
        lazy val last = messages.last
        lazy val lastCheck = predicate(last.message)

        if (headCheck == 0) {
          logger.trace(s"found offset = ${head.offset} (head)")
          Right(head.offset)
        } else if (headCheck > 0) {
          // look left
          if (offset == start) {
            logger.trace(s"no more ranges to check.  using ${head.offset} (head)")
            Right(head.offset)
          } else {
            logger.trace(s"need to look left (headCheck > 0)")
            Left((start, offset))
          }
        } else if (lastCheck == 0) {
          logger.trace(s"found offset = ${last.offset} (last)")
          Right(last.offset)
        } else if (lastCheck < 0) {
          // look right
          if (last.offset + 1 == end) {
            logger.trace(s"no more ranges to check.  using ${last.offset} (last)")
            Right(last.offset + 1)
          } else {
            logger.trace(s"need to look right (lastCheck < 0)")
            Left((last.offset + 1, end))
          }
        } else {
          // it's in this block
          val offset = messages.tail.find(m => predicate(m.message) >= 0).map(_.offset).get
          logger.trace(s"found offset = $offset (in block)")
          Right(offset)
        }

      case Success(messageSet) =>
        logger.debug(s"checking $bounds found no messages")
        Right(-1L) // no messages found

      case Failure(e) =>
        logger.warn("Error fetching messages to find offsets", e)
        throw e
    }

    result match {
      case Right(found) if found != -1 => Some(found)
      case Right(found) => None
      case Left(nextBounds) => binaryOffsetSearch(consumer, predicate, nextBounds)
    }
  }

  // `messageSet` may contain data at or beyond end, if so trim
  private[kafka10] def trim(messageSet: ByteBufferMessageSet, end: Long): Array[MessageAndOffset] =
  // use an array to allow direct access to the last element
    messageSet.takeWhile(msg => msg.offset < end).toArray

  @tailrec
  private def retryOffsets(function: () => Offsets, tries: Int): Offsets =
    try {
      function()
    } catch {
      case e: Exception =>
        if (tries < config.offsetsCommitMaxRetries) {
          logger.debug("Error getting offsets.  Retrying.")
          Thread.sleep(config.offsetsChannelBackoffMs * tries)
          retryOffsets(function, tries + 1)
        } else {
          throw e
        }
    }

  /**
    * Commit group offsets read
    */
  def commitOffsets(offsets: Map[TopicAndPartition, OffsetAndMetadata], isAutoCommit: Boolean = false): Unit =
    if (isAutoCommit) {
      // auto commits will not be retried on failures
      commitOffsets(offsets, config.offsetsCommitMaxRetries)
    } else {
      commitOffsets(offsets, 1)
    }

  @tailrec
  private def commitOffsets(offsets: Map[TopicAndPartition, OffsetAndMetadata], tries: Int): Unit = {
    val version = OffsetCommitRequest.CurrentVersion

    val request = new OffsetCommitRequest(config.groupId, offsets, version, 0, clientId)

    try {
      channel.channel().send(request)
      val response = OffsetCommitResponse.readFrom(KafkaUtils10.channelToPayload(channel.channel()))
      val errors = response.commitStatus.filter { case (_, code) => code != NoError }
      errors.foreach { case (topicAndPartition, code) =>
        if (code == OffsetMetadataTooLargeCode) {
          logger.error(s"Could not commit offset for topic $topicAndPartition: Metadata is too large")
        } else if (code == NotCoordinatorForConsumerCode || code == ConsumerCoordinatorNotAvailableCode) {
          logger.debug("Could not commit offsets because coordinator is unavailable")
          channel.disconnect()
          throw exceptionFor(code)
        } else {
          logger.error(s"Error committing offsets: ${exceptionFor(code)}")
          throw exceptionFor(code)
        }
      }
    } catch {
      case e: Exception =>
        if (tries < config.offsetsCommitMaxRetries) {
          Thread.sleep(config.offsetsChannelBackoffMs)
          commitOffsets(offsets, tries + 1)
        } else {
          throw new IOException(s"Error committing offsets", e)
        }
    }
  }

  override def close(): Unit = {
    channel.disconnect()
    zkUtils.close()
  }
}

object OffsetManager extends LazyLogging {

  type Offsets = Map[TopicAndPartition, Long]

  private val clientId = "offsetManager"

  /**
    * Gets the last saved offset for a group
    */
  def getGroupOffsets(channel: BlockingChannel,
                      partitions: Seq[TopicAndPartition],
                      config: ConsumerConfig): Offsets = {
    val version = OffsetFetchRequest.CurrentVersion
    val request = new OffsetFetchRequest(config.groupId, partitions, version, 0, clientId)
    channel.send(request)
    val response = OffsetFetchResponse.readFrom(KafkaUtils10.channelToPayload(channel))
    handleOffsetErrors(response.requestInfo.values.map(_.error))
    response.requestInfo.map { case (topicAndPartion, metadata) => (topicAndPartion, metadata.offset) }
  }

  /**
    * Gets offset based on a time - this is only accurate to the log file level, not message time level
    */
  def getOffsetsBefore(topic: String,
                       partitions: Seq[PartitionMetadata],
                       time: Long,
                       config: ConsumerConfig): Offsets = {
    partitions.flatMap { partition =>
      val tap = TopicAndPartition(topic, partition.partitionId)
      val leader = partition.leader.map(l => Broker(l.host, l.port)).getOrElse(findNewLeader(tap, None, config))
      val consumer = createConsumer(leader.host, leader.port, config, "offsetLookup")
      try {
        val requestInfo = Map(tap -> PartitionOffsetRequestInfo(time, 1))
        val request = OffsetRequest(requestInfo, clientId = consumer.clientId)
        val errorAndOffsets = consumer.getOffsetsBefore(request).partitionErrorAndOffsets(tap)
        maybeThrowException(errorAndOffsets.error)
        errorAndOffsets.offsets.headOption.map(tap -> _)
      } finally {
        consumer.close()
      }
    }.toMap
  }

  private def handleOffsetErrors(codes: Iterable[Short]): Unit =
    codes.filter(_ != NoError).foreach { code =>
      if (code != NotCoordinatorForConsumerCode && code != OffsetsLoadInProgressCode) {
        logger.error(s"Error fetching offsets: ${exceptionFor(code)}")
      }
      throw exceptionFor(code)
    }
}

/**
  * Container for passing around a channel so that it can be rebuilt without losing the reference to it
  */
case class WrappedChannel(zkUtils: ZkUtils10, config: ConsumerConfig) {

  private var reusableChannel: BlockingChannel = null
  private val timeout = config.offsetsChannelSocketTimeoutMs
  private val backoff = config.offsetsChannelBackoffMs

  /**
    * Gets the channel, connecting if not already connected
    */
  def channel() = synchronized {
    if (reusableChannel == null || !reusableChannel.isConnected) {
      reconnect()
    }
    reusableChannel
  }

  /**
    * Disconnects then reconnects the channel
    */
  def reconnect(): Unit = synchronized {
    disconnect()
    reusableChannel = zkUtils.channelToOffsetManager(config.groupId, timeout, backoff)
  }

  /**
    * Disconnects the channel
    */
  def disconnect(): Unit = synchronized {
    if (reusableChannel != null) {
      reusableChannel.disconnect()
      reusableChannel = null
    }
  }
}
