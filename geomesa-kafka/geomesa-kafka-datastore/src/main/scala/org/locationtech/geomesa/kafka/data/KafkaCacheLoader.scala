/***********************************************************************
 * Copyright (c) 2013-2025 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.data

import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.consumer.{Consumer, ConsumerRebalanceListener, ConsumerRecord, ConsumerRecords}
import org.apache.kafka.common.TopicPartition
import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.api.filter.Filter
import org.locationtech.geomesa.kafka.consumer.ThreadedConsumer
import org.locationtech.geomesa.kafka.consumer.ThreadedConsumer.ConsumerErrorHandler
import org.locationtech.geomesa.kafka.data.KafkaDataStore.ExpiryTimeConfig
import org.locationtech.geomesa.kafka.index.KafkaFeatureCache
import org.locationtech.geomesa.kafka.utils.GeoMessage.{Change, Clear, Delete}
import org.locationtech.geomesa.kafka.utils.GeoMessageSerializer
import org.locationtech.geomesa.kafka.versions.{KafkaConsumerVersions, RecordVersions}
import org.locationtech.geomesa.utils.concurrent.CachedThreadPool
import org.locationtech.geomesa.utils.io.CloseWithLogging

import java.io.Closeable
import java.time.Duration
import java.util.Collections
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{ConcurrentHashMap, CountDownLatch, Future}
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

/**
  * Reads from Kafka and populates a `KafkaFeatureCache`.
  * Manages geotools feature listeners
  */
trait KafkaCacheLoader extends Closeable with LazyLogging {
  def cache: KafkaFeatureCache
}

object KafkaCacheLoader extends LazyLogging {

  object LoaderStatus {

    private val loading = Collections.newSetFromMap(new ConcurrentHashMap[AnyRef, java.lang.Boolean]())
    private val firstLoadStartTime = new AtomicLong(0L)

    def startLoad(loader: AnyRef): Boolean = synchronized {
      if (!loading.add(loader)) {
        logger.warn(s"Called startLoad for a loader that was already registered and has not yet completed: $loader")
      }
      firstLoadStartTime.compareAndSet(0L, System.currentTimeMillis())
    }

    def completedLoad(loader: AnyRef): Unit = synchronized {
      if (!loading.remove(loader)) {
        logger.warn(s"Called completedLoad for a loader that was not registered or already deregistered: $loader")
      } else if (loading.isEmpty) {
        logger.info(s"Last active initial load completed.  " +
          s"Initial loads took ${System.currentTimeMillis() - firstLoadStartTime.get} milliseconds.")
        firstLoadStartTime.set(0L)
      }
    }

    def allLoaded(): Boolean = loading.isEmpty
  }

  object NoOpLoader extends KafkaCacheLoader {
    override val cache: KafkaFeatureCache = KafkaFeatureCache.empty()
    override def close(): Unit = {}
  }

  class KafkaCacheLoaderImpl(
      sft: SimpleFeatureType,
      override val cache: KafkaFeatureCache,
      consumers: Seq[Consumer[Array[Byte], Array[Byte]]],
      topic: String,
      frequency: Duration,
      offsetCommitInterval: FiniteDuration,
      serializer: GeoMessageSerializer,
      initialLoad: Option[scala.concurrent.duration.Duration],
      initialLoadConfig: ExpiryTimeConfig,
    ) extends ThreadedConsumer(consumers, frequency, offsetCommitInterval) with KafkaCacheLoader {

    try { classOf[ConsumerRecord[Any, Any]].getMethod("timestamp") } catch {
      case _: NoSuchMethodException => logger.warn("This version of Kafka doesn't support timestamps, using system time")
    }

    private val initialLoader = initialLoad.map { readBack =>
      // for the initial load, don't bother spatially indexing until we have the final state
      new InitialLoader(sft, consumers, topic, frequency, offsetCommitInterval, serializer, readBack, initialLoadConfig, this)
    }

    def start(): Unit = {
      initialLoader match {
        case None =>
          consumers.foreach(KafkaConsumerVersions.subscribe(_, topic))
          startConsumers()
        case Some(loader) =>
          consumers.foreach(KafkaConsumerVersions.subscribe(_, topic, loader))
          loader.start()
      }
    }

    override def close(): Unit = {
      try { super.close() } finally {
        CloseWithLogging(initialLoader)
        cache.close()
      }
    }

    override protected [KafkaCacheLoader] def consume(record: ConsumerRecord[Array[Byte], Array[Byte]]): Unit = {
      val headers = RecordVersions.getHeaders(record)
      val timestamp = RecordVersions.getTimestamp(record)
      val message = serializer.deserialize(record.key(), record.value(), headers, timestamp)
      logger.trace(s"Consumed message [$topic:${record.partition}:${record.offset}] $message")
      message match {
        case m: Change => cache.fireChange(timestamp, m.feature); cache.put(m.feature)
        case m: Delete => cache.fireDelete(timestamp, m.id, cache.query(m.id).orNull); cache.remove(m.id)
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
        case m => throw new IllegalArgumentException(s"Unknown message: $m")
      }
    }
  }

  /**
   * Handles initial loaded 'from-beginning' without indexing features in the spatial index. Will still
   * trigger message events.
   *
   * @param sft simple feature type
   * @param consumers consumers, won't be closed even on call to 'close()'
   * @param topic kafka topic
   * @param frequency polling frequency
   * @param offsetCommitInterval how often to commit offsets
   * @param serializer message serializer
   * @param readBack initial load read back
   * @param ordering feature ordering
   * @param toLoad main cache loader, used for callback when bulk loading is done
   */
  private class InitialLoader(
      sft: SimpleFeatureType,
      consumers: Seq[Consumer[Array[Byte], Array[Byte]]],
      topic: String,
      frequency: Duration,
      offsetCommitInterval: FiniteDuration,
      serializer: GeoMessageSerializer,
      readBack: scala.concurrent.duration.Duration,
      ordering: ExpiryTimeConfig,
      toLoad: KafkaCacheLoaderImpl
    ) extends ThreadedConsumer(consumers, frequency, offsetCommitInterval, false) with Runnable with ConsumerRebalanceListener {

    import scala.collection.JavaConverters._

    private val cache = KafkaFeatureCache.nonIndexing(sft, ordering)

    // track the offsets that we want to read to
    private val offsets = new ConcurrentHashMap[Int, Long]()
    private val assignment = Collections.newSetFromMap(new ConcurrentHashMap[Int, java.lang.Boolean]())
    @volatile
    private var done: Boolean = false
    private var latch: CountDownLatch = _
    @volatile
    private var submission: Future[_] = _

    override def onPartitionsRevoked(topicPartitions: java.util.Collection[TopicPartition]): Unit = {}

    override def onPartitionsAssigned(topicPartitions: java.util.Collection[TopicPartition]): Unit = {
      logger.debug(s"Partitions assigned: ${topicPartitions.asScala.mkString(", ")}")
      topicPartitions.asScala.foreach { tp =>
        if (assignment.add(tp.partition())) {
          val consumer = consumers.find(_.assignment().contains(tp)).orNull
          if (consumer == null) {
            logger.warn("Partition assigned but no consumer contains the assignment")
          } else {
            KafkaConsumerVersions.pause(consumer, tp)
            try {
              logger.debug(s"Checking offsets for [${tp.topic()}:${tp.partition()}]")
              // the only reliable way we've found to check max offset is to seek to the end and check the position there
              consumer.seekToEnd(Collections.singleton(tp))
              val end = consumer.position(tp)
              logger.debug(s"Setting max offset to [${tp.topic}:${tp.partition}:${end - 1}]")
              offsets.put(tp.partition(), end - 1)
              if (!readBack.isFinite) {
                KafkaConsumerVersions.seekToBeginning(consumer, tp)
              } else {
                val offset = Try {
                  val time = System.currentTimeMillis() - readBack.toMillis
                  KafkaConsumerVersions.offsetsForTimes(consumer, tp.topic, Seq(tp.partition), time).get(tp.partition)
                }
                offset match {
                  case Success(Some(o)) =>
                    logger.debug(s"Seeking to offset $o for read-back $readBack on [${tp.topic}:${tp.partition}]")
                    consumer.seek(tp, o)

                  case Success(None) =>
                    logger.debug(s"No prior offset found for read-back $readBack on [${tp.topic}:${tp.partition}], " +
                      "reading from head of queue")

                  case Failure(e) =>
                    logger.warn(s"Error finding initial offset: [${tp.topic}:${tp.partition}], seeking to beginning", e)
                    KafkaConsumerVersions.seekToBeginning(consumer, tp)
                }
              }
            } finally {
              KafkaConsumerVersions.resume(consumer, tp)
            }
          }
        }
      }
    }

    override protected def createConsumerRunnable(
        id: String,
        consumer: Consumer[Array[Byte], Array[Byte]],
        handler: ConsumerErrorHandler): Runnable = {
      new InitialLoaderConsumerRunnable(id, consumer, handler)
    }

    override protected def consume(record: ConsumerRecord[Array[Byte], Array[Byte]]): Unit = {
      if (done) { toLoad.consume(record) } else {
        val headers = RecordVersions.getHeaders(record)
        val timestamp = RecordVersions.getTimestamp(record)
        val message = serializer.deserialize(record.key, record.value, headers, timestamp)
        logger.trace(s"Consumed message [$topic:${record.partition}:${record.offset}] $message")
        message match {
          case m: Change => toLoad.cache.fireChange(timestamp, m.feature); cache.put(m.feature)
          case m: Delete => toLoad.cache.fireDelete(timestamp, m.id, cache.query(m.id).orNull); cache.remove(m.id)
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
          case m => throw new IllegalArgumentException(s"Unknown message: $m")
        }
        if (record.offset > 0 && record.offset % 1048576 == 0) { // magic number 2^20
          logger.info(s"Initial load: consumed [$topic:${record.partition}:${record.offset}]")
        }
      }
    }

    def start(): Unit = {
      LoaderStatus.startLoad(this)
      try {
        val partitions = consumers.head.partitionsFor(topic).asScala.map(_.partition)
        logger.info(s"Starting initial load for [$topic] with ${partitions.size} partitions")
        latch = new CountDownLatch(partitions.size)
        startConsumers() // kick off the asynchronous consumer threads
        submission = CachedThreadPool.submit(this)
      } catch {
        case NonFatal(e) =>
          LoaderStatus.completedLoad(this)
          throw e
      }
    }

    override def run(): Unit = {
      try {
        try { latch.await() } finally {
          // stop the consumer threads, but won't close the consumers due to `closeConsumers`
          // note: don't call this.close() as it would interrupt this thread
          super.close()
        }
        // set a flag just in case the consumer threads haven't finished spinning down, so that we will
        // pass any additional messages back to the main loader
        done = true
        logger.info(s"Finished initial load, transferring to indexed cache for [$topic]")
        cache.query(Filter.INCLUDE).foreach(toLoad.cache.put)
        logger.info(s"Finished transfer for [$topic], starting normal load")
        toLoad.startConsumers()
      } finally {
        LoaderStatus.completedLoad(this)
      }
    }

    override def close(): Unit = {
      try { super.close() } finally {
        if (submission != null && !submission.isDone) {
          submission.cancel(true)
        }
      }
    }

    /**
     * Consumer runnable that tracks when we have completed the initial load
     *
     * @param id id
     * @param consumer consumer
     * @param handler error handler
     */
    private class InitialLoaderConsumerRunnable(id: String, consumer: Consumer[Array[Byte], Array[Byte]], handler: ConsumerErrorHandler)
        extends ConsumerRunnable(id, consumer, handler) {

      override protected def processPoll(result: ConsumerRecords[Array[Byte], Array[Byte]]): Unit = {
        try {
          super.processPoll(result)
        } finally {
          result.partitions().asScala.foreach { tp =>
            val position = consumer.position(tp)
            // once we've hit the max offset for the partition, remove from the offset map so we don't double count it
            if (position >= offsets.getOrDefault(tp.partition(), Long.MaxValue)) {
              offsets.remove(tp.partition)
              latch.countDown()
              logger.info(s"Initial load: consumed [$topic:${tp.partition}:${position - 1}]")
              logger.info(s"Initial load completed for [$topic:${tp.partition}], ${latch.getCount} partitions remaining")
            }
          }
        }
      }
    }
  }
}

