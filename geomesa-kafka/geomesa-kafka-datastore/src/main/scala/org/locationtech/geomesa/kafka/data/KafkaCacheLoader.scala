/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.kafka.data

import com.typesafe.scalalogging.LazyLogging
import io.micrometer.core.instrument.{DistributionSummary, Metrics, Tags}
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
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{ConcurrentHashMap, CountDownLatch, Future}
import java.util.{Collections, Date}
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

  private val MetricsPrefix = s"${KafkaDataStore.MetricsPrefix}.consumer"

  /**
   * Object for tracking the initial load status of data stores
   */
  object LoaderStatus {

    private val loading = Collections.newSetFromMap(new ConcurrentHashMap[AnyRef, java.lang.Boolean]())
    private val firstLoadStartTime = new AtomicLong(0L)

    /**
     * Register a loader starting
     *
     * @param loader loader instance
     * @return
     */
    def startLoad(loader: AnyRef): Boolean = synchronized {
      if (!loading.add(loader)) {
        logger.warn(s"Called startLoad for a loader that was already registered and has not yet completed: $loader")
      }
      firstLoadStartTime.compareAndSet(0L, System.currentTimeMillis())
    }

    /**
     * De-register a loader as having completed
     *
     * @param loader loader instance
     */
    def completedLoad(loader: AnyRef): Unit = synchronized {
      if (!loading.remove(loader)) {
        logger.warn(s"Called completedLoad for a loader that was not registered or already deregistered: $loader")
      } else if (loading.isEmpty) {
        logger.info(s"Last active initial load completed.  " +
          s"Initial loads took ${System.currentTimeMillis() - firstLoadStartTime.get} milliseconds.")
        firstLoadStartTime.set(0L)
      }
    }

    /**
     * Indicates if any active loads are ongoing
     *
     * @return
     */
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
      tags: Tags,
    ) extends ThreadedConsumer(consumers, frequency, offsetCommitInterval) with KafkaCacheLoader {

    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    try { classOf[ConsumerRecord[Any, Any]].getMethod("timestamp") } catch {
      case _: NoSuchMethodException => logger.warn("This version of Kafka doesn't support timestamps, using system time")
    }

    private val updates = Metrics.counter(s"$MetricsPrefix.updates", tags)
    private val deletes = Metrics.counter(s"$MetricsPrefix.deletes", tags)
    private val clears = Metrics.counter(s"$MetricsPrefix.clears", tags)
    private val dtgMetrics = sft.getDtgIndex.map { i =>
      val last = Metrics.gauge(s"$MetricsPrefix.dtg.latest", tags, new AtomicLong())
      val latency =
        DistributionSummary
          .builder(s"$MetricsPrefix.dtg.latency")
          .baseUnit("milliseconds")
          .tags(tags)
          .publishPercentileHistogram()
          .minimumExpectedValue(1d)
          .maximumExpectedValue(24 * 60 * 60 * 1000d) // 1 day
          .register(Metrics.globalRegistry)
      DateMetrics(i, last, latency)
    }

    // for the initial load, don't bother spatially indexing until we have the final state
    private val initialLoader = initialLoad.map(readBack => new InitialLoader(readBack))

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

    override protected def consume(record: ConsumerRecord[Array[Byte], Array[Byte]]): Unit = {
      val headers = RecordVersions.getHeaders(record)
      val timestamp = RecordVersions.getTimestamp(record)
      val message = serializer.deserialize(record.key(), record.value(), headers, timestamp)
      logger.trace(s"Consumed message [$topic:${record.partition}:${record.offset}] $message")
      message match {
        case m: Change =>
          cache.fireChange(timestamp, m.feature)
          cache.put(m.feature)
          updates.increment()
          dtgMetrics.foreach { case DateMetrics(index, dtg, latency) =>
            val date = m.feature.getAttribute(index).asInstanceOf[Date]
            if (date != null) {
              dtg.updateAndGet(prev => Math.max(prev, date.getTime))
              latency.record(System.currentTimeMillis() - date.getTime)
            }
          }

        case m: Delete =>
          cache.fireDelete(timestamp, m.id, cache.query(m.id).orNull)
          cache.remove(m.id)
          deletes.increment()

        case _: Clear =>
          cache.fireClear(timestamp)
          cache.clear()
          clears.increment()

        case m => throw new IllegalArgumentException(s"Unknown message: $m")
      }
    }

    /**
     * Handles initial loaded 'from-beginning' without indexing features in the spatial index. Will still
     * trigger message events.
     *
     * @param readBack initial load read back
     */
    private class InitialLoader(readBack: scala.concurrent.duration.Duration)
        extends ThreadedConsumer(consumers, frequency, offsetCommitInterval, false) with Runnable with ConsumerRebalanceListener {

      import scala.collection.JavaConverters._

      private val cache = KafkaFeatureCache.nonIndexing(sft, initialLoadConfig)
      private val toLoad = KafkaCacheLoaderImpl.this

      private val updates = Metrics.counter(s"$MetricsPrefix.readback.updates", tags)
      private val deletes = Metrics.counter(s"$MetricsPrefix.readback.deletes", tags)
      private val clears = Metrics.counter(s"$MetricsPrefix.readback.clears", tags)

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
                try {
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
                  checkComplete(consumer, tp)
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
            case m: Change =>
              toLoad.cache.fireChange(timestamp, m.feature)
              cache.put(m.feature)
              updates.increment()

            case m: Delete =>
              toLoad.cache.fireDelete(timestamp, m.id, cache.query(m.id).orNull)
              cache.remove(m.id)
              deletes.increment()

            case _: Clear =>
              toLoad.cache.fireClear(timestamp)
              cache.clear()
              clears.increment()

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
       * Checks the current position of the consumer, and notifies through the countdown latch when the initial load
       * is complete
       *
       * @param consumer consumer
       * @param tp topic and partition to check
       */
      private def checkComplete(consumer: Consumer[Array[Byte], Array[Byte]], tp: TopicPartition): Unit = {
        val position = consumer.position(tp)
        // once we've hit the max offset for the partition, remove from the offset map so we don't double count it
        if (position > offsets.getOrDefault(tp.partition(), Long.MaxValue)) {
          offsets.remove(tp.partition)
          latch.countDown()
          logger.info(s"Initial load completed for [$topic:${tp.partition}], ${latch.getCount} partitions remaining")
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
          try { super.processPoll(result) } finally {
            result.partitions().asScala.foreach(checkComplete(consumer, _))
          }
        }
      }
    }
  }

  private case class DateMetrics(i: Int, last: AtomicLong, latency: DistributionSummary)
}

