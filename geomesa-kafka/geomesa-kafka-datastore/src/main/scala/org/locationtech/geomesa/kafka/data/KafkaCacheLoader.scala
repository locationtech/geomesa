/***********************************************************************
 * Copyright (c) 2013-2025 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.data

import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.consumer.{Consumer, ConsumerRecord, ConsumerRecords}
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
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import java.util.concurrent.{ConcurrentHashMap, CountDownLatch, Future}
import scala.util.control.NonFatal

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
      frequency: Long,
      serializer: GeoMessageSerializer,
      doInitialLoad: Boolean,
      initialLoadConfig: ExpiryTimeConfig
    ) extends ThreadedConsumer(consumers, Duration.ofMillis(frequency)) with KafkaCacheLoader {
    try { classOf[ConsumerRecord[Any, Any]].getMethod("timestamp") } catch {
      case _: NoSuchMethodException => logger.warn("This version of Kafka doesn't support timestamps, using system time")
    }

    private val initialLoader =
      if (doInitialLoad) {
        // for the initial load, don't bother spatially indexing until we have the final state
        Some(new InitialLoader(sft, consumers, topic, frequency, serializer, initialLoadConfig, this))
      } else {
        None
      }

    def start(): Unit = {
      initialLoader match {
        case None => startConsumers()
        case Some(loader) => loader.start()
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
    * @param consumers consumers, won't be closed even on call to 'close()'
    * @param topic kafka topic
    * @param frequency polling frequency in milliseconds
    * @param serializer message serializer
    * @param toLoad main cache loader, used for callback when bulk loading is done
    */
  private class InitialLoader(
      sft: SimpleFeatureType,
      consumers: Seq[Consumer[Array[Byte], Array[Byte]]],
      topic: String,
      frequency: Long,
      serializer: GeoMessageSerializer,
      ordering: ExpiryTimeConfig,
      toLoad: KafkaCacheLoaderImpl
    ) extends ThreadedConsumer(consumers, Duration.ofMillis(frequency), false) with Runnable {

    import scala.collection.JavaConverters._

    private val cache = KafkaFeatureCache.nonIndexing(sft, ordering)

    // track the offsets that we want to read to
    private val offsets = new ConcurrentHashMap[Int, Long]()
    private val done = new AtomicBoolean(false)
    private var latch: CountDownLatch = _
    @volatile
    private var submission: Future[_] = _

    override protected def createConsumerRunnable(
        id: String,
        consumer: Consumer[Array[Byte], Array[Byte]],
        handler: ConsumerErrorHandler): Runnable = {
      new InitialLoaderConsumerRunnable(id, consumer, handler)
    }

    override protected def consume(record: ConsumerRecord[Array[Byte], Array[Byte]]): Unit = {
      if (done.get) { toLoad.consume(record) } else {
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
        val doInitialLoad =
          try {
            // note: these methods are not available in kafka 0.9, which will cause it to fall back to normal loading
            val beginningOffsets = KafkaConsumerVersions.beginningOffsets(consumers.head, topic, partitions.toSeq)
            val endOffsets = KafkaConsumerVersions.endOffsets(consumers.head, topic, partitions.toSeq)
            partitions.exists(p => beginningOffsets.getOrElse(p, 0L) < endOffsets.getOrElse(p, 0L))
          } catch {
            case e: NoSuchMethodException =>
              logger.warn(s"Can't support initial bulk loading for current Kafka version: $e")
              false
          }
        if (doInitialLoad) {
          logger.info(s"Starting initial load for [$topic] with ${partitions.size} partitions")
          latch = new CountDownLatch(partitions.size)
          startConsumers() // kick off the asynchronous consumer threads
          submission = CachedThreadPool.submit(this)
        } else {
          // don't bother spinning up the consumer threads if we don't need to actually bulk load anything
          startNormalLoad()
          LoaderStatus.completedLoad(this)
        }
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
        done.set(true)
        logger.info(s"Finished initial load, transferring to indexed cache for [$topic]")
        cache.query(Filter.INCLUDE).foreach(toLoad.cache.put)
        logger.info(s"Finished transfer for [$topic]")
        startNormalLoad()
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
    // start the normal loading
    private def startNormalLoad(): Unit = {
      logger.info(s"Starting normal load for [$topic]")
      toLoad.startConsumers()
    }

    /**
     * Consumer runnable for the initial load. It's not possible to check assignments until after poll() has been called,
     * and checking endOffsets is not reliable as a high water mark, due to transaction markers or other issues that
     * may advance the endOffset past where the consumer can actually reach (unless new messages are written).
     *
     * @param id id
     * @param consumer consumer
     * @param handler error handler
     */
    private class InitialLoaderConsumerRunnable(id: String, consumer: Consumer[Array[Byte], Array[Byte]], handler: ConsumerErrorHandler)
        extends ConsumerRunnable(id, consumer, handler) {

      private var checkOffsets = true

      override protected def processPoll(result: ConsumerRecords[Array[Byte], Array[Byte]]): Unit = {
        if (checkOffsets) {
          consumer.assignment().asScala.foreach { tp =>
            // the only reliable way we've found to check max offset is to seek to the end and check the position there
            val position = consumer.position(tp)
            consumer.seekToEnd(Collections.singleton(tp))
            val end = consumer.position(tp)
            // return to the original position
            consumer.seek(tp, position)
            logger.debug(s"Setting max offset to [${tp.topic}:${tp.partition}:${end - 1}]")
            offsets.put(tp.partition(), end - 1)
          }
          checkOffsets = false
        }
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

