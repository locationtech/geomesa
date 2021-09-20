/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.data

import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.consumer.{Consumer, ConsumerRecord}
import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.api.filter.Filter
import org.locationtech.geomesa.kafka.consumer.ThreadedConsumer
import org.locationtech.geomesa.kafka.data.KafkaDataStore.ExpiryTimeConfig
import org.locationtech.geomesa.kafka.index.KafkaFeatureCache
import org.locationtech.geomesa.kafka.utils.GeoMessage.{Change, Clear, Delete}
import org.locationtech.geomesa.kafka.utils.GeoMessageSerializer
<<<<<<< HEAD
import org.locationtech.geomesa.kafka.versions.{KafkaConsumerVersions, RecordVersions}
=======
import org.locationtech.geomesa.kafka.{KafkaConsumerVersions, RecordVersions}
>>>>>>> af0a88eb17 (GEOMESA-3100 Kafka layer views (#2784))
import org.locationtech.geomesa.utils.concurrent.CachedThreadPool
import org.locationtech.geomesa.utils.io.CloseWithLogging

import java.io.Closeable
import java.time.Duration
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong}
import java.util.concurrent.{ConcurrentHashMap, CountDownLatch}

/**
  * Reads from Kafka and populates a `KafkaFeatureCache`.
  * Manages geotools feature listeners
  */
trait KafkaCacheLoader extends Closeable with LazyLogging {
  def cache: KafkaFeatureCache
}

object KafkaCacheLoader extends LazyLogging {

  object LoaderStatus {
    private val count = new AtomicInteger(0)
    private val firstLoadStartTime = new AtomicLong(0L)

    def startLoad(): Boolean = synchronized {
      count.incrementAndGet()
      firstLoadStartTime.compareAndSet(0L, System.currentTimeMillis())
    }
    def completedLoad(): Unit = synchronized {
      if (count.decrementAndGet() == 0) {
        logger.info(s"Last active initial load completed.  " +
          s"Initial loads took ${System.currentTimeMillis()-firstLoadStartTime.get} milliseconds.")
        firstLoadStartTime.set(0L)
      }
    }

    def allLoaded(): Boolean = count.get() == 0
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

    private val initialLoader = if (doInitialLoad) {
      // for the initial load, don't bother spatially indexing until we have the final state
      val loader = new InitialLoader(sft, consumers, topic, frequency, serializer, initialLoadConfig, this)
      CachedThreadPool.execute(loader)
      Some(loader)
    } else {
      startConsumers()
      None
    }

    override def close(): Unit = {
      try {
        super.close()
      } finally {
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
=======
        case m: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> af0a88eb17 (GEOMESA-3100 Kafka layer views (#2784))
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016bef (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> 08d14c1e3e (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ef0eb58d93 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 6055a8bb0c (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> af7a3e028b (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d0dc799ff1 (GEOMESA-3100 Kafka layer views (#2784))
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
=======
        case m: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f7038468b7 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 08d14c1e3e (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> locationtech-main
=======
        case m: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 58286bfd3 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 5ca0cd6de5 (GEOMESA-3100 Kafka layer views (#2784))
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
=======
        case m: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> bddfdbea54 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> ef0eb58d93 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> af7a3e028b (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 6055a8bb0c (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> af7a3e028b (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> locationtech-main
=======
        case m: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 58286bfd3 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d0dc799ff1 (GEOMESA-3100 Kafka layer views (#2784))
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

    private val cache = KafkaFeatureCache.nonIndexing(sft, ordering)

    // track the offsets that we want to read to
    private val offsets = new ConcurrentHashMap[Int, Long]()
    private var latch: CountDownLatch = _
    private val done = new AtomicBoolean(false)

    override protected def consume(record: ConsumerRecord[Array[Byte], Array[Byte]]): Unit = {
      if (done.get) { toLoad.consume(record) } else {
        val headers = RecordVersions.getHeaders(record)
        val timestamp = RecordVersions.getTimestamp(record)
        val message = serializer.deserialize(record.key, record.value, headers, timestamp)
        logger.trace(s"Consumed message [$topic:${record.partition}:${record.offset}] $message")
        message match {
          case m: Change => toLoad.cache.fireChange(timestamp, m.feature); cache.put(m.feature)
          case m: Delete => toLoad.cache.fireDelete(timestamp, m.id, cache.query(m.id).orNull); cache.remove(m.id)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
=======
          case m: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> af0a88eb17 (GEOMESA-3100 Kafka layer views (#2784))
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016bef (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> 08d14c1e3e (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ef0eb58d93 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 6055a8bb0c (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> af7a3e028b (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d0dc799ff1 (GEOMESA-3100 Kafka layer views (#2784))
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
=======
          case m: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> f7038468b7 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 08d14c1e3e (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> locationtech-main
=======
          case m: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 58286bfd3 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 5ca0cd6de5 (GEOMESA-3100 Kafka layer views (#2784))
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
=======
          case m: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> bddfdbea54 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> ef0eb58d93 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> af7a3e028b (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 6055a8bb0c (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> af7a3e028b (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> locationtech-main
=======
          case m: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 58286bfd3 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d0dc799ff1 (GEOMESA-3100 Kafka layer views (#2784))
          case m => throw new IllegalArgumentException(s"Unknown message: $m")
        }
        // once we've hit the max offset for the partition, remove from the offset map to indicate we're done
        val maxOffset = offsets.getOrDefault(record.partition, Long.MaxValue)
        if (maxOffset <= record.offset) {
          offsets.remove(record.partition)
          latch.countDown()
          logger.info(s"Initial load: consumed [$topic:${record.partition}:${record.offset}] of $maxOffset, " +
              s"${latch.getCount} partitions remaining")
        } else if (record.offset > 0 && record.offset % 1048576 == 0) { // magic number 2^20
          logger.info(s"Initial load: consumed [$topic:${record.partition}:${record.offset}] of $maxOffset")
        }
      }
    }

    override def run(): Unit = {
      LoaderStatus.startLoad()

      import scala.collection.JavaConverters._

      val partitions = consumers.head.partitionsFor(topic).asScala.map(_.partition)
      try {
        // note: these methods are not available in kafka 0.9, which will cause it to fall back to normal loading
        val beginningOffsets = KafkaConsumerVersions.beginningOffsets(consumers.head, topic, partitions.toSeq)
        val endOffsets = KafkaConsumerVersions.endOffsets(consumers.head, topic, partitions.toSeq)
        partitions.foreach { p =>
          // end offsets are the *next* offset that will be returned, so subtract one to track the last offset
          // we will actually consume
          val endOffset = endOffsets.getOrElse(p, 0L) - 1L
          // note: not sure if start offsets are also off by one, but at the worst we would skip bulk loading
          // for the last message per topic
          val beginningOffset = beginningOffsets.getOrElse(p, 0L)
          if (beginningOffset < endOffset) {
            offsets.put(p, endOffset)
          }
        }
      } catch {
        case e: NoSuchMethodException => logger.warn(s"Can't support initial bulk loading for current Kafka version: $e")
      }
      // don't bother spinning up the consumer threads if we don't need to actually bulk load anything
      if (!offsets.isEmpty) {
        logger.info(s"Starting initial load for [$topic] with ${offsets.size} partitions")
        latch = new CountDownLatch(offsets.size)
        startConsumers() // kick off the asynchronous consumer threads
        try { latch.await() } finally {
          // stop the consumer threads, but won't close the consumers due to `closeConsumers`
          close()
        }
        // set a flag just in case the consumer threads haven't finished spinning down, so that we will
        // pass any additional messages back to the main loader
        done.set(true)
        logger.info(s"Finished initial load, transferring to indexed cache for [$topic]")
        cache.query(Filter.INCLUDE).foreach(toLoad.cache.put)
        logger.info(s"Finished transfer for [$topic]")
      }
      logger.info(s"Starting normal load for [$topic]")
      // start the normal loading
      toLoad.startConsumers()
      LoaderStatus.completedLoad()
    }
  }
}

