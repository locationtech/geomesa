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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
import org.locationtech.geomesa.kafka.{KafkaConsumerVersions, RecordVersions}
<<<<<<< HEAD
>>>>>>> af0a88eb17 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> a0314fb7ff (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
=======
import org.locationtech.geomesa.kafka.{KafkaConsumerVersions, RecordVersions}
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 17f13b3a7a (GEOMESA-3100 Kafka layer views (#2784))
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 2f66072323 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c82e069f24 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> c8421ce4b1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bbf7cc3b06 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 862857ce2e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 46554dec25 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> c354d233a4 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a7765277e6 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 4231e686dd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9a412492cb (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 8347f5d271 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6e5007fe41 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> 186135b4fc (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ea0ead77fe (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 335182b815 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 6af71ff432 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c1afaaa1de (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> b4c6267f1c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ff40ff0969 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> a62b956fb6 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c82e069f2 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bbf7cc3b0 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
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
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> d0dc799ff1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d42d165496 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 890b70c869 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 7c7dc18e87 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9361541304 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 2f66072323 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> a62b956fb6 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 186135b4fc (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ef0eb58d9 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c82e069f24 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> c354d233a4 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 6055a8bb0 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c8421ce4b1 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ea0ead77fe (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 335182b815 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> bbf7cc3b06 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> ea3b40e187 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 93cedd7b35 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4d37d5ef54 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> ad15fc9fe3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> cedd740417 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9bda77cfe3 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 7c0f257cef (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9236b02087 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 862857ce2e (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ef0eb58d9 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 46554dec25 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> c354d233a4 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> a7765277e6 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 28427dfd8f (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 95c83ca7f (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 4231e686dd (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d42d16549 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 9a412492cb (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 890b70c86 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 8347f5d271 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 7c7dc18e8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 6e5007fe41 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> ac0357d362 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> a62b956fb6 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> ef0eb58d9 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> c82e069f2 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 186135b4fc (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> ea0ead77fe (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> bbf7cc3b0 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 335182b815 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 4adaa7f479 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d2cb939f5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 6af71ff432 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 93cedd7b3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> c1afaaa1de (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4d37d5ef5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> b4c6267f1c (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> ad15fc9fe (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> ff40ff0969 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 0185d52d35 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d2002b5afd (GEOMESA-3100 Kafka layer views (#2784))
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
=======
        case m: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
<<<<<<< HEAD
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
>>>>>>> 2f66072323 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c82e069f24 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> c8421ce4b1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bbf7cc3b06 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 862857ce2e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 46554dec25 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> c354d233a4 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a7765277e6 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 4231e686dd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9a412492cb (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 8347f5d271 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6e5007fe41 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> 186135b4fc (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ea0ead77fe (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 335182b815 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 6af71ff432 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c1afaaa1de (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> b4c6267f1c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ff40ff0969 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> a62b956fb6 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c82e069f2 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bbf7cc3b0 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
=======
>>>>>>> 9361541304 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9236b02087 (GEOMESA-3100 Kafka layer views (#2784))
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
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> af7a3e028b (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> 9361541304 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9236b02087 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> locationtech-main
=======
        case m: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 58286bfd3 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> d0dc799ff1 (GEOMESA-3100 Kafka layer views (#2784))
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
=======
        case m: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 95c83ca7fa (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> d42d165496 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> 7c7dc18e87 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ad15fc9fe3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 7c0f257cef (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 890b70c869 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 7c7dc18e87 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 9361541304 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ea3b40e187 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 28427dfd8f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ac0357d362 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a62b956fb6 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4adaa7f479 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 2f66072323 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 46554dec25 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> ef0eb58d9 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c82e069f24 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
>>>>>>> c354d233a4 (GEOMESA-3100 Kafka layer views (#2784))
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6055a8bb0 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> c8421ce4b1 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> ad15fc9fe3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 7c0f257cef (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> bbf7cc3b06 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e187 (GEOMESA-3100 Kafka layer views (#2784))
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
=======
        case m: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d2cb939f51 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 93cedd7b35 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 186135b4fc (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ea0ead77fe (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 7c7dc18e8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 4d37d5ef54 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ad15fc9fe3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
=======
        case m: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> a0314fb7ff (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> cedd740417 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9bda77cfe3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7c0f257cef (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 9236b02087 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 862857ce2e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 46554dec25 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 6055a8bb0 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c354d233a4 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 6e5007fe41 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ea0ead77fe (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 6055a8bb0 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 7c7dc18e8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> a7765277e6 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 28427dfd8f (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 95c83ca7f (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 4231e686dd (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> d42d16549 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 9a412492cb (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ad15fc9fe (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 890b70c86 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 8347f5d271 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 7c7dc18e8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 6e5007fe41 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> ac0357d362 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> a62b956fb6 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> c82e069f2 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 186135b4fc (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> 6055a8bb0 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> ea0ead77fe (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
<<<<<<< HEAD
>>>>>>> 6055a8bb0 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ad15fc9fe (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> bbf7cc3b0 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 335182b815 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 4adaa7f479 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d2cb939f5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 6af71ff432 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 93cedd7b3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> c1afaaa1de (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4d37d5ef5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> b4c6267f1c (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ad15fc9fe (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> ff40ff0969 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
=======
        case m: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 17f13b3a7a (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 0185d52d35 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
        case _: Clear  => cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d2002b5afd (GEOMESA-3100 Kafka layer views (#2784))
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
>>>>>>> 2f66072323 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c82e069f24 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> c8421ce4b1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bbf7cc3b06 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 862857ce2e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 46554dec25 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> c354d233a4 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a7765277e6 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 4231e686dd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9a412492cb (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 8347f5d271 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6e5007fe41 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> 186135b4fc (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ea0ead77fe (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 335182b815 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 6af71ff432 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c1afaaa1de (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> b4c6267f1c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ff40ff0969 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> a62b956fb6 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c82e069f2 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bbf7cc3b0 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
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
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> d0dc799ff1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> d42d165496 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 890b70c869 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 7c7dc18e87 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9361541304 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 2f66072323 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> a62b956fb6 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 186135b4fc (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ef0eb58d9 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c82e069f24 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> c354d233a4 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 6055a8bb0 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c8421ce4b1 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ea0ead77fe (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 335182b815 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> bbf7cc3b06 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> ea3b40e187 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 93cedd7b35 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4d37d5ef54 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> ad15fc9fe3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> cedd740417 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9bda77cfe3 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 7c0f257cef (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9236b02087 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 862857ce2e (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ef0eb58d9 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 46554dec25 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> c354d233a4 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> a7765277e6 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 28427dfd8f (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 95c83ca7f (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 4231e686dd (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d42d16549 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 9a412492cb (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 890b70c86 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 8347f5d271 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 7c7dc18e8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 6e5007fe41 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> ac0357d362 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> a62b956fb6 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> ef0eb58d9 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> c82e069f2 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 186135b4fc (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> ea0ead77fe (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
=======
<<<<<<< HEAD
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> bbf7cc3b0 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 335182b815 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 4adaa7f479 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d2cb939f5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 6af71ff432 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 93cedd7b3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> c1afaaa1de (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4d37d5ef5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> b4c6267f1c (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> ad15fc9fe (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> ff40ff0969 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 0185d52d35 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d2002b5afd (GEOMESA-3100 Kafka layer views (#2784))
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
=======
          case m: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
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
<<<<<<< HEAD
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
>>>>>>> 2f66072323 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c82e069f24 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> c8421ce4b1 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bbf7cc3b06 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 862857ce2e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 46554dec25 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> c354d233a4 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a7765277e6 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 4231e686dd (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9a412492cb (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 8347f5d271 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 6e5007fe41 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> 186135b4fc (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ea0ead77fe (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 335182b815 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 6af71ff432 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c1afaaa1de (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> b4c6267f1c (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ff40ff0969 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> a62b956fb6 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> c82e069f2 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bbf7cc3b0 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
=======
>>>>>>> 9361541304 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9236b02087 (GEOMESA-3100 Kafka layer views (#2784))
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
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> af7a3e028b (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> 9361541304 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 9236b02087 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> locationtech-main
=======
          case m: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 58286bfd3 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 5ca0cd6de (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> d0dc799ff1 (GEOMESA-3100 Kafka layer views (#2784))
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
=======
          case m: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 95c83ca7fa (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> d42d165496 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> 7c7dc18e87 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ad15fc9fe3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 7c0f257cef (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 890b70c869 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 7c7dc18e87 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 9361541304 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ea3b40e187 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 28427dfd8f (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ac0357d362 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> a62b956fb6 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4adaa7f479 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 2f66072323 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 46554dec25 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> ef0eb58d9 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> c82e069f24 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
>>>>>>> c354d233a4 (GEOMESA-3100 Kafka layer views (#2784))
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 6055a8bb0 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> c8421ce4b1 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> ad15fc9fe3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 7c0f257cef (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> bbf7cc3b06 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e187 (GEOMESA-3100 Kafka layer views (#2784))
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
=======
          case m: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d2cb939f51 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 93cedd7b35 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 186135b4fc (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ea0ead77fe (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 7c7dc18e8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
=======
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 4d37d5ef54 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> ad15fc9fe3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> 3be8d2a5a4 (Merge branch 'feature/postgis-fixes')
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
=======
          case m: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> a0314fb7ff (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> cedd740417 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 9bda77cfe3 (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 7c0f257cef (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 9236b02087 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 862857ce2e (GEOMESA-3100 Kafka layer views (#2784))
=======
>>>>>>> 46554dec25 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> 6055a8bb0 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c354d233a4 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 6e5007fe41 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ea0ead77fe (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 6055a8bb0 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 7c7dc18e8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> a7765277e6 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 28427dfd8f (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 95c83ca7f (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 4231e686dd (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> d42d16549 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 9a412492cb (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> ad15fc9fe (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 890b70c86 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 8347f5d271 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> 7c7dc18e8 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 6e5007fe41 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> 936154130 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> ac0357d362 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
>>>>>>> bddfdbea5 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 2f6607232 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> a62b956fb6 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> c82e069f2 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 186135b4fc (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
>>>>>>> 6055a8bb0 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> c8421ce4b (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> ea0ead77fe (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
<<<<<<< HEAD
>>>>>>> 6055a8bb0 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ad15fc9fe (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 08d14c1e3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> af7a3e028 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> bbf7cc3b0 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> 335182b815 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
>>>>>>> d0dc799ff (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> ea3b40e18 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 4adaa7f479 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> d2cb939f5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> 6af71ff432 (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 93cedd7b3 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> c1afaaa1de (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 4d37d5ef5 (GEOMESA-3100 Kafka layer views (#2784))
<<<<<<< HEAD
>>>>>>> b4c6267f1c (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
>>>>>>> ad15fc9fe (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
<<<<<<< HEAD
>>>>>>> ff40ff0969 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> 3be8d2a5a (Merge branch 'feature/postgis-fixes')
>>>>>>> db8d998aa2 (Merge branch 'feature/postgis-fixes')
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
=======
          case m: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> af0a88eb1 (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> 17f13b3a7a (GEOMESA-3100 Kafka layer views (#2784))
=======
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
>>>>>>> 0185d52d35 (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
=======
          case _: Clear  => toLoad.cache.fireClear(timestamp); cache.clear()
>>>>>>> 60b8016be (GEOMESA-3135 Fix classpath for HBase/Kudu/Bigtable GeoServer Avro export (#2805))
=======
>>>>>>> f7038468b (GEOMESA-3100 Kafka layer views (#2784))
>>>>>>> d2002b5afd (GEOMESA-3100 Kafka layer views (#2784))
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

